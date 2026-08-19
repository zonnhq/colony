// Package orchestrator runs the LLM killer test. It creates split inference
// jobs, hands each node its role through the heartbeat command channel, and
// relays activation tensors between the two nodes over WebSockets (nodes sit
// behind NAT and cannot reach each other directly).
package orchestrator

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// defaultPendingTimeout bounds how long a job waits for both workers to open
// their relay connection before it is given up on as unreachable. The mock
// engine's process is on the socket almost immediately, but the real engine
// has to start Python and import torch and transformers first, which alone
// can take well over 30 seconds on a modest remote machine (this was
// measured taking 40 to 90+ seconds on real hardware in this Zone), before it
// ever reaches the code that opens the relay connection. A short timeout here
// does not affect the mock engine at all, but it kills every real job before
// the worker gets a chance to even try, which is exactly the "works with
// mock, always fails for real" pattern this was chosen to fix.
const defaultPendingTimeout = 3 * time.Minute

// Job lifecycle states.
const (
	StatusPending = "PENDING"
	StatusRunning = "RUNNING"
	StatusDone    = "DONE"
	StatusFailed  = "FAILED"
)

// Command is the job spec handed to a single node inside its heartbeat response.
type Command struct {
	JobID        string  `json:"job_id"`
	Role         string  `json:"role"` // primary or secondary
	Model        string  `json:"model"`
	Prompt       string  `json:"prompt"`
	Engine       string  `json:"engine"` // real or mock
	MaxNewTokens int     `json:"max_new_tokens"`
	Split        float64 `json:"split"` // fraction of layers on the primary
	RelayPort    string  `json:"relay_port"`
	RelayPath    string  `json:"relay_path"`
	RelayURL     string  `json:"relay_url,omitempty"`
}

// JobEvent is one entry in a job's per-node activity log: a worker connecting,
// a loading or generation status update, a completion, or a failure. This is
// what makes a split run legible as two machines cooperating rather than one
// opaque black box: every node's actual contribution is visible, in order.
type JobEvent struct {
	Ts      time.Time `json:"ts"`
	NodeID  string    `json:"node_id"`
	Role    string    `json:"role"` // primary or secondary
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

// maxJobEvents caps the per-job log so a long run cannot grow it without
// bound; the oldest entries are dropped first.
const maxJobEvents = 300

// Job is a split inference run across two nodes.
type Job struct {
	ID              string    `json:"id"`
	ColonyID        string    `json:"colony_id"`
	Model           string    `json:"model"`
	Prompt          string    `json:"prompt"`
	Engine          string    `json:"engine"`
	MaxNewTokens    int       `json:"max_new_tokens"`
	PrimaryNodeID   string    `json:"primary_node_id"`
	SecondaryNodeID string    `json:"secondary_node_id"`
	Status          string    `json:"status"`
	Progress        string    `json:"progress,omitempty"`
	Result          string    `json:"result"`
	Error           string    `json:"error"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`

	// PrimaryTensorsRelayed and SecondaryTensorsRelayed count the activation
	// tensors each node has actually forwarded through the relay: the real,
	// per-node measure of how much of the split computation each machine has
	// carried so far, not just which role it was assigned.
	PrimaryTensorsRelayed   int `json:"primary_tensors_relayed"`
	SecondaryTensorsRelayed int `json:"secondary_tensors_relayed"`

	// Events is the ordered, per-node activity log for this job: connects,
	// status updates, results, and failures, each attributed to the node and
	// role that produced it.
	Events []JobEvent `json:"events"`
}

// Manager holds jobs, pending node commands, and relay sessions.
type Manager struct {
	mu             sync.Mutex
	jobs           map[string]*Job
	commands       map[string][]Command // nodeID -> pending command queue
	sessions       map[string]*session
	relayURL       relayConfig
	pendingTimeout time.Duration
}

type relayConfig struct {
	Port string
	Path string
	URL  string
}

// NewManager builds a manager. relayPort and relayPath tell nodes where to
// connect their workers for tensor relaying.
func NewManager(relayPort, relayPath, relayURL string) *Manager {
	return newManager(relayPort, relayPath, relayURL, defaultPendingTimeout)
}

func newManager(relayPort, relayPath, relayURL string, pendingTimeout time.Duration) *Manager {
	return &Manager{
		jobs:           make(map[string]*Job),
		commands:       make(map[string][]Command),
		sessions:       make(map[string]*session),
		relayURL:       relayConfig{Port: relayPort, Path: relayPath, URL: relayURL},
		pendingTimeout: pendingTimeout,
	}
}

// CreateJob starts a split job across the two nodes and queues their commands.
func (m *Manager) CreateJob(colonyID, model, prompt, engine string, primary, secondary string, maxNewTokens int) *Job {
	if engine == "" {
		engine = "mock"
	}
	if maxNewTokens <= 0 {
		maxNewTokens = 20
	}
	now := time.Now().UTC()
	job := &Job{
		ID:              uuid.NewString(),
		ColonyID:        colonyID,
		Model:           model,
		Prompt:          prompt,
		Engine:          engine,
		MaxNewTokens:    maxNewTokens,
		PrimaryNodeID:   primary,
		SecondaryNodeID: secondary,
		Status:          StatusPending,
		Progress:        "Waiting for workers",
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	m.mu.Lock()
	m.jobs[job.ID] = job
	m.commands[primary] = append(m.commands[primary], m.command(job, "primary"))
	m.commands[secondary] = append(m.commands[secondary], m.command(job, "secondary"))
	m.appendEventLocked(job, primary, "primary", "INFO", "assigned the primary role (lower layers)")
	m.appendEventLocked(job, secondary, "secondary", "INFO", "assigned the secondary role (upper layers)")
	m.mu.Unlock()

	if m.pendingTimeout > 0 {
		time.AfterFunc(m.pendingTimeout, func() {
			reason := "workers did not connect to the relay within " + m.pendingTimeout.String()
			if m.failPending(job.ID, reason) {
				m.addEvent(job.ID, "primary", "ERROR", reason)
				m.addEvent(job.ID, "secondary", "ERROR", reason)
			}
		})
	}
	return job
}

func (m *Manager) command(job *Job, role string) Command {
	return Command{
		JobID:        job.ID,
		Role:         role,
		Model:        job.Model,
		Prompt:       job.Prompt,
		Engine:       job.Engine,
		MaxNewTokens: job.MaxNewTokens,
		Split:        0.5,
		RelayPort:    m.relayURL.Port,
		RelayPath:    m.relayURL.Path,
		RelayURL:     m.relayURL.URL,
	}
}

// TakeCommand returns and clears any pending command for a node, JSON encoded
// for the heartbeat response. The empty string means nothing is pending.
func (m *Manager) TakeCommand(nodeID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	queue := m.commands[nodeID]
	if len(queue) == 0 {
		return ""
	}
	cmd := queue[0]
	if len(queue) == 1 {
		delete(m.commands, nodeID)
	} else {
		m.commands[nodeID] = queue[1:]
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return ""
	}
	return string(data)
}

// Job returns a copy of a job.
func (m *Manager) Job(id string) (Job, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *j, true
}

// Jobs returns copies of all jobs, newest first.
func (m *Manager) Jobs() []Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		out = append(out, *j)
	}
	// Simple insertion by CreatedAt descending.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].CreatedAt.After(out[j-1].CreatedAt); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// nodeForRole returns which node holds a role on a job, for event attribution.
func (j *Job) nodeForRole(role string) string {
	if role == "primary" {
		return j.PrimaryNodeID
	}
	return j.SecondaryNodeID
}

// addEvent appends one entry to a job's activity log, attributed to whichever
// node currently holds the given role. Oldest entries drop first once the log
// reaches maxJobEvents, so a long-running job cannot grow it without bound.
func (m *Manager) addEvent(jobID, role, level, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return
	}
	m.appendEventLocked(j, j.nodeForRole(role), role, level, message)
}

// appendEventLocked is the shared low-level append; callers already hold m.mu.
func (m *Manager) appendEventLocked(j *Job, nodeID, role, level, message string) {
	j.Events = append(j.Events, JobEvent{
		Ts:      time.Now().UTC(),
		NodeID:  nodeID,
		Role:    role,
		Level:   level,
		Message: message,
	})
	if over := len(j.Events) - maxJobEvents; over > 0 {
		j.Events = j.Events[over:]
	}
}

// recordTensor counts one activation tensor a role has relayed, the real
// per-node measure of how much of the split computation it has carried.
func (m *Manager) recordTensor(jobID, fromRole string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return
	}
	if fromRole == "primary" {
		j.PrimaryTensorsRelayed++
	} else {
		j.SecondaryTensorsRelayed++
	}
}

func (m *Manager) setStatus(jobID, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[jobID]; ok {
		if j.Status == StatusDone || j.Status == StatusFailed {
			return
		}
		j.Status = status
		j.UpdatedAt = time.Now().UTC()
	}
}

func (m *Manager) setProgress(jobID, detail string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[jobID]; ok {
		if j.Status == StatusDone || j.Status == StatusFailed {
			return
		}
		j.Status = StatusRunning
		j.Progress = detail
		j.UpdatedAt = time.Now().UTC()
	}
}

func (m *Manager) setResult(jobID, text string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[jobID]; ok {
		if j.Status == StatusDone || j.Status == StatusFailed {
			return
		}
		j.Result = text
		j.Status = StatusDone
		j.UpdatedAt = time.Now().UTC()
	}
}

func (m *Manager) fail(jobID, reason string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failLocked(jobID, reason, false)
}

func (m *Manager) failPending(jobID, reason string) bool {
	m.mu.Lock()
	changed := m.failLocked(jobID, reason, true)
	m.mu.Unlock()
	if changed {
		m.stopSession(jobID)
	}
	return changed
}

func (m *Manager) failLocked(jobID, reason string, pendingOnly bool) bool {
	j, ok := m.jobs[jobID]
	if !ok || j.Status == StatusDone || j.Status == StatusFailed || (pendingOnly && j.Status != StatusPending) {
		return false
	}
	j.Error = strings.TrimSpace(reason)
	if j.Error == "" {
		j.Error = "worker failed without an error message"
	}
	j.Status = StatusFailed
	j.UpdatedAt = time.Now().UTC()
	for _, nodeID := range []string{j.PrimaryNodeID, j.SecondaryNodeID} {
		queue := m.commands[nodeID]
		kept := queue[:0]
		for _, cmd := range queue {
			if cmd.JobID != jobID {
				kept = append(kept, cmd)
			}
		}
		if len(kept) == 0 {
			delete(m.commands, nodeID)
		} else {
			m.commands[nodeID] = kept
		}
	}
	return true
}

// FailJobFromNode converts the structured worker failure message reported by a
// participating node into a terminal job state. Unrelated error messages and
// reports from nodes outside the job are ignored.
func (m *Manager) FailJobFromNode(nodeID, message string) bool {
	const prefix = "LLM task "
	const separator = " failed: "
	if !strings.HasPrefix(message, prefix) {
		return false
	}
	rest := strings.TrimPrefix(message, prefix)
	parts := strings.SplitN(rest, separator, 2)
	if len(parts) != 2 {
		return false
	}
	jobID := strings.TrimSpace(parts[0])
	if _, err := uuid.Parse(jobID); err != nil {
		return false
	}

	m.mu.Lock()
	j, ok := m.jobs[jobID]
	if !ok || (j.PrimaryNodeID != nodeID && j.SecondaryNodeID != nodeID) {
		m.mu.Unlock()
		return false
	}
	role := "secondary"
	if j.PrimaryNodeID == nodeID {
		role = "primary"
	}
	changed := m.failLocked(jobID, parts[1], false)
	if changed {
		m.appendEventLocked(j, nodeID, role, "ERROR", parts[1])
	}
	m.mu.Unlock()
	if changed {
		m.stopSession(jobID)
	}
	return changed
}
