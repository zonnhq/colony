// Package daemon is the node's engine. It detects hardware, talks to the
// Coordinator, and keeps a live snapshot of state for the local API and the GUI.
// It is designed to run headless: the Wails window is just a viewer on top.
package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/projectcolony/colony/node-gui/internal/client"
	"github.com/projectcolony/colony/node-gui/internal/config"
	"github.com/projectcolony/colony/node-gui/internal/resources"
	"github.com/projectcolony/colony/node-gui/internal/worker"
)

// Connection states between the node and the Coordinator.
const (
	Connecting   = "CONNECTING"
	Connected    = "CONNECTED"
	Disconnected = "DISCONNECTED"
	// Paused means the operator turned availability off, so the node is not
	// contributing and is not trying to reach the Coordinator.
	Paused = "PAUSED"
)

const (
	minBackoff  = 1 * time.Second
	maxBackoff  = 30 * time.Second
	maxEvents   = 200
	registerTTL = 8 * time.Second
)

// Event is one line in the activity log.
type Event struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

// Ranking is the node's live standing in the fleet.
type Ranking struct {
	Rank         int     `json:"rank"`
	ActiveNodes  int     `json:"active_nodes"`
	Score        float64 `json:"contribution_score"`
	AverageScore float64 `json:"average_score"`
}

// ColonyUsage is how much of the contribution the Zone is actually using right
// now, measured from the worker process. It is zero when no job is running.
type ColonyUsage struct {
	CPUCores float64 `json:"cpu_cores"`
	RAMGB    float64 `json:"ram_gb"`
	Active   bool    `json:"active"`
}

// State is an immutable snapshot handed to the API and the GUI.
type State struct {
	NodeName       string                `json:"node_name"`
	NodeID         string                `json:"node_id"`
	Connection     string                `json:"connection"`
	Status         string                `json:"status"`
	Available      bool                  `json:"available"`
	ColonyID       string                `json:"colony_id"`
	CoordinatorURL string                `json:"coordinator_url"`
	Specs          resources.Specs       `json:"specs"`
	Allocation     config.Allocation     `json:"allocation"`
	Utilization    resources.Utilization `json:"utilization"`
	ColonyUsage    ColonyUsage           `json:"colony_usage"`
	Ranking        Ranking               `json:"ranking"`
	Events         []Event               `json:"events"`
}

// Daemon holds the mutable state behind a mutex.
type Daemon struct {
	mu             sync.RWMutex
	cfg            config.Config
	specs          resources.Specs
	fingerprint    string
	nodeID         string
	connection     string
	colonyID       string
	util           resources.Utilization
	ranking        Ranking
	events         []Event
	conn           *client.Client
	gpuOverTemp    bool
	lastCommandKey string
	colonyUsage    worker.Usage
	colonyBusy     bool

	reconnect chan struct{}
}

// gpuWarnTempC is the GPU temperature above which the node warns the operator.
const gpuWarnTempC = 90.0

// New detects hardware and builds a daemon from the given config. On the first
// run it seeds a starting contribution of 20% of the machine so a new node
// arrives with something to give rather than zeros the user must dial up.
func New(cfg config.Config) *Daemon {
	specs := resources.Detect()
	seeded := false
	if !cfg.Configured {
		cfg.Allocation = defaultAllocation(specs)
		cfg.Configured = true
		seeded = true
	}
	cfg.Allocation = clampAllocation(cfg.Allocation, specs)
	d := &Daemon{
		cfg:         cfg,
		specs:       specs,
		fingerprint: resources.Fingerprint(),
		connection:  Disconnected,
		reconnect:   make(chan struct{}, 1),
	}
	d.log("INFO", "Detected "+specs.OS+" with "+humanSpecs(specs))
	if seeded {
		if err := config.Save(cfg); err != nil {
			d.log("WARN", "Could not save the starting allocation: "+err.Error())
		} else {
			d.log("INFO", "Seeded a starting contribution of 20% of this machine; adjust it in Settings")
		}
	}
	return d
}

// defaultAllocation is the first run contribution: 20% of each detected resource,
// and 200 Mbps (20% of the 1000 Mbps bandwidth range).
func defaultAllocation(s resources.Specs) config.Allocation {
	pct := func(total int) int { return int(math.Round(float64(total) * 0.2)) }
	return config.Allocation{
		CPUCores:      pct(s.CPUCores),
		RAMGB:         pct(s.RAMGB),
		GPUMemory:     pct(s.GPUMemoryGB),
		BandwidthMbps: 200,
	}
}

// Specs returns the detected hardware.
func (d *Daemon) Specs() resources.Specs {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.specs
}

// Config returns the current config.
func (d *Daemon) Config() config.Config {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.cfg
}

// Snapshot returns the current state, events newest first.
func (d *Daemon) Snapshot() State {
	d.mu.RLock()
	defer d.mu.RUnlock()

	events := make([]Event, len(d.events))
	for i, e := range d.events {
		events[len(d.events)-1-i] = e
	}
	status := "OFFLINE"
	if d.connection == Connected {
		status = "ONLINE"
	}
	return State{
		NodeName:       d.cfg.NodeName,
		NodeID:         d.nodeID,
		Connection:     d.connection,
		Status:         status,
		Available:      d.cfg.Available,
		ColonyID:       d.colonyID,
		CoordinatorURL: d.cfg.CoordinatorURL,
		Specs:          d.specs,
		Allocation:     d.cfg.Allocation,
		Utilization:    d.util,
		ColonyUsage: ColonyUsage{
			CPUCores: d.colonyUsage.CPUCores,
			RAMGB:    d.colonyUsage.RAMGB,
			Active:   d.colonyBusy,
		},
		Ranking: d.ranking,
		Events:  events,
	}
}

// UpdateConfig validates and saves new settings, then forces a reconnect so the
// Coordinator sees the updated allocation right away.
func (d *Daemon) UpdateConfig(newCfg config.Config) error {
	d.mu.Lock()
	newCfg.Allocation = clampAllocation(newCfg.Allocation, d.specs)
	if newCfg.NodeName == "" {
		newCfg.NodeName = d.cfg.NodeName
	}
	if newCfg.CoordinatorURL == "" {
		newCfg.CoordinatorURL = d.cfg.CoordinatorURL
	}
	d.cfg = newCfg
	d.mu.Unlock()

	if err := config.Save(newCfg); err != nil {
		return err
	}
	d.log("INFO", "Settings updated, applying the changes")
	d.triggerReconnect()
	return nil
}

// available reports whether the operator currently wants the node contributing.
func (d *Daemon) available() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.cfg.Available
}

// Run drives the connect, register, and heartbeat lifecycle until ctx is done.
// It never returns on transient errors, it backs off and retries. When the
// operator turns availability off, it parks in the Paused state and waits to be
// woken by a config change rather than hammering the Coordinator.
func (d *Daemon) Run(ctx context.Context) {
	defer d.recoverCrash("Run")
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		if !d.available() {
			d.park(ctx)
			backoff = minBackoff
			continue
		}
		if d.connectOnce(ctx) {
			backoff = minBackoff // a clean session resets the backoff
		}
		if ctx.Err() != nil {
			return
		}
		if !d.available() {
			continue // turned off during the session, park without waiting out the backoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		case <-d.reconnect:
		}
		backoff = nextBackoff(backoff)
	}
}

// notifyOffline tells the Coordinator this node is pausing on purpose so the
// fleet reflects it at once instead of waiting out the reaper. Best effort: if
// the Coordinator is unreachable the reaper still catches the silence.
func (d *Daemon) notifyOffline() {
	d.mu.RLock()
	nodeID := d.nodeID
	addr := d.cfg.CoordinatorURL
	d.mu.RUnlock()
	if nodeID == "" {
		return
	}
	cli, err := client.Dial(addr)
	if err != nil {
		return
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), registerTTL)
	defer cancel()
	if err := cli.SetOffline(ctx, nodeID, "paused by operator"); err != nil {
		d.log("WARN", "Could not tell the Coordinator about the pause: "+err.Error())
	}
}

// park holds the node in the Paused state until availability is turned back on
// (a config change signals d.reconnect) or the context is cancelled. It tells the
// Coordinator up front so the pause shows immediately.
func (d *Daemon) park(ctx context.Context) {
	d.notifyOffline()
	d.setConnection(Paused)
	d.log("INFO", "Paused: this machine is not available to the Zone")
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.reconnect:
			if d.available() {
				d.log("INFO", "Resumed: making this machine available to the Zone")
				return
			}
			// A config change that left the node unavailable (for example an
			// allocation edit while paused); stay parked.
		}
	}
}

// connectOnce dials, registers, and runs the heartbeat loop for one session.
// It returns true if the session reached the connected state.
func (d *Daemon) connectOnce(ctx context.Context) bool {
	cfg := d.Config()
	d.setConnection(Connecting)
	d.log("INFO", "Connecting to Coordinator at "+cfg.CoordinatorURL)

	cli, err := client.Dial(cfg.CoordinatorURL)
	if err != nil {
		d.log("ERROR", "Could not reach the Coordinator: "+err.Error())
		d.setConnection(Disconnected)
		return false
	}
	defer func() {
		d.mu.Lock()
		d.conn = nil
		d.mu.Unlock()
		cli.Close()
	}()

	d.mu.Lock()
	d.conn = cli
	d.mu.Unlock()

	regCtx, cancel := context.WithTimeout(ctx, registerTTL)
	res, err := cli.Register(regCtx, cfg, d.Specs(), d.fingerprint)
	cancel()
	if err != nil {
		d.log("ERROR", "Registration failed: "+err.Error())
		d.setConnection(Disconnected)
		return false
	}

	// Adopt the id the Coordinator settled on. It can differ from ours when the
	// Coordinator recognised this machine by fingerprint and merged us onto the
	// existing record, which is how one device avoids showing up twice.
	if res.NodeID != "" && res.NodeID != cfg.NodeID {
		d.mu.Lock()
		d.cfg.NodeID = res.NodeID
		newCfg := d.cfg
		d.mu.Unlock()
		if err := config.Save(newCfg); err != nil {
			d.log("WARN", "Could not persist the Coordinator assigned id: "+err.Error())
		}
	}

	d.mu.Lock()
	d.nodeID = res.NodeID
	d.connection = Connected
	d.mu.Unlock()
	d.log("INFO", "Registered with the Zone, node id "+res.NodeID)

	d.heartbeatLoop(ctx, cli, res.NodeID, res.HeartbeatInterval)

	d.setConnection(Disconnected)
	return true
}

func (d *Daemon) heartbeatLoop(ctx context.Context, cli *client.Client, nodeID string, interval time.Duration) {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := cli.HeartbeatStream(connCtx)
	if err != nil {
		d.log("ERROR", "Could not open the heartbeat stream: "+err.Error())
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.reconnect:
			d.log("INFO", "Restarting the Coordinator connection")
			return
		case <-ticker.C:
			util := resources.Sample(d.specs.CPUCores)
			d.mu.RLock()
			workerUsage := d.colonyUsage
			workerActive := d.colonyBusy
			d.mu.RUnlock()
			ack, err := client.SendHeartbeat(stream, nodeID, util, workerUsage.CPUCores, workerUsage.RAMGB, workerActive)
			if err != nil {
				d.log("WARN", "Lost the Coordinator connection: "+err.Error())
				return
			}
			d.mu.Lock()
			d.util = util
			d.colonyID = ack.GetAssignedColonyId()
			d.ranking = Ranking{
				Rank:         int(ack.GetRank()),
				ActiveNodes:  int(ack.GetActiveNodes()),
				Score:        ack.GetContributionScore(),
				AverageScore: ack.GetAverageScore(),
			}
			d.mu.Unlock()
			d.checkGPUTemp(util.GPUTempC)
			d.handleCommand(ctx, ack.GetCommand())
		}
	}
}

func (d *Daemon) setConnection(state string) {
	d.mu.Lock()
	d.connection = state
	if state != Connected {
		d.colonyID = ""
	}
	d.mu.Unlock()
}

func (d *Daemon) triggerReconnect() {
	select {
	case d.reconnect <- struct{}{}:
	default:
	}
}

// handleCommand parses a job command from the heartbeat and, if it is new,
// launches the inference worker for this node's role.
func (d *Daemon) handleCommand(ctx context.Context, raw string) {
	if strings.TrimSpace(raw) == "" {
		return
	}
	var cmd worker.Command
	if err := json.Unmarshal([]byte(raw), &cmd); err != nil {
		d.log("WARN", "Ignoring an unreadable job command")
		return
	}
	if cmd.JobID == "" {
		return
	}

	d.mu.Lock()
	commandKey := cmd.JobID + ":" + cmd.Role
	if commandKey == d.lastCommandKey {
		d.mu.Unlock()
		return
	}
	d.lastCommandKey = commandKey
	host := coordinatorHost(d.cfg.CoordinatorURL)
	d.mu.Unlock()

	// A heartbeat stream is only a delivery channel. It can be recycled by
	// ngrok or gRPC while a model is still loading, so the worker must not inherit
	// that stream's cancellation. The relay owns the job's terminal lifecycle.
	go d.runJob(context.Background(), cmd, host)
}

func (d *Daemon) runJob(ctx context.Context, cmd worker.Command, host string) {
	defer d.recoverCrash("runJob")
	d.log("INFO", "Received LLM task "+cmd.JobID+" as the "+cmd.Role+" node")

	d.mu.Lock()
	d.colonyBusy = true
	d.mu.Unlock()

	sample := func(u worker.Usage) {
		d.mu.Lock()
		d.colonyUsage = u
		d.mu.Unlock()
	}
	err := worker.Launch(ctx, cmd, host, d.log, sample)

	d.mu.Lock()
	d.colonyBusy = false
	d.colonyUsage = worker.Usage{}
	d.mu.Unlock()

	if err != nil {
		d.PushError("ERROR", "LLM task "+cmd.JobID+" failed: "+err.Error())
	}
}

func coordinatorHost(coordinatorURL string) string {
	if host, _, err := net.SplitHostPort(coordinatorURL); err == nil {
		return host
	}
	return coordinatorURL
}

// PushError records an issue locally and forwards it to the Coordinator issues
// feed when connected, so the operator sees node side problems.
func (d *Daemon) PushError(level, message string) {
	d.log(level, message)

	d.mu.RLock()
	conn := d.conn
	nodeID := d.nodeID
	d.mu.RUnlock()

	if conn == nil || nodeID == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), registerTTL)
		defer cancel()
		if err := conn.ReportError(ctx, nodeID, level, message); err != nil {
			d.log("WARN", "Could not send the report to the Coordinator: "+err.Error())
		}
	}()
}

// recoverCrash catches a panic in the named goroutine so one bad job or a
// flaky driver call cannot take down the whole node process. It reports an
// anonymized crash report to the Coordinator issues feed: what kind of error
// it was, where it happened in the code, and the operating system. It never
// includes the hostname, username, file paths outside this repo, or any
// network address, so the report cannot identify the contributor or their
// machine.
func (d *Daemon) recoverCrash(where string) {
	r := recover()
	if r == nil {
		return
	}
	d.mu.RLock()
	enabled := d.cfg.CrashReportsEnabled
	osName := d.specs.OS
	d.mu.RUnlock()

	trace := string(debug.Stack())
	d.log("ERROR", fmt.Sprintf("Recovered from a crash in %s: %v", where, r))

	if !enabled {
		return
	}
	report := fmt.Sprintf("Crash in %s (%T): %v\nOS: %s\n%s", where, r, r, osName, trace)
	d.PushError("ERROR", report)
}

// checkGPUTemp warns the operator once when the GPU crosses the temperature
// threshold, and notes when it recovers. Debounced so it does not spam.
func (d *Daemon) checkGPUTemp(tempC float64) {
	if tempC <= 0 {
		return
	}
	d.mu.Lock()
	over := d.gpuOverTemp
	d.mu.Unlock()

	switch {
	case tempC >= gpuWarnTempC && !over:
		d.mu.Lock()
		d.gpuOverTemp = true
		d.mu.Unlock()
		d.PushError("WARN", "GPU temperature is high: "+itoa(int(tempC))+" C")
	case tempC < gpuWarnTempC && over:
		d.mu.Lock()
		d.gpuOverTemp = false
		d.mu.Unlock()
		d.log("INFO", "GPU temperature back to normal: "+itoa(int(tempC))+" C")
	}
}

func (d *Daemon) log(level, message string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, Event{Time: time.Now().UTC(), Level: level, Message: message})
	if len(d.events) > maxEvents {
		d.events = d.events[len(d.events)-maxEvents:]
	}
}

func clampAllocation(a config.Allocation, s resources.Specs) config.Allocation {
	a.CPUCores = clamp(a.CPUCores, 0, s.CPUCores)
	a.RAMGB = clamp(a.RAMGB, 0, s.RAMGB)
	a.GPUMemory = clamp(a.GPUMemory, 0, s.GPUMemoryGB)
	if a.BandwidthMbps < 0 {
		a.BandwidthMbps = 0
	}
	return a
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if hi > 0 && v > hi {
		return hi
	}
	return v
}

func humanSpecs(s resources.Specs) string {
	out := itoa(s.CPUCores) + " cores, " + itoa(s.RAMGB) + " GB RAM"
	if s.GPUModel != "" {
		out += ", " + s.GPUModel
		if s.GPUMemoryGB > 0 {
			out += " " + itoa(s.GPUMemoryGB) + " GB"
		}
	}
	return out
}

func itoa(n int) string {
	return strconv.Itoa(n)
}

func nextBackoff(b time.Duration) time.Duration {
	b *= 2
	if b > maxBackoff {
		return maxBackoff
	}
	return b
}
