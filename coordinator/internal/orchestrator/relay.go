package orchestrator

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1 << 16,
	WriteBufferSize: 1 << 16,
	// Nodes connect from arbitrary hosts, so any origin is allowed for the beta.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// pingInterval keeps the relay connection's public tunnel (ngrok, or any NAT
// in between) from treating a connection as idle and dropping it. A real
// model load on a modest remote machine can leave one side of a job with
// nothing to relay for several minutes while its peer is still loading, so
// the server pings unilaterally on a fixed clock rather than waiting for
// idle-based logic. The worker's WebSocket client already answers a PING
// with a PONG the next time it reads a frame, but it does not poll for
// incoming frames while it is busy loading a model, so this ping is not
// paired with a read-deadline or a required PONG: enforcing one would close
// a perfectly healthy connection just because the peer was mid-load and not
// yet back in its receive loop. The goal here is only to put real bytes on
// the wire often enough that nothing in between decides the link is dead.
const pingInterval = 20 * time.Second

// reconnectGrace is how long a job waits, after one role's connection drops,
// for that same role to rejoin before the job is given up on. The worker's
// own client already retries a dropped connection immediately, so this is
// only reached when that retry itself failed, or the worker process is gone
// for good. Without a bound here, a peer that vanishes for good (crashed,
// network cut, machine put to sleep) leaves the job sitting in RUNNING
// forever: the surviving peer, if any, is blocked waiting for tensors or a
// stop signal that will never arrive, and the operator sees a job that never
// finishes and never reports why.
const reconnectGrace = 45 * time.Second

// readIdleTimeout bounds how long the relay will treat a connection as alive
// after hearing nothing from it at all: no data frame, and no PONG answering
// one of our own pings. A worker process that has already exited (crashed,
// killed, machine slept) leaves nothing behind able to answer a ping ever
// again, but the underlying TCP connection through a NAT or a public tunnel
// can otherwise look "open" to this side indefinitely, since neither a FIN
// nor an RST is guaranteed to arrive promptly (or at all) through that path.
// Without this, ReadMessage blocks forever and the job sits in RUNNING with
// a peer waiting on a stop signal nobody is left to send. The window is wide
// on purpose: it must comfortably outlast every legitimate silence, chiefly
// a single token's worth of real CPU inference on modest hardware, which is
// the only phase a healthy worker is neither sending nor reading.
const readIdleTimeout = 3 * time.Minute

// safeConn serializes writes to a websocket connection.
type safeConn struct {
	conn *websocket.Conn
	wmu  sync.Mutex
}

func (c *safeConn) writeBinary(data []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (c *safeConn) writeText(data []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

// writePing is safe to call concurrently with the other write methods: gorilla
// documents WriteControl as callable alongside WriteMessage without the caller
// serializing them itself.
func (c *safeConn) writePing() error {
	return c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
}

// keepAlive pings the connection on a fixed clock until stop closes or a ping
// fails, which means the connection is already gone and there is no point
// scheduling another one.
func keepAlive(conn *safeConn, stop <-chan struct{}) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := conn.writePing(); err != nil {
				return
			}
		}
	}
}

// session pairs the two workers of a job and buffers tensors that arrive before
// their peer has connected.
type session struct {
	mu      sync.Mutex
	conns   map[string]*safeConn // role -> conn
	pending map[string][][]byte  // dest role -> buffered binary frames
}

func newSession() *session {
	return &session{
		conns:   make(map[string]*safeConn),
		pending: make(map[string][][]byte),
	}
}

// controlMessage is the JSON a worker sends on a text frame.
type controlMessage struct {
	Type   string `json:"type"`   // status, result, error
	Status string `json:"status"` // optional detail for status
	Text   string `json:"text"`   // final generated text for result
	Error  string `json:"error"`  // reason for error
}

func peerRole(role string) string {
	if role == "primary" {
		return "secondary"
	}
	return "primary"
}

func (m *Manager) getSession(jobID string) *session {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[jobID]
	if !ok {
		s = newSession()
		m.sessions[jobID] = s
	}
	return s
}

func (m *Manager) dropSession(jobID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, jobID)
}

func (m *Manager) stopSession(jobID string) {
	m.mu.Lock()
	sess := m.sessions[jobID]
	m.mu.Unlock()
	if sess == nil {
		return
	}
	sess.mu.Lock()
	conns := make([]*safeConn, 0, len(sess.conns))
	for _, conn := range sess.conns {
		conns = append(conns, conn)
	}
	sess.mu.Unlock()
	for _, conn := range conns {
		_ = conn.writeText([]byte(`{"type":"stop"}`))
		_ = conn.conn.Close()
	}
}

func (m *Manager) acceptsRelay(jobID string) (bool, bool) {
	job, exists := m.Job(jobID)
	if !exists {
		return false, false
	}
	return job.Status != StatusDone && job.Status != StatusFailed, true
}

// ServeRelay upgrades a worker connection and relays its frames to its peer.
// Binary frames are activation tensors forwarded to the peer. Text frames are
// control messages the Coordinator reads to track job status and capture the
// final result.
func (m *Manager) ServeRelay(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job")
	role := r.URL.Query().Get("role")
	if jobID == "" || (role != "primary" && role != "secondary") {
		http.Error(w, "job and role are required", http.StatusBadRequest)
		return
	}
	if accepted, exists := m.acceptsRelay(jobID); !accepted {
		if !exists {
			http.Error(w, "job not found", http.StatusNotFound)
		} else {
			http.Error(w, "job is already finished", http.StatusGone)
		}
		return
	}

	rawConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("relay: upgrade for job %s role %s: %v", jobID, role, err)
		return
	}
	conn := &safeConn{conn: rawConn}
	if accepted, _ := m.acceptsRelay(jobID); !accepted {
		_ = rawConn.Close()
		return
	}
	log.Printf("relay: %s connected for job %s", role, jobID)
	m.addEvent(jobID, role, "INFO", "connected to the relay")

	// A pong answers one of our own keepalive pings; either kind of traffic
	// proves the peer process is still alive and pushes the deadline out
	// again. Data frames reset the same deadline in the read loop below.
	rawConn.SetReadDeadline(time.Now().Add(readIdleTimeout))
	rawConn.SetPongHandler(func(string) error {
		rawConn.SetReadDeadline(time.Now().Add(readIdleTimeout))
		return nil
	})

	sess := m.getSession(jobID)
	m.setStatus(jobID, StatusRunning)

	// Register and flush any tensors that arrived before this peer connected.
	sess.mu.Lock()
	sess.conns[role] = conn
	buffered := sess.pending[role]
	sess.pending[role] = nil
	sess.mu.Unlock()
	for _, frame := range buffered {
		if err := conn.writeBinary(frame); err != nil {
			log.Printf("relay: flush to %s for job %s: %v", role, jobID, err)
		}
	}

	stopPing := make(chan struct{})
	go keepAlive(conn, stopPing)

	defer func() {
		close(stopPing)
		sess.mu.Lock()
		// A worker may reconnect through a recycled tunnel before the old handler
		// unwinds. Never let the stale handler delete the replacement connection.
		if sess.conns[role] == conn {
			delete(sess.conns, role)
		}
		empty := len(sess.conns) == 0
		sess.mu.Unlock()
		rawConn.Close()
		m.addEvent(jobID, role, "INFO", "disconnected from the relay")
		if empty {
			m.dropSession(jobID)
		}
		go m.failIfRoleNeverReturns(jobID, role)
	}()

	for {
		mt, data, err := rawConn.ReadMessage()
		if err != nil {
			return
		}
		rawConn.SetReadDeadline(time.Now().Add(readIdleTimeout))
		switch mt {
		case websocket.BinaryMessage:
			m.recordTensor(jobID, role)
			m.forwardBinary(sess, role, data)
		case websocket.TextMessage:
			if done := m.handleControl(sess, jobID, role, data); done {
				return
			}
		}
	}
}

func (m *Manager) forwardBinary(sess *session, fromRole string, data []byte) {
	dest := peerRole(fromRole)
	sess.mu.Lock()
	peer := sess.conns[dest]
	if peer == nil {
		// Peer not connected yet, buffer until it arrives.
		frame := make([]byte, len(data))
		copy(frame, data)
		sess.pending[dest] = append(sess.pending[dest], frame)
		sess.mu.Unlock()
		return
	}
	sess.mu.Unlock()
	if err := peer.writeBinary(data); err != nil {
		log.Printf("relay: forward to %s: %v", dest, err)
	}
}

// handleControl reacts to a worker's text frame. It returns true when the job is
// finished and this connection should close.
func (m *Manager) handleControl(sess *session, jobID, fromRole string, data []byte) bool {
	var msg controlMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return false
	}
	switch msg.Type {
	case "result":
		m.setResult(jobID, msg.Text)
		m.addEvent(jobID, fromRole, "INFO", "produced the final result")
		log.Printf("relay: job %s finished", jobID)
		m.stopPeer(sess, fromRole)
		return true
	case "error":
		m.fail(jobID, msg.Error)
		m.addEvent(jobID, fromRole, "ERROR", msg.Error)
		log.Printf("relay: job %s failed: %s", jobID, msg.Error)
		m.stopPeer(sess, fromRole)
		return true
	case "status":
		m.setProgress(jobID, msg.Status)
		m.addEvent(jobID, fromRole, "INFO", msg.Status)
	}
	return false
}

// failIfRoleNeverReturns gives a role that just disconnected a grace window
// to rejoin the same job (its own client retries a drop immediately, so this
// only matters when that retry also failed or the process is gone for good).
// If the role has not rejoined by the end of the window, the job is failed
// so it does not sit in RUNNING forever with a peer waiting on data or a stop
// signal that can no longer arrive. m.fail is already a no-op on a job that
// finished normally in the meantime, so this never overrides a real result.
func (m *Manager) failIfRoleNeverReturns(jobID, role string) {
	time.Sleep(reconnectGrace)
	m.mu.Lock()
	sess := m.sessions[jobID]
	m.mu.Unlock()
	if sess != nil {
		sess.mu.Lock()
		_, back := sess.conns[role]
		sess.mu.Unlock()
		if back {
			return
		}
	}
	reason := fmt.Sprintf("lost contact with the %s node and it did not reconnect", role)
	if !m.fail(jobID, reason) {
		// The job already finished (normally or otherwise) before this grace
		// window ran out; there is nothing to report or clean up.
		return
	}
	m.addEvent(jobID, role, "ERROR", reason)
	// A surviving peer, if any, is otherwise blocked forever waiting on
	// tensors or a stop signal that can no longer arrive now that the job
	// itself is failed.
	m.stopSession(jobID)
}

func (m *Manager) stopPeer(sess *session, fromRole string) {
	dest := peerRole(fromRole)
	sess.mu.Lock()
	peer := sess.conns[dest]
	sess.mu.Unlock()
	if peer != nil {
		_ = peer.writeText([]byte(`{"type":"stop"}`))
	}
}
