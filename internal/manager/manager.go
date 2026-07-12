// Package manager is the connection registry: the authoritative in-memory index
// of all live connections on a node, with concurrency-safe lookup by connection
// id, user id, and session id, plus targeted and broadcast delivery helpers and
// coordinated graceful shutdown.
//
// The registry holds only process-local, reconstructible state. It is NOT the
// authoritative home of any correctness-critical data (that lives in Redis /
// Postgres in later modules); losing a node loses only its live sockets, which
// clients re-establish.
package manager

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"cpip/internal/connection"
	"cpip/internal/metrics"
	"cpip/internal/websocket"
)

// Errors returned by Register.
var (
	// ErrConnectionLimit means the node is at its configured capacity.
	ErrConnectionLimit = errors.New("manager: connection limit reached")
	// ErrManagerClosed means the manager is shutting down and will not admit
	// new connections.
	ErrManagerClosed = errors.New("manager: shutting down")
	// ErrDuplicate means a connection with the same id is already registered.
	ErrDuplicate = errors.New("manager: duplicate connection id")
)

// Params configure a Manager.
type Params struct {
	Logger         *slog.Logger
	Metrics        metrics.Recorder
	MaxConnections int
}

// Manager indexes live connections and coordinates their lifecycle.
type Manager struct {
	mu        sync.RWMutex
	byConn    map[string]*connection.Connection
	byUser    map[string]map[string]*connection.Connection // userID -> connID -> conn
	bySession map[string]*connection.Connection

	log     *slog.Logger
	metrics metrics.Recorder
	maxConn int

	active atomic.Int64
	closed atomic.Bool
	wg     sync.WaitGroup // tracks registered connections for shutdown draining
}

// New builds a Manager.
func New(p Params) *Manager {
	return &Manager{
		byConn:    make(map[string]*connection.Connection),
		byUser:    make(map[string]map[string]*connection.Connection),
		bySession: make(map[string]*connection.Connection),
		log:       p.Logger,
		metrics:   p.Metrics,
		maxConn:   p.MaxConnections,
	}
}

// AtCapacity reports whether the node is at its connection limit. It is a
// lock-free hint used by the gateway to reject before upgrading; Register
// performs the authoritative check.
func (m *Manager) AtCapacity() bool {
	return int(m.active.Load()) >= m.maxConn
}

// Register admits a connection into the registry. It enforces the connection
// limit and the closed state authoritatively. On success the connection is
// indexed and counted, and the shutdown WaitGroup is incremented; the matching
// decrement happens in Unregister (wired as the connection's OnClose).
func (m *Manager) Register(c *connection.Connection) error {
	if m.closed.Load() {
		return ErrManagerClosed
	}
	m.mu.Lock()
	if int(m.active.Load()) >= m.maxConn {
		m.mu.Unlock()
		return ErrConnectionLimit
	}
	if _, exists := m.byConn[c.ID()]; exists {
		m.mu.Unlock()
		return ErrDuplicate
	}
	m.byConn[c.ID()] = c
	set := m.byUser[c.UserID()]
	if set == nil {
		set = make(map[string]*connection.Connection)
		m.byUser[c.UserID()] = set
	}
	set[c.ID()] = c
	if sid := c.SessionID(); sid != "" {
		m.bySession[sid] = c
	}
	n := m.active.Add(1)
	m.wg.Add(1)
	m.mu.Unlock()

	m.metrics.SetActiveConnections(int(n))
	m.log.Info("connection registered", "conn_id", c.ID(), "user_id", c.UserID(), "active", n)
	return nil
}

// Unregister removes a connection from the registry. It is idempotent: a
// connection that is not present is ignored, so the shutdown WaitGroup is never
// decremented below zero.
func (m *Manager) Unregister(c *connection.Connection) {
	m.mu.Lock()
	if _, ok := m.byConn[c.ID()]; !ok {
		m.mu.Unlock()
		return
	}
	delete(m.byConn, c.ID())
	if set, ok := m.byUser[c.UserID()]; ok {
		delete(set, c.ID())
		if len(set) == 0 {
			delete(m.byUser, c.UserID())
		}
	}
	if sid := c.SessionID(); sid != "" {
		// Only delete if it still maps to this connection.
		if cur, ok := m.bySession[sid]; ok && cur.ID() == c.ID() {
			delete(m.bySession, sid)
		}
	}
	n := m.active.Add(-1)
	m.mu.Unlock()

	m.metrics.SetActiveConnections(int(n))
	m.wg.Done()
	m.log.Info("connection unregistered", "conn_id", c.ID(), "user_id", c.UserID(), "active", n)
}

// Get returns the connection with the given id.
func (m *Manager) Get(connID string) (*connection.Connection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.byConn[connID]
	return c, ok
}

// GetByUser returns all live connections for a user (a user may have several,
// e.g. multiple tabs or devices).
func (m *Manager) GetByUser(userID string) []*connection.Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()
	set := m.byUser[userID]
	out := make([]*connection.Connection, 0, len(set))
	for _, c := range set {
		out = append(out, c)
	}
	return out
}

// GetBySession returns the connection for a session id.
func (m *Manager) GetBySession(sessionID string) (*connection.Connection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.bySession[sessionID]
	return c, ok
}

// Count returns the number of live connections.
func (m *Manager) Count() int { return int(m.active.Load()) }

// snapshot returns a point-in-time slice of all connections. Delivery helpers
// snapshot under the read lock and then send outside it, so a slow/blocking
// send can never hold the registry lock (Send is itself non-blocking, but this
// keeps lock scope minimal and future-proof).
func (m *Manager) snapshot() []*connection.Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*connection.Connection, 0, len(m.byConn))
	for _, c := range m.byConn {
		out = append(out, c)
	}
	return out
}

// Broadcast sends a data frame to every live connection. It returns the number
// of connections the frame was successfully queued to. Connections whose queue
// is full are closed by their own Send (slow-consumer isolation) and counted as
// failures.
func (m *Manager) Broadcast(mt websocket.MessageType, data []byte) int {
	sent := 0
	for _, c := range m.snapshot() {
		if err := c.Send(mt, data); err == nil {
			sent++
		}
	}
	return sent
}

// SendToUser delivers a frame to all of a user's connections; returns the count
// successfully queued.
func (m *Manager) SendToUser(userID string, mt websocket.MessageType, data []byte) int {
	sent := 0
	for _, c := range m.GetByUser(userID) {
		if err := c.Send(mt, data); err == nil {
			sent++
		}
	}
	return sent
}

// SendToConn delivers a frame to a single connection by id.
func (m *Manager) SendToConn(connID string, mt websocket.MessageType, data []byte) error {
	c, ok := m.Get(connID)
	if !ok {
		return errors.New("manager: connection not found")
	}
	return c.Send(mt, data)
}

// Shutdown stops admitting connections, initiates a graceful close on every live
// connection, and waits for them all to drain or for ctx to expire. It is the
// coordinated counterpart to the gateway's HTTP-server shutdown.
func (m *Manager) Shutdown(ctx context.Context) {
	m.closed.Store(true)
	conns := m.snapshot()
	m.log.Info("manager shutdown: closing connections", "count", len(conns))
	for _, c := range conns {
		c.Shutdown()
	}

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.log.Info("manager shutdown: all connections closed")
	case <-ctx.Done():
		m.log.Warn("manager shutdown: timed out draining connections", "remaining", m.active.Load())
	}
}
