package manager_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cpip/internal/auth"
	"cpip/internal/connection"
	"cpip/internal/logger"
	"cpip/internal/manager"
	"cpip/internal/metrics"
	"cpip/internal/session"
	"cpip/internal/websocket"
	"cpip/internal/wstest"
)

func connConfig() connection.Config {
	return connection.Config{
		HeartbeatInterval: 50 * time.Millisecond,
		PongTimeout:       time.Second,
		WriteTimeout:      time.Second,
		MaxPayloadBytes:   1024,
		SendQueueSize:     8,
	}
}

// buildConn creates a connection wired to mgr.Unregister as its OnClose. It does
// not start Serve unless the caller does.
func buildConn(mgr *manager.Manager, connID, userID string, fc websocket.Conn) *connection.Connection {
	return connection.New(connection.Params{
		ID:       connID,
		Identity: auth.Identity{UserID: userID},
		Session:  session.New(userID, time.Now()),
		Conn:     fc,
		Config:   connConfig(),
		Logger:   logger.Nop(),
		Metrics:  metrics.NewNoop(),
		Handler:  connection.NoopHandler{},
		Parent:   context.Background(),
		OnClose:  mgr.Unregister,
	})
}

func newManager(max int) *manager.Manager {
	return manager.New(manager.Params{Logger: logger.Nop(), Metrics: metrics.NewNoop(), MaxConnections: max})
}

func TestRegisterAndLookup(t *testing.T) {
	m := newManager(10)
	c := buildConn(m, "c1", "alice", wstest.NewFakeConn())
	if err := m.Register(c); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got, ok := m.Get("c1"); !ok || got != c {
		t.Fatal("Get did not return the registered connection")
	}
	if conns := m.GetByUser("alice"); len(conns) != 1 || conns[0] != c {
		t.Fatalf("GetByUser returned %d conns, want 1", len(conns))
	}
	if got, ok := m.GetBySession(c.SessionID()); !ok || got != c {
		t.Fatal("GetBySession failed")
	}
	if m.Count() != 1 {
		t.Fatalf("Count = %d, want 1", m.Count())
	}
}

func TestMultipleConnectionsPerUser(t *testing.T) {
	m := newManager(10)
	c1 := buildConn(m, "c1", "alice", wstest.NewFakeConn())
	c2 := buildConn(m, "c2", "alice", wstest.NewFakeConn())
	_ = m.Register(c1)
	_ = m.Register(c2)
	if conns := m.GetByUser("alice"); len(conns) != 2 {
		t.Fatalf("GetByUser = %d, want 2", len(conns))
	}
}

func TestConnectionLimit(t *testing.T) {
	m := newManager(2)
	_ = m.Register(buildConn(m, "c1", "u", wstest.NewFakeConn()))
	_ = m.Register(buildConn(m, "c2", "u", wstest.NewFakeConn()))
	err := m.Register(buildConn(m, "c3", "u", wstest.NewFakeConn()))
	if !errors.Is(err, manager.ErrConnectionLimit) {
		t.Fatalf("expected ErrConnectionLimit, got %v", err)
	}
	if !m.AtCapacity() {
		t.Fatal("AtCapacity should be true")
	}
}

func TestDuplicateID(t *testing.T) {
	m := newManager(10)
	_ = m.Register(buildConn(m, "dup", "u", wstest.NewFakeConn()))
	err := m.Register(buildConn(m, "dup", "u", wstest.NewFakeConn()))
	if !errors.Is(err, manager.ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
}

func TestUnregisterIdempotent(t *testing.T) {
	m := newManager(10)
	c := buildConn(m, "c1", "u", wstest.NewFakeConn())
	_ = m.Register(c)
	m.Unregister(c)
	m.Unregister(c) // must not panic or drive Count negative
	if m.Count() != 0 {
		t.Fatalf("Count = %d, want 0", m.Count())
	}
}

func TestBroadcastAndTargetedSend(t *testing.T) {
	m := newManager(10)
	c1 := buildConn(m, "c1", "alice", wstest.NewFakeConn())
	c2 := buildConn(m, "c2", "bob", wstest.NewFakeConn())
	_ = m.Register(c1)
	_ = m.Register(c2)

	if n := m.Broadcast(websocket.TextMessage, []byte("hi")); n != 2 {
		t.Fatalf("Broadcast queued to %d, want 2", n)
	}
	if n := m.SendToUser("alice", websocket.TextMessage, []byte("yo")); n != 1 {
		t.Fatalf("SendToUser queued to %d, want 1", n)
	}
	if err := m.SendToConn("c2", websocket.TextMessage, []byte("z")); err != nil {
		t.Fatalf("SendToConn: %v", err)
	}
	if err := m.SendToConn("missing", websocket.TextMessage, nil); err == nil {
		t.Fatal("SendToConn to missing id should error")
	}
}

func TestConcurrentRegisterUnregister(t *testing.T) {
	m := newManager(100_000)
	const workers = 50
	const perWorker = 200

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := fmt.Sprintf("c-%d-%d", w, i)
				c := buildConn(m, id, fmt.Sprintf("user-%d", w), wstest.NewFakeConn())
				if err := m.Register(c); err != nil {
					t.Errorf("Register: %v", err)
					return
				}
				// Interleave a lookup to exercise the read path under contention.
				_, _ = m.Get(id)
				m.Unregister(c)
			}
		}(w)
	}
	wg.Wait()

	if m.Count() != 0 {
		t.Fatalf("Count = %d after balanced register/unregister, want 0", m.Count())
	}
}

func TestShutdownClosesAllConnections(t *testing.T) {
	m := newManager(100)
	const n = 20
	for i := 0; i < n; i++ {
		fc := wstest.NewFakeConn()
		c := buildConn(m, fmt.Sprintf("c%d", i), "u", fc)
		if err := m.Register(c); err != nil {
			t.Fatalf("Register: %v", err)
		}
		go c.Serve()
	}
	// Let the connections reach their read loops.
	time.Sleep(50 * time.Millisecond)
	if m.Count() != n {
		t.Fatalf("Count = %d, want %d", m.Count(), n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m.Shutdown(ctx)

	if m.Count() != 0 {
		t.Fatalf("Count = %d after shutdown, want 0", m.Count())
	}
	if ctx.Err() != nil {
		t.Fatal("shutdown timed out instead of draining cleanly")
	}
}

func TestRegisterAfterShutdownRejected(t *testing.T) {
	m := newManager(10)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m.Shutdown(ctx)
	err := m.Register(buildConn(m, "c1", "u", wstest.NewFakeConn()))
	if !errors.Is(err, manager.ErrManagerClosed) {
		t.Fatalf("expected ErrManagerClosed, got %v", err)
	}
}
