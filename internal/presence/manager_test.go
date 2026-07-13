package presence

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"cpip/internal/presence/awareness"
	"cpip/internal/presence/config"
	"cpip/internal/presence/events"
	"cpip/internal/presence/types"
)

type mockTransport struct {
	mu    sync.Mutex
	sends map[string][][]byte
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		sends: make(map[string][][]byte),
	}
}

func (t *mockTransport) SendText(connID string, payload []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sends[connID] = append(t.sends[connID], payload)
	return nil
}

func (t *mockTransport) getSends(connID string) [][]byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sends[connID]
}

func TestPresenceLifecycle(t *testing.T) {
	cfg := config.Default()
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cfg.RecoveryTimeout = 100 * time.Millisecond

	transport := newMockTransport()
	mgr := NewManager(Params{
		Config:    cfg,
		Transport: transport,
	})
	mgr.Start()
	defer mgr.Stop()

	// 1. Subscribe to events
	evCh := mgr.Events().Subscribe(10)
	defer mgr.Events().Unsubscribe(evCh)

	// 2. Register
	err := mgr.Register("conn-1", "user-1", "room-1", "sess-1", "token-1", map[string]any{"foo": "bar"})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Verify Registration
	p, ok := mgr.Registry().Get("conn-1")
	if !ok {
		t.Fatal("Expected presence to be registered")
	}
	if p.UserID != "user-1" || p.State != StateOnline || p.Metadata["foo"] != "bar" {
		t.Errorf("Unexpected presence state: %+v", p)
	}

	// Check for Online Event
	select {
	case ev := <-evCh:
		if ev.Type != events.PresenceOnline || ev.UserID != "user-1" {
			t.Errorf("Expected online event, got: %+v", ev)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timeout waiting for online event")
	}

	// 3. Heartbeat update
	err = mgr.Heartbeat("conn-1")
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	// 4. Explicit Deregister
	mgr.Deregister("conn-1", "explicit_leave")
	_, ok = mgr.Registry().Get("conn-1")
	if ok {
		t.Fatal("Expected presence to be unregistered")
	}

	// Check for Offline Event
	select {
	case ev := <-evCh:
		if ev.Type != events.PresenceOffline || ev.UserID != "user-1" || ev.Payload != "explicit_leave" {
			t.Errorf("Expected offline event, got: %+v", ev)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timeout waiting for offline event")
	}
}

func TestCursorAndSelection(t *testing.T) {
	cfg := config.Default()
	cfg.BroadcastInterval = 10 * time.Millisecond

	transport := newMockTransport()
	mgr := NewManager(Params{
		Config:    cfg,
		Transport: transport,
	})
	mgr.Start()
	defer mgr.Stop()

	evCh := mgr.Events().Subscribe(10)
	defer mgr.Events().Unsubscribe(evCh)

	_ = mgr.Register("conn-1", "user-1", "room-1", "sess-1", "token-1", nil)

	// Consume online event
	<-evCh

	// Update Cursor
	err := mgr.UpdateCursor("conn-1", 10, 25, "#FF0000", "main.go", true)
	if err != nil {
		t.Fatalf("UpdateCursor failed: %v", err)
	}

	p, _ := mgr.Registry().Get("conn-1")
	if p.Cursor.Line != 10 || p.Cursor.Ch != 25 || p.Cursor.Color != "#FF0000" || p.Cursor.FilePath != "main.go" {
		t.Errorf("Cursor coordinates mismatch: %+v", p.Cursor)
	}

	select {
	case ev := <-evCh:
		if ev.Type != events.CursorMoved || ev.RoomID != "room-1" {
			t.Errorf("Expected cursor event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for cursor event")
	}

	// Update Selection
	err = mgr.UpdateSelection("conn-1", 10, 20, 10, 30)
	if err != nil {
		t.Fatalf("UpdateSelection failed: %v", err)
	}

	p, _ = mgr.Registry().Get("conn-1")
	if p.Selection.AnchorLine != 10 || p.Selection.AnchorCh != 20 || p.Selection.FocusCh != 30 || p.Selection.Direction != 1 {
		t.Errorf("Selection range mismatch: %+v", p.Selection)
	}

	select {
	case ev := <-evCh:
		if ev.Type != events.SelectionChanged || ev.RoomID != "room-1" {
			t.Errorf("Expected selection event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for selection event")
	}
}

func TestTypingSpamPrevention(t *testing.T) {
	cfg := config.Default()
	cfg.TypingTimeout = 100 * time.Millisecond

	transport := newMockTransport()
	mgr := NewManager(Params{
		Config:    cfg,
		Transport: transport,
	})
	mgr.Start()
	defer mgr.Stop()

	evCh := mgr.Events().Subscribe(10)
	defer mgr.Events().Unsubscribe(evCh)

	_ = mgr.Register("conn-1", "user-1", "room-1", "sess-1", "token-1", nil)
	<-evCh // online

	// Start Typing
	_ = mgr.UpdateTyping("conn-1", true)
	select {
	case ev := <-evCh:
		if ev.Type != events.TypingStarted {
			t.Errorf("Expected TypingStarted event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for typing started event")
	}

	// Start Typing again immediately (redundant - spam prevention should ignore)
	_ = mgr.UpdateTyping("conn-1", true)
	select {
	case ev := <-evCh:
		t.Fatalf("Unexpected event received under spam protection: %+v", ev)
	case <-time.After(50 * time.Millisecond):
		// Success: no event received
	}

	// Wait for typing timeout to expire
	time.Sleep(150 * time.Millisecond)
	mgr.runSweep()

	select {
	case ev := <-evCh:
		if ev.Type != events.TypingStopped {
			t.Errorf("Expected TypingStopped event from sweep, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for typing stopped timeout event")
	}

	p, _ := mgr.Registry().Get("conn-1")
	if p.IsTyping {
		t.Error("Expected Typing status to be expired")
	}
}

func TestSessionRecovery(t *testing.T) {
	cfg := config.Default()
	cfg.RecoveryTimeout = 100 * time.Millisecond

	transport := newMockTransport()
	mgr := NewManager(Params{
		Config:    cfg,
		Transport: transport,
	})
	mgr.Start()
	defer mgr.Stop()

	evCh := mgr.Events().Subscribe(10)
	defer mgr.Events().Unsubscribe(evCh)

	_ = mgr.Register("conn-1", "user-1", "room-1", "sess-1", "token-1", nil)
	<-evCh // online

	// Disconnect connection (initiates recovery window because token is not empty)
	mgr.Deregister("conn-1", "network_drop")
	select {
	case ev := <-evCh:
		if ev.Type != events.PresenceOffline || ev.Payload != "disconnected" {
			t.Errorf("Expected offline recovery event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for recovery disconnect event")
	}

	p, ok := mgr.Registry().Get("conn-1")
	if !ok || p.State != StateDisconnected {
		t.Errorf("Expected presence to remain in registry as Disconnected, got ok=%v, state=%v", ok, p.State)
	}

	// Recover session under conn-2
	recovered, err := mgr.RecoverSession("user-1", "token-1", "conn-2")
	if err != nil {
		t.Fatalf("Session recovery failed: %v", err)
	}

	if recovered.ConnID != "conn-2" || recovered.State != StateOnline {
		t.Errorf("Unexpected recovered presence: %+v", recovered)
	}

	// Ensure old connection registration is evicted
	_, ok = mgr.Registry().Get("conn-1")
	if ok {
		t.Error("Expected old connection id conn-1 to be evicted")
	}

	// Verify events
	select {
	case ev := <-evCh:
		if ev.Type != events.PresenceRecovered || ev.ConnID != "conn-2" {
			t.Errorf("Expected recovered event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for recovery event")
	}
}

func TestBackgroundSweepAndInactivity(t *testing.T) {
	cfg := config.Default()
	cfg.HeartbeatInterval = 1 * time.Second // Heartbeat timeout is 2 seconds, won't fire during sleep
	cfg.IdleTimeout = 40 * time.Millisecond
	cfg.AwayTimeout = 80 * time.Millisecond
	cfg.RecoveryTimeout = 100 * time.Millisecond

	transport := newMockTransport()
	mgr := NewManager(Params{
		Config:    cfg,
		Transport: transport,
	})
	mgr.Start()
	defer mgr.Stop()

	evCh := mgr.Events().Subscribe(10)
	defer mgr.Events().Unsubscribe(evCh)

	_ = mgr.Register("conn-1", "user-1", "room-1", "sess-1", "token-1", nil)
	<-evCh // online

	// 1. Check Idle Transition
	time.Sleep(50 * time.Millisecond)
	mgr.runSweep()

	select {
	case ev := <-evCh:
		if ev.Type != events.UserIdle {
			t.Errorf("Expected UserIdle event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for idle transition")
	}

	p, _ := mgr.Registry().Get("conn-1")
	if p.State != StateIdle {
		t.Errorf("Expected StateIdle, got %v", p.State)
	}

	// 2. Check Away Transition
	time.Sleep(50 * time.Millisecond)
	mgr.runSweep()

	select {
	case ev := <-evCh:
		if ev.Type != events.UserAway {
			t.Errorf("Expected UserAway event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for away transition")
	}

	p, _ = mgr.Registry().Get("conn-1")
	if p.State != StateAway {
		t.Errorf("Expected StateAway, got %v", p.State)
	}

	// 3. Check Heartbeat Timeout
	// Manually age the heartbeat to trigger a timeout
	_, _ = mgr.Registry().Mutate("conn-1", func(pr *types.Presence) error {
		pr.LastHeartbeat = time.Now().Add(-3 * time.Second)
		return nil
	})
	mgr.runSweep()

	select {
	case ev := <-evCh:
		if ev.Type != events.HeartbeatTimeout {
			t.Errorf("Expected HeartbeatTimeout event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for heartbeat timeout event")
	}

	// Wait for another offline event because it transitions to offline recovery first
	select {
	case ev := <-evCh:
		if ev.Type != events.PresenceOffline || ev.Payload != "heartbeat_timeout" {
			t.Errorf("Expected offline timeout event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for offline timeout event")
	}

	// 4. Check Recovery Expiration Eviction
	// Manually age the last activity/heartbeat to exceed the recovery window
	_, _ = mgr.Registry().Mutate("conn-1", func(pr *types.Presence) error {
		pr.LastActivity = time.Now().Add(-200 * time.Millisecond)
		pr.LastHeartbeat = time.Now().Add(-200 * time.Millisecond)
		return nil
	})
	mgr.runSweep()

	select {
	case ev := <-evCh:
		if ev.Type != events.PresenceOffline || ev.Payload != "recovery_expired" {
			t.Errorf("Expected recovery expired offline event, got: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for recovery expiration event")
	}

	_, ok := mgr.Registry().Get("conn-1")
	if ok {
		t.Error("Expected presence to be evicted after recovery timeout")
	}
}

func TestBroadcastThrottling(t *testing.T) {
	cfg := config.Default()
	cfg.BroadcastInterval = 30 * time.Millisecond

	transport := newMockTransport()
	mgr := NewManager(Params{
		Config:    cfg,
		Transport: transport,
	})
	mgr.Start()
	defer mgr.Stop()

	_ = mgr.Register("conn-1", "user-1", "room-1", "sess-1", "token-1", nil)

	// Trigger a series of cursor updates rapidly
	for i := 0; i < 5; i++ {
		_ = mgr.UpdateCursor("conn-1", i, 0, "", "", true)
	}

	// Wait for a broadcast tick
	time.Sleep(60 * time.Millisecond)

	sends := transport.getSends("conn-1")
	if len(sends) == 0 {
		t.Fatal("Expected broadcasts to be delivered to room members")
	}

	// The last broadcast should carry the final cursor state (incremental sync frame)
	lastBytes := sends[len(sends)-1]
	var frame awareness.Frame
	err := json.Unmarshal(lastBytes, &frame)
	if err != nil {
		t.Fatalf("Failed to unmarshal frame: %v", err)
	}

	if frame.Type != awareness.SyncIncremental {
		t.Errorf("Expected incremental sync, got %s", frame.Type)
	}

	// Ensure it contains the last cursor update (line = 4)
	foundCursor := false
	for _, p := range frame.Presences {
		if p.ConnID == "conn-1" {
			if p.Cursor.Line == 4 {
				foundCursor = true
			}
		}
	}

	if !foundCursor {
		t.Error("Expected incremental sync frame to carry latest cursor position")
	}
}

func TestConcurrencyAndRace(t *testing.T) {
	cfg := config.Default()
	cfg.BroadcastInterval = 5 * time.Millisecond

	transport := newMockTransport()
	mgr := NewManager(Params{
		Config:    cfg,
		Transport: transport,
	})
	mgr.Start()
	defer mgr.Stop()

	var wg sync.WaitGroup
	workers := 20
	actionsPerWorker := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			connID := "conn-worker-" + string(rune(workerID))
			userID := "user-worker-" + string(rune(workerID))

			_ = mgr.Register(connID, userID, "room-1", "sess-"+connID, "token-"+connID, nil)

			for a := 0; a < actionsPerWorker; a++ {
				_ = mgr.UpdateCursor(connID, a, a, "", "", true)
				_ = mgr.UpdateSelection(connID, a, 0, a, 10)
				_ = mgr.UpdateTyping(connID, true)
				_ = mgr.Heartbeat(connID)
			}

			mgr.Deregister(connID, "done")
		}(i)
	}

	wg.Wait()
}
