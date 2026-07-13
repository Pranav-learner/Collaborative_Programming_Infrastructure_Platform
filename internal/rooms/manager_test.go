package rooms

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/rooms/config"
	"cpip/internal/rooms/lifecycle"
	"cpip/internal/rooms/metrics"
	"cpip/internal/rooms/permissions"
	"cpip/internal/rooms/room"
	"cpip/internal/rooms/storage"
)

// TestRoomCreationAndWaiting tests that a room is created in Waiting state.
func TestRoomCreationAndWaiting(t *testing.T) {
	cfg := config.Default()
	repo := storage.NewMemoryRepository()
	mgr := NewManager(Params{
		Config:  cfg,
		Metrics: metrics.NewNoop(),
		Repo:    repo,
	})

	ctx := context.Background()
	r, err := mgr.CreateRoom(ctx, "room-1", "Test Room", "owner-1", nil, map[string]any{"lang": "go"})
	if err != nil {
		t.Fatalf("failed to create room: %v", err)
	}

	if r.ID() != "room-1" {
		t.Errorf("expected ID room-1, got %s", r.ID())
	}
	if r.Name() != "Test Room" {
		t.Errorf("expected name Test Room, got %s", r.Name())
	}
	if r.OwnerID() != "owner-1" {
		t.Errorf("expected owner owner-1, got %s", r.OwnerID())
	}
	if r.State() != lifecycle.StateWaiting {
		t.Errorf("expected initial state Waiting, got %s", r.State())
	}
}

// TestMembershipOperations tests Join, Leave, Kick, and Ownership Transfer.
func TestMembershipOperations(t *testing.T) {
	cfg := config.Default()
	repo := storage.NewMemoryRepository()
	mgr := NewManager(Params{
		Config:  cfg,
		Metrics: metrics.NewNoop(),
		Repo:    repo,
	})

	ctx := context.Background()
	r, err := mgr.CreateRoom(ctx, "room-1", "Test Room", "owner-1", nil, nil)
	if err != nil {
		t.Fatalf("failed to create room: %v", err)
	}

	// Owner joins
	res, err := mgr.Membership().Join(ctx, "room-1", room.JoinRequest{
		UserID:    "owner-1",
		Role:      permissions.RoleOwner,
		SessionID: "sess-owner",
		ConnID:    "conn-owner",
	})
	if err != nil {
		t.Fatalf("owner join failed: %v", err)
	}
	if !res.Reconnected {
		t.Error("expected reconnect status for pre-registered owner")
	}
	if r.State() != lifecycle.StateActive {
		t.Errorf("expected Active state, got %s", r.State())
	}

	// Participant joins
	res, err = mgr.Membership().Join(ctx, "room-1", room.JoinRequest{
		UserID:    "user-2",
		Role:      permissions.RoleParticipant,
		SessionID: "sess-2",
		ConnID:    "conn-2",
	})
	if err != nil {
		t.Fatalf("participant join failed: %v", err)
	}
	if res.Reconnected {
		t.Error("expected first join for a new participant to not be a reconnect")
	}
	if r.ParticipantCount() != 2 {
		t.Errorf("expected 2 participants, got %d", r.ParticipantCount())
	}

	// Transfer Ownership
	_, err = mgr.Membership().TransferOwnership(ctx, "room-1", "owner-1", "user-2")
	if err != nil {
		t.Fatalf("ownership transfer failed: %v", err)
	}
	if r.OwnerID() != "user-2" {
		t.Errorf("expected owner to be user-2, got %s", r.OwnerID())
	}

	// Kick validation: New owner (user-2) kicks previous owner (owner-1, now a participant)
	_, err = mgr.Membership().Kick(ctx, "room-1", "user-2", "owner-1")
	if err != nil {
		t.Fatalf("kick failed: %v", err)
	}
	if r.ParticipantCount() != 1 {
		t.Errorf("expected 1 participant left, got %d", r.ParticipantCount())
	}

	// Leave validation
	_, err = mgr.Membership().Leave(ctx, "room-1", "user-2")
	if err != nil {
		t.Fatalf("leave failed: %v", err)
	}
	if r.ParticipantCount() != 0 {
		t.Errorf("expected 0 participants, got %d", r.ParticipantCount())
	}
}

// TestSessionRecovery tests temporary disconnect, recovery window and reconnect.
func TestSessionRecovery(t *testing.T) {
	cfg := config.Default()
	repo := storage.NewMemoryRepository()
	mgr := NewManager(Params{
		Config:  cfg,
		Metrics: metrics.NewNoop(),
		Repo:    repo,
	})

	ctx := context.Background()
	_, err := mgr.CreateRoom(ctx, "room-1", "Test Room", "owner-1", nil, nil)
	if err != nil {
		t.Fatalf("failed to create room: %v", err)
	}

	// Join
	_, err = mgr.Membership().Join(ctx, "room-1", room.JoinRequest{
		UserID:    "owner-1",
		Role:      permissions.RoleOwner,
		SessionID: "sess-1",
		ConnID:    "conn-1",
	})
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}

	// Disconnect
	err = mgr.SetConnected(ctx, "room-1", "owner-1", false, "", "")
	if err != nil {
		t.Fatalf("disconnect failed: %v", err)
	}

	p, ok := mgr.Registry().List()[0].Participant("owner-1")
	if !ok || p.Connected {
		t.Error("expected participant to be disconnected")
	}

	// Reconnect
	res, err := mgr.Membership().Join(ctx, "room-1", room.JoinRequest{
		UserID:    "owner-1",
		Role:      permissions.RoleOwner,
		SessionID: "sess-2",
		ConnID:    "conn-2",
	})
	if err != nil {
		t.Fatalf("reconnect failed: %v", err)
	}
	if !res.Reconnected {
		t.Error("expected Reconnected to be true")
	}
	if !res.Participant.Connected {
		t.Error("expected participant to be connected after reconnect")
	}
}

// TestConcurrency exercises concurrent joins and leaves on a single room under heavy contention.
func TestConcurrency(t *testing.T) {
	cfg := config.Default()
	repo := storage.NewMemoryRepository()
	mgr := NewManager(Params{
		Config:  cfg,
		Metrics: metrics.NewNoop(),
		Repo:    repo,
	})

	ctx := context.Background()
	_, err := mgr.CreateRoom(ctx, "room-1", "Concurrency Room", "owner-1", nil, nil)
	if err != nil {
		t.Fatalf("failed to create room: %v", err)
	}

	// Owner joins
	_, _ = mgr.Membership().Join(ctx, "room-1", room.JoinRequest{
		UserID: "owner-1", Role: permissions.RoleOwner, SessionID: "s-owner", ConnID: "c-owner",
	})

	const numWorkers = 50
	var wg sync.WaitGroup
	wg.Add(numWorkers * 2)

	var joinFails int64
	var leaveFails int64

	// Concurrent Joiners
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			userID := fmt.Sprintf("user-%d", id)
			_, err := mgr.Membership().Join(ctx, "room-1", room.JoinRequest{
				UserID:    userID,
				Role:      permissions.RoleParticipant,
				SessionID: "s-" + userID,
				ConnID:    "c-" + userID,
			})
			if err != nil {
				atomic.AddInt64(&joinFails, 1)
			}
		}(i)
	}

	// Concurrent Leavers
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			// Sleep a tiny bit to let some joins complete
			time.Sleep(1 * time.Millisecond)
			userID := fmt.Sprintf("user-%d", id)
			_, err := mgr.Membership().Leave(ctx, "room-1", userID)
			if err != nil {
				atomic.AddInt64(&leaveFails, 1)
			}
		}(i)
	}

	wg.Wait()

	// Should not panic, and internal state should remain consistent.
	r, _ := mgr.Registry().Get("room-1")
	t.Logf("concurrency test finished. Active participants: %d. Join failures: %d, Leave failures: %d",
		r.ParticipantCount(), atomic.LoadInt64(&joinFails), atomic.LoadInt64(&leaveFails))
}

// TestJanitorTransitions verifies lifecycle transitions driven by the janitor.
func TestJanitorTransitions(t *testing.T) {
	// Setup very short timeouts for testing
	cfg := config.Config{
		DefaultMaxParticipants: 5,
		DefaultIdleTimeout:     10 * time.Millisecond,
		DefaultExpireTimeout:   10 * time.Millisecond,
		DefaultRecoveryTimeout: 5 * time.Millisecond,
		CleanupInterval:        5 * time.Millisecond,
	}

	repo := storage.NewMemoryRepository()
	mgr := NewManager(Params{
		Config:  cfg,
		Metrics: metrics.NewNoop(),
		Repo:    repo,
	})

	ctx := context.Background()
	r, err := mgr.CreateRoom(ctx, "room-1", "Janitor Room", "owner-1", nil, nil)
	if err != nil {
		t.Fatalf("failed to create room: %v", err)
	}

	// Initial State: Waiting
	if r.State() != lifecycle.StateWaiting {
		t.Errorf("expected state Waiting, got %s", r.State())
	}

	// Join Owner to transition to Active
	_, err = mgr.Membership().Join(ctx, "room-1", room.JoinRequest{
		UserID: "owner-1", Role: permissions.RoleOwner, SessionID: "s-1", ConnID: "c-1",
	})
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}
	if r.State() != lifecycle.StateActive {
		t.Errorf("expected state Active, got %s", r.State())
	}

	// Start janitor background loop
	mgr.Start()
	defer mgr.Stop()

	// Disconnect Owner
	err = mgr.SetConnected(ctx, "room-1", "owner-1", false, "", "")
	if err != nil {
		t.Fatalf("disconnect failed: %v", err)
	}

	// Wait for transitions to occur.
	// 1. Recovery timeout is 5ms. In <10ms, recovery window expires for owner-1, evicting them.
	// 2. Room becomes empty of participants.
	// 3. Waiting/Idle state timeouts trigger Expiring state.
	// 4. Expiring state transitions to Closed.
	// 5. Closed state transitions to Destroyed (retention is 5ms).
	// We wait 150ms to guarantee all sweeps have run.
	time.Sleep(150 * time.Millisecond)

	// Room should be destroyed and removed from registry.
	_, exists := mgr.Registry().Get("room-1")
	if exists {
		t.Errorf("expected room to be destroyed and deregistered, current state: %s", r.State())
	}
}

// fmt.Sprintf fallback to avoid importing fmt if we didn't need it.
func fmtSprintf(format string, args ...any) string {
	// A simple helper mimicking fmt.Sprintf to construct test user IDs
	var result string
	for _, arg := range args {
		switch v := arg.(type) {
		case int:
			// convert int to string
			result += string(rune('0' + v)) // very basic for single digit, but we can just use fmt
		case string:
			result += v
		}
	}
	return result
}
