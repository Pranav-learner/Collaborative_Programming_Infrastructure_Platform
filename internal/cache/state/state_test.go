package state_test

import (
	"context"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/manager"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/sessions"
	"cpip/internal/cache/state"
)

func sessionsParams() sessions.CreateParams {
	return sessions.CreateParams{UserID: "u1", DeviceID: "laptop"}
}

func newState(t *testing.T) *state.Manager {
	t.Helper()
	em := redis.NewEmulator()
	m, err := state.New(state.Params{Config: config.Default(), Client: em})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	return m
}

func TestStatePutGet(t *testing.T) {
	ctx := context.Background()
	m := newState(t)

	type worker struct {
		ID     string
		Status string
	}
	if err := m.PutState(ctx, state.NamespaceWorkerState, "w1", worker{ID: "w1", Status: "busy"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	var got worker
	found, err := m.GetState(ctx, state.NamespaceWorkerState, "w1", &got)
	if err != nil || !found {
		t.Fatalf("get state: found=%v err=%v", found, err)
	}
	if got.Status != "busy" {
		t.Fatalf("got %+v", got)
	}
	found, _ = m.GetState(ctx, state.NamespaceWorkerState, "missing", &got)
	if found {
		t.Fatal("expected miss")
	}
}

func TestStateCompareAndSwap(t *testing.T) {
	ctx := context.Background()
	m := newState(t)

	// CAS with expected absence creates it.
	ok, err := m.CompareAndSwapState(ctx, state.NamespaceExecutionStatus, "e1", nil, "running", time.Minute)
	if err != nil || !ok {
		t.Fatalf("initial CAS: ok=%v err=%v", ok, err)
	}
	// CAS with wrong expected fails.
	ok, _ = m.CompareAndSwapState(ctx, state.NamespaceExecutionStatus, "e1", "queued", "done", time.Minute)
	if ok {
		t.Fatal("CAS with wrong expected should fail")
	}
	// CAS with right expected succeeds.
	ok, _ = m.CompareAndSwapState(ctx, state.NamespaceExecutionStatus, "e1", "running", "done", time.Minute)
	if !ok {
		t.Fatal("CAS with right expected should succeed")
	}
	var status string
	m.GetState(ctx, state.NamespaceExecutionStatus, "e1", &status)
	if status != "done" {
		t.Fatalf("status = %q", status)
	}
}

func TestStateRoomMembership(t *testing.T) {
	ctx := context.Background()
	m := newState(t)

	m.AddRoomMember(ctx, "room1", "alice")
	m.AddRoomMember(ctx, "room1", "bob")
	members, err := m.RoomMembers(ctx, "room1")
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Fatalf("members = %v", members)
	}
	is, _ := m.IsRoomMember(ctx, "room1", "alice")
	if !is {
		t.Fatal("alice should be a member")
	}
	m.RemoveRoomMember(ctx, "room1", "alice")
	is, _ = m.IsRoomMember(ctx, "room1", "alice")
	if is {
		t.Fatal("alice should have been removed")
	}
}

// The Distributed State Manager wires the full stack; smoke-test that its
// subsystem facades are usable end to end.
func TestStateManagerSubsystems(t *testing.T) {
	ctx := context.Background()
	m := newState(t)

	// Cache
	if err := m.CacheManager().RegisterCache(manager.CacheSpec{Name: "t", TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if err := m.Cache().Set(ctx, "t", "k", "v"); err != nil {
		t.Fatal(err)
	}
	var v string
	if found, _ := m.Cache().Get(ctx, "t", "k", &v); !found || v != "v" {
		t.Fatal("cache facade round-trip failed")
	}

	// Sessions
	sess, err := m.Sessions().Create(ctx, sessionsParams())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Sessions().Get(ctx, sess.ID); err != nil {
		t.Fatal(err)
	}

	// Locks
	l, err := m.Locks().Acquire(ctx, "res", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Release(ctx)

	if m.Health(ctx) != "up" {
		t.Fatal("expected healthy state manager")
	}
}
