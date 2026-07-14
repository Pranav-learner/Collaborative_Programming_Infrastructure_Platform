package replication_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cpip/internal/cache/redis"
	"cpip/internal/cache/replication"
)

// Two replicators sharing one emulated Redis model two CPIP nodes. An update
// broadcast by node A must be applied on node B, but never echoed back to A.
func TestReplicationCrossNode(t *testing.T) {
	ctx := context.Background()
	em := redis.NewEmulator()

	a := replication.New(replication.Params{Client: em, ChannelPrefix: "repl", NodeID: "A"})
	b := replication.New(replication.Params{Client: em, ChannelPrefix: "repl", NodeID: "B"})
	defer a.Close()
	defer b.Close()

	var (
		mu       sync.Mutex
		appliedB []replication.Update
		appliedA []replication.Update
	)
	if err := b.Subscribe("ns", func(u replication.Update) {
		mu.Lock()
		appliedB = append(appliedB, u)
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.Subscribe("ns", func(u replication.Update) {
		mu.Lock()
		appliedA = append(appliedA, u)
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // let subscriptions establish

	if err := a.Broadcast(ctx, replication.Update{Namespace: "ns", ID: "x", Payload: "hello"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(appliedB) != 1 || appliedB[0].Payload != "hello" {
		t.Fatalf("node B applied = %+v, want 1 update", appliedB)
	}
	if len(appliedA) != 0 {
		t.Fatalf("node A should not apply its own echo, got %+v", appliedA)
	}
}

// LWW: an older-versioned update arriving after a newer one is dropped.
func TestReplicationLWWDropsStale(t *testing.T) {
	ctx := context.Background()
	em := redis.NewEmulator()
	a := replication.New(replication.Params{Client: em, ChannelPrefix: "repl", NodeID: "A"})
	b := replication.New(replication.Params{Client: em, ChannelPrefix: "repl", NodeID: "B"})
	defer a.Close()
	defer b.Close()

	var (
		mu       sync.Mutex
		payloads []string
	)
	b.Subscribe("ns", func(u replication.Update) {
		mu.Lock()
		payloads = append(payloads, u.Payload)
		mu.Unlock()
	})
	time.Sleep(20 * time.Millisecond)

	// Newer version first, then an older one for the same id.
	a.Broadcast(ctx, replication.Update{Namespace: "ns", ID: "k", Payload: "new", Version: 100})
	time.Sleep(20 * time.Millisecond)
	a.Broadcast(ctx, replication.Update{Namespace: "ns", ID: "k", Payload: "old", Version: 50})
	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(payloads) != 1 || payloads[0] != "new" {
		t.Fatalf("LWW failed: applied = %v (stale update should be dropped)", payloads)
	}
}
