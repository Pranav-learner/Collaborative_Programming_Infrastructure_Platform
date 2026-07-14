package replication_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/keys"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/replication"
)

func mkReplicator(be backend.Backend, node string) *replication.Replicator {
	return replication.New(replication.Params{
		Backend: be, Keys: keys.New("t", "c"), Config: config.Default().Replication,
		NodeID: node, Events: events.NewBus(), Logger: logger.New(nil),
	})
}

func TestCrossNodeReplication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	be := backend.NewMemory() // shared backend = shared cluster bus

	a := mkReplicator(be, "A")
	b := mkReplicator(be, "B")
	a.Start(ctx)
	b.Start(ctx)
	defer a.Close()
	defer b.Close()

	received := make(chan replication.Update, 4)
	if err := b.OnUpdate(ctx, replication.DomainPresence, func(u replication.Update) { received <- u }); err != nil {
		t.Fatal(err)
	}

	payload, _ := json.Marshal(map[string]string{"user": "alice", "room": "r1"})
	if err := a.Broadcast(ctx, replication.Update{
		Domain: replication.DomainPresence, Key: "alice", Payload: payload, Version: 1,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case u := <-received:
		if u.Origin != "A" || u.Key != "alice" {
			t.Fatalf("unexpected update: %+v", u)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("B did not receive A's replicated update")
	}
}

func TestReplicationDedupsSelfEchoes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	be := backend.NewMemory()

	a := mkReplicator(be, "A")
	a.Start(ctx)
	defer a.Close()

	var got int32
	var mu sync.Mutex
	if err := a.OnUpdate(ctx, replication.DomainRoom, func(replication.Update) {
		mu.Lock()
		got++
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	// A broadcasts to a domain it also subscribes to; it must NOT apply its own echo.
	_ = a.Broadcast(ctx, replication.Update{Domain: replication.DomainRoom, Key: "r1", Version: 1})
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if got != 0 {
		t.Fatalf("self-echo should be deduped, applied %d times", got)
	}
}

func TestLWWMerger(t *testing.T) {
	m := replication.LWWMerger{}
	now := time.Now()
	cur := replication.Update{Version: 1, Timestamp: now}
	// Higher version wins.
	if w, changed := m.Merge(replication.DomainNode, cur, replication.Update{Version: 2, Timestamp: now.Add(-time.Hour)}); !changed || w.Version != 2 {
		t.Fatalf("higher version should win")
	}
	// Lower version loses.
	if _, changed := m.Merge(replication.DomainNode, cur, replication.Update{Version: 0, Timestamp: now.Add(time.Hour)}); changed {
		t.Fatalf("lower version must not win")
	}
	// Same version, later timestamp wins.
	if w, changed := m.Merge(replication.DomainNode, cur, replication.Update{Version: 1, Timestamp: now.Add(time.Second)}); !changed || !w.Timestamp.After(now) {
		t.Fatalf("later timestamp should win on version tie")
	}
}
