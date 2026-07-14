package presence_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/presence"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/replication"
)

func newPresence(t *testing.T, em *redis.Emulator, nodeID string) (*presence.Manager, *replication.Replicator) {
	t.Helper()
	cfg := config.Default()
	repl := replication.New(replication.Params{Client: em, ChannelPrefix: cfg.Replication.ChannelPrefix, NodeID: nodeID})
	pm := presence.New(presence.Params{
		Client:      em,
		Replicator:  repl,
		Keys:        keys.New("cpip"),
		Config:      cfg.Replication,
		PresenceTTL: cfg.TTL.Presence,
		NodeID:      nodeID,
	})
	return pm, repl
}

func TestPresenceAnnounceAndRead(t *testing.T) {
	ctx := context.Background()
	em := redis.NewEmulator()
	pm, repl := newPresence(t, em, "A")
	defer repl.Close()
	defer pm.Close()

	err := pm.Announce(ctx, presence.Presence{RoomID: "r1", UserID: "u1", State: "online"})
	if err != nil {
		t.Fatal(err)
	}
	pm.UpdateCursor(ctx, "r1", "u1", `{"line":10}`)
	pm.SetTyping(ctx, "r1", "u1", true)

	list, err := pm.GetRoom(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("room has %d members, want 1", len(list))
	}
	if list[0].Cursor != `{"line":10}` || !list[0].Typing {
		t.Fatalf("presence not updated: %+v", list[0])
	}
}

func TestPresenceLeaveRemoves(t *testing.T) {
	ctx := context.Background()
	em := redis.NewEmulator()
	pm, repl := newPresence(t, em, "A")
	defer repl.Close()
	defer pm.Close()

	pm.Announce(ctx, presence.Presence{RoomID: "r1", UserID: "u1", State: "online"})
	pm.Announce(ctx, presence.Presence{RoomID: "r1", UserID: "u2", State: "online"})
	if err := pm.Leave(ctx, "r1", "u1"); err != nil {
		t.Fatal(err)
	}
	list, _ := pm.GetRoom(ctx, "r1")
	if len(list) != 1 || list[0].UserID != "u2" {
		t.Fatalf("after leave, room = %+v", list)
	}
}

// Presence announced on node A must replicate to a subscriber on node B.
func TestPresenceReplicatesCrossNode(t *testing.T) {
	ctx := context.Background()
	em := redis.NewEmulator()
	pmA, replA := newPresence(t, em, "A")
	pmB, replB := newPresence(t, em, "B")
	defer replA.Close()
	defer replB.Close()
	defer pmA.Close()
	defer pmB.Close()

	var (
		mu   sync.Mutex
		gotB []presence.Presence
	)
	if err := pmB.Subscribe("r1", func(p presence.Presence) {
		mu.Lock()
		gotB = append(gotB, p)
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	if err := pmA.Announce(ctx, presence.Presence{RoomID: "r1", UserID: "u1", State: "online"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(gotB) == 0 {
		t.Fatal("node B did not receive replicated presence")
	}
	if gotB[len(gotB)-1].UserID != "u1" {
		t.Fatalf("unexpected replicated presence: %+v", gotB)
	}
	// And node B's local view should reflect it.
	if len(pmB.GetRoomLocal("r1")) != 1 {
		t.Fatal("node B local view not updated")
	}
}

func TestPresenceHeartbeatKeepsAlive(t *testing.T) {
	ctx := context.Background()
	em := redis.NewEmulator()
	now := time.Now()
	em.SetClock(func() time.Time { return now })
	pm, repl := newPresence(t, em, "A")
	defer repl.Close()
	defer pm.Close()

	pm.Announce(ctx, presence.Presence{RoomID: "r1", UserID: "u1", State: "online"})
	// Advance almost to expiry, then heartbeat.
	now = now.Add(25 * time.Second)
	if err := pm.Heartbeat(ctx, "r1", "u1"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(25 * time.Second) // would have expired without heartbeat
	list, _ := pm.GetRoom(ctx, "r1")
	if len(list) != 1 {
		t.Fatalf("heartbeat did not keep presence alive: %d members", len(list))
	}
}
