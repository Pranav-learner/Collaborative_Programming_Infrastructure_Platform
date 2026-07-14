package invalidation_test

import (
	"context"
	"testing"
	"time"

	"cpip/internal/cache/invalidation"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/redis"
)

func newInval(t *testing.T) (*invalidation.Manager, *redis.Emulator, keys.Builder) {
	t.Helper()
	em := redis.NewEmulator()
	kb := keys.New("cpip")
	m := invalidation.New(invalidation.Params{Client: em, Keys: kb, NodeID: "node-a"})
	return m, em, kb
}

func TestInvalidateKeyAndBulk(t *testing.T) {
	ctx := context.Background()
	m, em, kb := newInval(t)

	em.Set(ctx, kb.Cache("c", "a"), "1", time.Minute)
	em.Set(ctx, kb.Cache("c", "b"), "2", time.Minute)

	if err := m.InvalidateKey(ctx, kb.Cache("c", "a")); err != nil {
		t.Fatal(err)
	}
	if n, _ := em.Exists(ctx, kb.Cache("c", "a")); n != 0 {
		t.Fatal("key a should be gone")
	}
	if err := m.InvalidateBulk(ctx, []string{kb.Cache("c", "b")}); err != nil {
		t.Fatal(err)
	}
	if em.KeyCount() != 0 {
		t.Fatalf("expected empty, got %d keys", em.KeyCount())
	}
}

func TestInvalidatePattern(t *testing.T) {
	ctx := context.Background()
	m, em, kb := newInval(t)
	em.Set(ctx, kb.Cache("room", "1"), "a", time.Minute)
	em.Set(ctx, kb.Cache("room", "2"), "b", time.Minute)
	em.Set(ctx, kb.Cache("doc", "1"), "c", time.Minute)

	n, err := m.InvalidatePattern(ctx, kb.CachePattern("room"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("invalidated %d, want 2", n)
	}
	if left, _ := em.Exists(ctx, kb.Cache("doc", "1")); left != 1 {
		t.Fatal("doc cache should be untouched")
	}
}

func TestInvalidateTag(t *testing.T) {
	ctx := context.Background()
	m, em, kb := newInval(t)

	// Two keys tagged "user:7".
	k1, k2 := kb.Cache("profile", "p1"), kb.Cache("session", "s1")
	em.Set(ctx, k1, "a", time.Minute)
	em.Set(ctx, k2, "b", time.Minute)
	if err := m.IndexTags(ctx, k1, []string{"user:7"}); err != nil {
		t.Fatal(err)
	}
	if err := m.IndexTags(ctx, k2, []string{"user:7"}); err != nil {
		t.Fatal(err)
	}

	n, err := m.InvalidateTag(ctx, "user:7")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("tag invalidated %d keys, want 2", n)
	}
	if em.KeyCount() != 0 {
		t.Fatalf("expected all tagged keys + tag set gone, got %d", em.KeyCount())
	}
}

// A remote broadcast on the shared channel must fire local hooks on other nodes.
func TestEventDrivenLocalHook(t *testing.T) {
	ctx := context.Background()
	em := redis.NewEmulator()
	kb := keys.New("cpip")

	nodeA := invalidation.New(invalidation.Params{Client: em, Keys: kb, NodeID: "A"})
	nodeB := invalidation.New(invalidation.Params{Client: em, Keys: kb, NodeID: "B"})

	evicted := make(chan []string, 1)
	nodeB.OnInvalidate(func(fullKeys []string) { evicted <- fullKeys })
	if err := nodeB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer nodeB.Stop()
	time.Sleep(20 * time.Millisecond)

	em.Set(ctx, kb.Cache("c", "x"), "v", time.Minute)
	if err := nodeA.InvalidateKey(ctx, kb.Cache("c", "x")); err != nil {
		t.Fatal(err)
	}

	select {
	case keys := <-evicted:
		if len(keys) != 1 || keys[0] != kb.Cache("c", "x") {
			t.Fatalf("hook got %v", keys)
		}
	case <-time.After(time.Second):
		t.Fatal("node B local hook never fired for remote invalidation")
	}
}
