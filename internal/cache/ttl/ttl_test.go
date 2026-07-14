package ttl_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/ttl"
)

func TestResolvePerCacheAndDefault(t *testing.T) {
	cfg := config.Default().TTL
	cfg.Jitter = 0
	m := ttl.New(cfg, nil, nil)
	m.Register("short", ttl.Policy{TTL: time.Second, Mode: ttl.Absolute})

	if got := m.Resolve("short", 0); got != time.Second {
		t.Fatalf("per-cache TTL = %v", got)
	}
	if got := m.Resolve("unknown", 0); got != cfg.Default {
		t.Fatalf("default TTL = %v, want %v", got, cfg.Default)
	}
	if got := m.Resolve("short", 5*time.Second); got != 5*time.Second {
		t.Fatalf("override TTL = %v", got)
	}
}

func TestJitterBounded(t *testing.T) {
	cfg := config.Default().TTL
	m := ttl.New(cfg, nil, nil)
	base := 100 * time.Second
	for _, seed := range []string{"a", "b", "c", "key:42", "room:1"} {
		got := m.Jitter(base, 0.1, seed)
		if got < 90*time.Second || got > 110*time.Second {
			t.Fatalf("jitter(%q) = %v out of ±10%% bounds", seed, got)
		}
	}
}

func TestSlidingMode(t *testing.T) {
	cfg := config.Default().TTL
	m := ttl.New(cfg, nil, nil)
	m.Register("sess", ttl.Policy{TTL: time.Minute, Mode: ttl.Sliding})
	if !m.IsSliding("sess") {
		t.Fatal("expected sliding mode")
	}
	if m.IsSliding("unknown") {
		t.Fatal("unknown cache should not be sliding")
	}
}

func TestWatchExpirationCallback(t *testing.T) {
	cfg := config.Default().TTL
	cfg.ReaperInterval = 10 * time.Millisecond
	m := ttl.New(cfg, nil, nil)

	now := time.Now()
	m.SetClock(func() time.Time { return now })

	var fired atomic.Int32
	m.Watch("k1", 50*time.Millisecond, func(key string) {
		if key == "k1" {
			fired.Add(1)
		}
	})
	if m.WatchedCount() != 1 {
		t.Fatalf("watched = %d", m.WatchedCount())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)

	// Advance the clock past the deadline; the reaper should fire the callback.
	now = now.Add(100 * time.Millisecond)
	deadline := time.After(time.Second)
	for fired.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("expiration callback never fired")
		case <-time.After(5 * time.Millisecond):
		}
	}
	m.Stop()
	if m.WatchedCount() != 0 {
		t.Fatalf("expired key not removed: watched = %d", m.WatchedCount())
	}
}

func TestTouchExtendsDeadline(t *testing.T) {
	cfg := config.Default().TTL
	m := ttl.New(cfg, nil, nil)
	now := time.Now()
	m.SetClock(func() time.Time { return now })

	m.Watch("k", 50*time.Millisecond, nil)
	now = now.Add(40 * time.Millisecond)
	m.Touch("k", 50*time.Millisecond) // reset deadline
	now = now.Add(40 * time.Millisecond)
	// Total 80ms elapsed but touched at 40ms → still alive.
	m.Sweep()
	if m.WatchedCount() != 1 {
		t.Fatal("touch did not extend the deadline")
	}
}
