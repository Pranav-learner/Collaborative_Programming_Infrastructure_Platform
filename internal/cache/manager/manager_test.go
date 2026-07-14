package manager_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/manager"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/policies"
	"cpip/internal/cache/redis"
)

func newManager(t *testing.T) (*manager.Manager, *redis.Emulator, *metrics.InMemoryRecorder) {
	t.Helper()
	em := redis.NewEmulator()
	rec := metrics.NewInMemoryRecorder()
	m, err := manager.New(manager.Params{
		Config:  config.Default(),
		Client:  em,
		Metrics: rec,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return m, em, rec
}

func TestManagerSetGetDelete(t *testing.T) {
	ctx := context.Background()
	m, _, rec := newManager(t)
	if err := m.RegisterCache(manager.CacheSpec{Name: "rooms", TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}

	type room struct {
		ID   string
		Name string
	}
	if err := m.Set(ctx, "rooms", "r1", room{ID: "r1", Name: "Lobby"}); err != nil {
		t.Fatal(err)
	}

	var got room
	found, err := m.Get(ctx, "rooms", "r1", &got)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.Name != "Lobby" {
		t.Fatalf("got %+v", got)
	}

	// Miss.
	found, _ = m.Get(ctx, "rooms", "nope", &got)
	if found {
		t.Fatal("expected miss")
	}

	if err := m.Delete(ctx, "rooms", "r1"); err != nil {
		t.Fatal(err)
	}
	found, _ = m.Get(ctx, "rooms", "r1", &got)
	if found {
		t.Fatal("expected miss after delete")
	}

	stats, _ := m.Stats("rooms")
	if stats.Hits != 1 || stats.Misses != 2 || stats.Sets != 1 {
		t.Fatalf("unexpected stats %+v", stats)
	}
	if rec.Counter(metrics.MetricCacheHit) != 1 {
		t.Fatalf("hit metric = %v", rec.Counter(metrics.MetricCacheHit))
	}
}

func TestManagerUnregisteredCacheRejected(t *testing.T) {
	ctx := context.Background()
	m, _, _ := newManager(t)
	if err := m.Set(ctx, "ghost", "k", "v"); err == nil {
		t.Fatal("expected error setting unregistered cache")
	}
}

func TestReadThroughPolicy(t *testing.T) {
	ctx := context.Background()
	m, _, _ := newManager(t)

	var loads atomic.Int64
	err := m.RegisterCache(manager.CacheSpec{
		Name:     "users",
		Strategy: policies.ReadThrough,
		TTL:      time.Minute,
		Loader: func(ctx context.Context, key string) (string, time.Duration, bool, error) {
			loads.Add(1)
			return `{"name":"loaded"}`, time.Minute, true, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var out struct{ Name string }
	found, err := m.Get(ctx, "users", "u1", &out)
	if err != nil || !found || out.Name != "loaded" {
		t.Fatalf("read-through miss-load failed: found=%v err=%v out=%+v", found, err, out)
	}
	// Second get should hit cache, not loader.
	found, _ = m.Get(ctx, "users", "u1", &out)
	if !found {
		t.Fatal("expected cache hit")
	}
	if loads.Load() != 1 {
		t.Fatalf("loader called %d times, want 1", loads.Load())
	}
}

func TestWriteThroughPolicy(t *testing.T) {
	ctx := context.Background()
	m, _, _ := newManager(t)

	written := make(map[string]string)
	var mu sync.Mutex
	err := m.RegisterCache(manager.CacheSpec{
		Name:     "profiles",
		Strategy: policies.WriteThrough,
		TTL:      time.Minute,
		Writer: func(ctx context.Context, key, value string) error {
			mu.Lock()
			written[key] = value
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Set(ctx, "profiles", "p1", "hello"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if written["p1"] != "hello" {
		t.Fatalf("write-through did not persist: %v", written)
	}
}

func TestWriteBehindPolicy(t *testing.T) {
	ctx := context.Background()
	em := redis.NewEmulator()
	cfg := config.Default()
	cfg.Policy.WriteBehindFlushInterval = 20 * time.Millisecond
	m, err := manager.New(manager.Params{Config: cfg, Client: em})
	if err != nil {
		t.Fatal(err)
	}

	var writes atomic.Int64
	_ = m.RegisterCache(manager.CacheSpec{
		Name:     "events",
		Strategy: policies.WriteBehind,
		TTL:      time.Minute,
		Writer: func(ctx context.Context, key, value string) error {
			writes.Add(1)
			return nil
		},
	})
	for i := 0; i < 10; i++ {
		_ = m.Set(ctx, "events", fmt.Sprintf("e%d", i), "x")
	}
	// Flush and confirm all deferred writes ran.
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if writes.Load() != 10 {
		t.Fatalf("write-behind flushed %d writes, want 10", writes.Load())
	}
}

func TestBulkOperations(t *testing.T) {
	ctx := context.Background()
	m, _, _ := newManager(t)
	_ = m.RegisterCache(manager.CacheSpec{Name: "b", TTL: time.Minute})

	_ = m.SetMany(ctx, "b", map[string]any{"k1": "v1", "k2": "v2", "k3": "v3"})
	items, err := m.GetMany(ctx, "b", []string{"k1", "k2", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if !items["k1"].Found || items["k1"].Value != `v1` {
		t.Fatalf("k1 = %+v", items["k1"])
	}
	if items["missing"].Found {
		t.Fatal("missing should not be found")
	}
}

func TestConcurrentCacheAccess(t *testing.T) {
	ctx := context.Background()
	m, _, _ := newManager(t)
	_ = m.RegisterCache(manager.CacheSpec{Name: "c", TTL: time.Minute})

	const workers = 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("k%d", i%20)
				_ = m.Set(ctx, "c", key, fmt.Sprintf("v-%d", w))
				var s string
				_, _ = m.Get(ctx, "c", key, &s)
			}
		}(w)
	}
	wg.Wait()

	stats, _ := m.Stats("c")
	if stats.Sets != workers*100 {
		t.Fatalf("sets = %d, want %d", stats.Sets, workers*100)
	}
}
