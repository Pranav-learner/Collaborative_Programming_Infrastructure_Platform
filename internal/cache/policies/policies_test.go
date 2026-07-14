package policies_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/policies"
	"cpip/internal/cache/types"
)

// memStore is a minimal in-memory policies.Store for isolated engine tests.
type memStore struct {
	mu   sync.Mutex
	data map[string]entry
}
type entry struct {
	value    string
	expireAt time.Time
}

func newMemStore() *memStore { return &memStore{data: make(map[string]entry)} }

func (s *memStore) RawGet(_ context.Context, k string) (string, time.Duration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[k]
	if !ok {
		return "", 0, false, nil
	}
	rem := time.Until(e.expireAt)
	if !e.expireAt.IsZero() && rem <= 0 {
		delete(s.data, k)
		return "", 0, false, nil
	}
	return e.value, rem, true, nil
}

func (s *memStore) RawSet(_ context.Context, k, v string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp := time.Time{}
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	s.data[k] = entry{value: v, expireAt: exp}
	return nil
}

func (s *memStore) RawDelete(_ context.Context, k string) error {
	s.mu.Lock()
	delete(s.data, k)
	s.mu.Unlock()
	return nil
}

func TestRegistrationValidatesCollaborators(t *testing.T) {
	e := policies.NewEngine(newMemStore(), config.Default().Policy, nil, nil)
	if err := e.Register("c", policies.Registration{Strategy: policies.ReadThrough}); !errors.Is(err, types.ErrNoLoader) {
		t.Fatalf("expected ErrNoLoader, got %v", err)
	}
	if err := e.Register("c", policies.Registration{Strategy: policies.WriteThrough}); !errors.Is(err, types.ErrNoWriter) {
		t.Fatalf("expected ErrNoWriter, got %v", err)
	}
	if err := e.Register("c", policies.Registration{Strategy: "bogus"}); !errors.Is(err, types.ErrUnknownPolicy) {
		t.Fatalf("expected ErrUnknownPolicy, got %v", err)
	}
}

func TestRefreshAheadTriggersBackgroundReload(t *testing.T) {
	store := newMemStore()
	cfg := config.Default().Policy
	e := policies.NewEngine(store, cfg, nil, nil)

	var loads atomic.Int64
	const fullTTL = 100 * time.Millisecond
	err := e.Register("hot", policies.Registration{
		Strategy:          policies.RefreshAhead,
		FullTTL:           fullTTL,
		RefreshAheadRatio: 0.5, // refresh once 50% of TTL elapsed
		Loader: func(ctx context.Context, key string) (string, time.Duration, bool, error) {
			loads.Add(1)
			return "reloaded", fullTTL, true, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Seed a value with only 30ms remaining (past the 50% threshold).
	_ = store.RawSet(context.Background(), "k", "stale", 30*time.Millisecond)

	// A Get should serve the (stale) value AND trigger a background refresh.
	v, found, err := e.Get(context.Background(), "hot", "k", fullTTL)
	if err != nil || !found || v != "stale" {
		t.Fatalf("get = %q found=%v err=%v", v, found, err)
	}
	// Wait for the async refresh to complete.
	deadline := time.After(time.Second)
	for loads.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("refresh-ahead did not trigger a background reload")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if loads.Load() != 1 {
		t.Fatalf("expected exactly one reload, got %d", loads.Load())
	}
}
