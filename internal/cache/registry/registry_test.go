package registry_test

import (
	"sync"
	"testing"

	"cpip/internal/cache/policies"
	"cpip/internal/cache/registry"
	"cpip/internal/cache/types"
)

func TestRegistryStatsAndHitRatio(t *testing.T) {
	r := registry.New()
	r.Register(registry.Descriptor{Name: "c", Strategy: policies.CacheAside})

	for i := 0; i < 7; i++ {
		r.RecordHit("c")
	}
	for i := 0; i < 3; i++ {
		r.RecordMiss("c")
	}
	r.RecordSet("c")

	s, ok := r.Stats("c")
	if !ok {
		t.Fatal("stats missing")
	}
	if s.Hits != 7 || s.Misses != 3 {
		t.Fatalf("stats = %+v", s)
	}
	if s.HitRatio < 0.69 || s.HitRatio > 0.71 {
		t.Fatalf("hit ratio = %v, want 0.7", s.HitRatio)
	}
}

func TestRegistryHealthAndUnknown(t *testing.T) {
	r := registry.New()
	r.Register(registry.Descriptor{Name: "c"})
	if r.Health("c") != types.HealthUp {
		t.Fatal("new cache should be up")
	}
	r.SetHealth("c", types.HealthDegraded)
	if r.Health("c") != types.HealthDegraded {
		t.Fatal("health not updated")
	}
	if r.Health("ghost") != types.HealthDown {
		t.Fatal("unknown cache should be down")
	}
	// Recording on unknown cache must not panic.
	r.RecordHit("ghost")
}

func TestRegistryConcurrentCounters(t *testing.T) {
	r := registry.New()
	r.Register(registry.Descriptor{Name: "c"})

	const workers = 50
	const each = 1000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				r.RecordHit("c")
			}
		}()
	}
	wg.Wait()
	s, _ := r.Stats("c")
	if s.Hits != workers*each {
		t.Fatalf("hits = %d, want %d (lost increments)", s.Hits, workers*each)
	}
}
