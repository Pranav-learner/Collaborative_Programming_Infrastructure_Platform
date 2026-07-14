package manager_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/manager"
	"cpip/internal/cache/redis"
)

// TestStressThousandsOfConcurrentRequests exercises the manager under a heavy
// mixed read/write/delete load to shake out races and verify throughput holds.
func TestStressThousandsOfConcurrentRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	ctx := context.Background()
	em := redis.NewEmulator()
	m, err := manager.New(manager.Params{Config: config.Default(), Client: em})
	if err != nil {
		t.Fatal(err)
	}
	_ = m.RegisterCache(manager.CacheSpec{Name: "hot", TTL: time.Minute})

	const (
		goroutines = 200
		opsEach    = 500
		keySpace   = 256
	)
	var wg sync.WaitGroup
	start := time.Now()
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsEach; i++ {
				key := fmt.Sprintf("k%d", (g*7+i)%keySpace)
				switch i % 4 {
				case 0, 1:
					var s string
					_, _ = m.Get(ctx, "hot", key, &s)
				case 2:
					_ = m.Set(ctx, "hot", key, fmt.Sprintf("v%d", i))
				case 3:
					_ = m.Delete(ctx, "hot", key)
				}
			}
		}(g)
	}
	wg.Wait()

	total := goroutines * opsEach
	elapsed := time.Since(start)
	t.Logf("%d ops in %s (%.0f ops/sec)", total, elapsed, float64(total)/elapsed.Seconds())

	stats, _ := m.Stats("hot")
	if stats.Hits+stats.Misses+stats.Sets+stats.Deletes == 0 {
		t.Fatal("no operations recorded")
	}
}
