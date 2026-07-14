package locks_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/locks"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/types"
)

func newLockMgr(t *testing.T) (*locks.Manager, *redis.Emulator) {
	t.Helper()
	em := redis.NewEmulator()
	m := locks.New(locks.Params{
		Client: em,
		Config: config.Default().Lock,
		Keys:   keys.New("cpip"),
		NodeID: "node-a",
	})
	return m, em
}

func TestLockAcquireRelease(t *testing.T) {
	ctx := context.Background()
	m, _ := newLockMgr(t)

	l, err := m.Acquire(ctx, "res1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.Token() == "" {
		t.Fatal("expected non-empty token")
	}
	// Second acquire should time out quickly.
	_, err = m.TryAcquire(ctx, "res1", nil)
	if !errors.Is(err, types.ErrLockNotAcquired) {
		t.Fatalf("expected not acquired, got %v", err)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// Now acquirable again.
	l2, err := m.TryAcquire(ctx, "res1", nil)
	if err != nil {
		t.Fatalf("re-acquire failed: %v", err)
	}
	_ = l2.Release(ctx)
}

func TestLockReleaseNotOwned(t *testing.T) {
	ctx := context.Background()
	m, em := newLockMgr(t)
	l, _ := m.Acquire(ctx, "res", &locks.Options{Lease: time.Minute})
	// Simulate the lease being taken over by someone else.
	_ = em.Set(ctx, keys.New("cpip").Lock("res"), "someone-else", time.Minute)
	if err := l.Release(ctx); !errors.Is(err, types.ErrLockNotHeld) {
		t.Fatalf("expected not held, got %v", err)
	}
}

func TestLockRenew(t *testing.T) {
	ctx := context.Background()
	m, _ := newLockMgr(t)
	l, _ := m.Acquire(ctx, "res", &locks.Options{Lease: 200 * time.Millisecond})
	if err := l.Renew(ctx, time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}
	_ = l.Release(ctx)
}

func TestLockMutualExclusionUnderContention(t *testing.T) {
	ctx := context.Background()
	m, _ := newLockMgr(t)

	var (
		inside  atomic.Int32
		maxSeen atomic.Int32
		counter int
	)
	const workers = 30
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := m.WithLock(ctx, "critical", &locks.Options{
				Lease:          2 * time.Second,
				AcquireTimeout: 5 * time.Second,
			}, func(ctx context.Context) error {
				n := inside.Add(1)
				if n > maxSeen.Load() {
					maxSeen.Store(n)
				}
				// Non-atomic mutation guarded solely by the distributed lock.
				counter++
				time.Sleep(time.Millisecond)
				inside.Add(-1)
				return nil
			})
			if err != nil {
				t.Errorf("WithLock: %v", err)
			}
		}()
	}
	wg.Wait()

	if maxSeen.Load() != 1 {
		t.Fatalf("mutual exclusion violated: %d goroutines were inside at once", maxSeen.Load())
	}
	if counter != workers {
		t.Fatalf("counter = %d, want %d", counter, workers)
	}
}

func TestLockAutoRenewWatchdog(t *testing.T) {
	ctx := context.Background()
	em := redis.NewEmulator()
	cfg := config.Default().Lock
	cfg.AutoRenewFraction = 0.3
	m := locks.New(locks.Params{Client: em, Config: cfg, Keys: keys.New("cpip"), NodeID: "n"})

	// Short lease with auto-renew: the lock must survive well past one lease.
	l, err := m.Acquire(ctx, "long", &locks.Options{Lease: 100 * time.Millisecond, AutoRenew: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release(ctx)

	time.Sleep(350 * time.Millisecond) // > 3 lease periods
	if l.IsLost() {
		t.Fatal("watchdog failed: lock was lost")
	}
	n, _ := em.Exists(ctx, keys.New("cpip").Lock("long"))
	if n != 1 {
		t.Fatal("lock key should still exist thanks to watchdog")
	}
}
