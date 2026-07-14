package locks_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/keys"
	"cpip/internal/coordination/locks"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/types"
)

func mkLockMgr(be backend.Backend, node string) *locks.Manager {
	return locks.New(locks.Params{
		Backend: be, Config: config.Default().Lock, Keys: keys.New("t", "c"),
		NodeID: node, Events: events.NewBus(), Logger: logger.New(nil),
	})
}

func TestLockMutualExclusion(t *testing.T) {
	ctx := context.Background()
	be := backend.NewMemory()
	m := mkLockMgr(be, "n1")

	l, err := m.TryAcquire(ctx, "res", nil)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// A second acquisition must fail while held.
	if _, err := m.TryAcquire(ctx, "res", nil); !errors.Is(err, types.ErrLockNotAcquired) {
		t.Fatalf("expected contention, got %v", err)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	// Now it can be re-acquired.
	l2, err := m.TryAcquire(ctx, "res", nil)
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	_ = l2.Release(ctx)
}

func TestLockFencingReleaseNotHeld(t *testing.T) {
	ctx := context.Background()
	be := backend.NewMemory()
	m := mkLockMgr(be, "n1")
	l, _ := m.TryAcquire(ctx, "res", nil)

	// Simulate lease expiry + another owner taking the key.
	_, _ = be.Delete(ctx, keys.New("t", "c").LockKey("res"))
	_, _ = be.SetNX(ctx, keys.New("t", "c").LockKey("res"), "other", time.Minute)

	// Release must report not-held (fencing prevents deleting another's lock).
	if err := l.Release(ctx); !errors.Is(err, types.ErrLockNotHeld) {
		t.Fatalf("expected not-held on release after takeover, got %v", err)
	}
}

func TestLockRenewExtendsLease(t *testing.T) {
	ctx := context.Background()
	be := backend.NewMemory()
	m := mkLockMgr(be, "n1")
	l, _ := m.TryAcquire(ctx, "res", &locks.Options{Lease: 200 * time.Millisecond})
	if err := l.Renew(ctx, 500*time.Millisecond); err != nil {
		t.Fatalf("renew: %v", err)
	}
	_ = l.Release(ctx)
}

func TestLockConcurrentSingleHolder(t *testing.T) {
	ctx := context.Background()
	be := backend.NewMemory()
	m := mkLockMgr(be, "n1")

	const n = 200
	var concurrent atomic.Int32
	var maxSeen atomic.Int32
	var acquired atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := m.TryAcquire(ctx, "hot", &locks.Options{Lease: time.Second})
			if err != nil {
				return
			}
			acquired.Add(1)
			cur := concurrent.Add(1)
			if cur > maxSeen.Load() {
				maxSeen.Store(cur)
			}
			// Hold briefly inside the critical section.
			time.Sleep(time.Millisecond)
			concurrent.Add(-1)
			_ = l.Release(ctx)
		}()
	}
	wg.Wait()
	if maxSeen.Load() > 1 {
		t.Fatalf("critical section entered by %d holders concurrently", maxSeen.Load())
	}
	if acquired.Load() == 0 {
		t.Fatalf("no goroutine ever acquired the lock")
	}
}

func TestLockAcquireBlocksUntilReleased(t *testing.T) {
	ctx := context.Background()
	be := backend.NewMemory()
	m := mkLockMgr(be, "n1")

	l, _ := m.TryAcquire(ctx, "res", &locks.Options{Lease: time.Second})
	done := make(chan struct{})
	go func() {
		l2, err := m.Acquire(ctx, "res", &locks.Options{AcquireTimeout: 2 * time.Second, Lease: time.Second})
		if err == nil {
			_ = l2.Release(ctx)
		}
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	_ = l.Release(ctx)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("blocked Acquire did not proceed after release")
	}
}
