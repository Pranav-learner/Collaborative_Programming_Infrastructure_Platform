package backend

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemorySetGetDelete(t *testing.T) {
	ctx := context.Background()
	b := NewMemory()
	if err := b.Set(ctx, "k", "v", 0); err != nil {
		t.Fatal(err)
	}
	v, ok, _ := b.Get(ctx, "k")
	if !ok || v != "v" {
		t.Fatalf("got (%q,%v), want (v,true)", v, ok)
	}
	n, _ := b.Delete(ctx, "k")
	if n != 1 {
		t.Fatalf("delete count = %d, want 1", n)
	}
	if _, ok, _ := b.Get(ctx, "k"); ok {
		t.Fatalf("key should be gone")
	}
}

func TestMemoryTTLExpiry(t *testing.T) {
	ctx := context.Background()
	b := NewMemory()
	now := time.Now()
	b.now = func() time.Time { return now }
	_ = b.Set(ctx, "k", "v", 50*time.Millisecond)
	if _, ok, _ := b.Get(ctx, "k"); !ok {
		t.Fatalf("key should be present before expiry")
	}
	now = now.Add(100 * time.Millisecond)
	if _, ok, _ := b.Get(ctx, "k"); ok {
		t.Fatalf("key should have expired")
	}
}

func TestMemorySetNXAndCAS(t *testing.T) {
	ctx := context.Background()
	b := NewMemory()
	ok, _ := b.SetNX(ctx, "lock", "owner1", time.Minute)
	if !ok {
		t.Fatalf("first SetNX should succeed")
	}
	ok, _ = b.SetNX(ctx, "lock", "owner2", time.Minute)
	if ok {
		t.Fatalf("second SetNX should fail")
	}
	// CompareAndDelete with wrong token fails, right token succeeds.
	if ok, _ := b.CompareAndDelete(ctx, "lock", "owner2"); ok {
		t.Fatalf("CAD with wrong token should fail")
	}
	if ok, _ := b.CompareAndDelete(ctx, "lock", "owner1"); !ok {
		t.Fatalf("CAD with right token should succeed")
	}
}

func TestMemoryCompareAndSwap(t *testing.T) {
	ctx := context.Background()
	b := NewMemory()
	// Absent key with expected "" acts as create.
	if ok, _ := b.CompareAndSwap(ctx, "k", "", "v1", 0); !ok {
		t.Fatalf("CAS create should succeed")
	}
	if ok, _ := b.CompareAndSwap(ctx, "k", "wrong", "v2", 0); ok {
		t.Fatalf("CAS with wrong expected should fail")
	}
	if ok, _ := b.CompareAndSwap(ctx, "k", "v1", "v2", 0); !ok {
		t.Fatalf("CAS with right expected should succeed")
	}
	v, _, _ := b.Get(ctx, "k")
	if v != "v2" {
		t.Fatalf("value = %q, want v2", v)
	}
}

func TestMemorySetsAndScan(t *testing.T) {
	ctx := context.Background()
	b := NewMemory()
	_, _ = b.SAdd(ctx, "s", "a", "b", "c")
	_, _ = b.SRem(ctx, "s", "b")
	mem, _ := b.SMembers(ctx, "s")
	if len(mem) != 2 {
		t.Fatalf("set size = %d, want 2", len(mem))
	}
	_ = b.Set(ctx, "p:1", "x", 0)
	_ = b.Set(ctx, "p:2", "y", 0)
	_ = b.Set(ctx, "q:1", "z", 0)
	keys, _ := b.Scan(ctx, "p:")
	if len(keys) != 2 {
		t.Fatalf("scan matched %d, want 2", len(keys))
	}
}

func TestMemoryPubSub(t *testing.T) {
	ctx := context.Background()
	b := NewMemory()
	sub, _ := b.Subscribe(ctx, "ch")
	defer sub.Close()
	if err := b.Publish(ctx, "ch", "hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-sub.Messages():
		if msg != "hello" {
			t.Fatalf("got %q, want hello", msg)
		}
	case <-time.After(time.Second):
		t.Fatalf("no message received")
	}
}

func TestMemoryConcurrentSetNXSingleWinner(t *testing.T) {
	ctx := context.Background()
	b := NewMemory()
	const n = 100
	var wins int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := b.SetNX(ctx, "race", "x", time.Minute); ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("expected exactly one SetNX winner, got %d", wins)
	}
}
