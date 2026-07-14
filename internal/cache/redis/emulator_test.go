package redis_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cpip/internal/cache/redis"
	"cpip/internal/cache/types"
)

func newEm() *redis.Emulator { return redis.NewEmulator() }

func TestEmulatorStringLifecycle(t *testing.T) {
	ctx := context.Background()
	e := newEm()

	if _, err := e.Get(ctx, "missing"); !errors.Is(err, types.ErrNil) {
		t.Fatalf("expected ErrNil, got %v", err)
	}
	if err := e.Set(ctx, "k", "v", 0); err != nil {
		t.Fatal(err)
	}
	got, err := e.Get(ctx, "k")
	if err != nil || got != "v" {
		t.Fatalf("get = %q, %v", got, err)
	}
	n, _ := e.Exists(ctx, "k", "missing")
	if n != 1 {
		t.Fatalf("exists = %d, want 1", n)
	}
	del, _ := e.Del(ctx, "k")
	if del != 1 {
		t.Fatalf("del = %d", del)
	}
	if _, err := e.Get(ctx, "k"); !errors.Is(err, types.ErrNil) {
		t.Fatalf("expected miss after delete")
	}
}

func TestEmulatorTTLExpiry(t *testing.T) {
	ctx := context.Background()
	e := newEm()
	now := time.Now()
	e.SetClock(func() time.Time { return now })

	_ = e.Set(ctx, "k", "v", 100*time.Millisecond)
	ttl, _ := e.TTL(ctx, "k")
	if ttl <= 0 {
		t.Fatalf("ttl = %v", ttl)
	}
	now = now.Add(200 * time.Millisecond)
	if _, err := e.Get(ctx, "k"); !errors.Is(err, types.ErrNil) {
		t.Fatalf("expected expiry")
	}
	ttl, _ = e.TTL(ctx, "missing")
	if ttl != -2*time.Second {
		t.Fatalf("missing ttl = %v", ttl)
	}
}

func TestEmulatorSetNXAndCAS(t *testing.T) {
	ctx := context.Background()
	e := newEm()

	ok, _ := e.SetNX(ctx, "lock", "owner1", time.Minute)
	if !ok {
		t.Fatal("first SetNX should succeed")
	}
	ok, _ = e.SetNX(ctx, "lock", "owner2", time.Minute)
	if ok {
		t.Fatal("second SetNX should fail")
	}
	// Wrong owner cannot delete.
	ok, _ = e.CompareAndDelete(ctx, "lock", "owner2")
	if ok {
		t.Fatal("CompareAndDelete with wrong token should fail")
	}
	// Right owner can extend.
	ok, _ = e.CompareAndExtend(ctx, "lock", "owner1", 2*time.Minute)
	if !ok {
		t.Fatal("CompareAndExtend with right token should succeed")
	}
	// Right owner can delete.
	ok, _ = e.CompareAndDelete(ctx, "lock", "owner1")
	if !ok {
		t.Fatal("CompareAndDelete with right token should succeed")
	}
}

func TestEmulatorCompareAndSet(t *testing.T) {
	ctx := context.Background()
	e := newEm()

	// expected="" requires absence.
	ok, _ := e.CompareAndSet(ctx, "s", "", "v1", 0)
	if !ok {
		t.Fatal("CAS on absent key with empty expected should succeed")
	}
	ok, _ = e.CompareAndSet(ctx, "s", "", "v2", 0)
	if ok {
		t.Fatal("CAS expecting absence on present key should fail")
	}
	ok, _ = e.CompareAndSet(ctx, "s", "v1", "v2", 0)
	if !ok {
		t.Fatal("CAS with matching expected should succeed")
	}
	got, _ := e.Get(ctx, "s")
	if got != "v2" {
		t.Fatalf("value = %q", got)
	}
}

func TestEmulatorHashAndSet(t *testing.T) {
	ctx := context.Background()
	e := newEm()

	_ = e.HSet(ctx, "h", map[string]string{"a": "1", "b": "2"})
	v, _ := e.HGet(ctx, "h", "a")
	if v != "1" {
		t.Fatalf("hget = %q", v)
	}
	all, _ := e.HGetAll(ctx, "h")
	if len(all) != 2 {
		t.Fatalf("hgetall = %v", all)
	}
	e.HDel(ctx, "h", "a")
	if _, err := e.HGet(ctx, "h", "a"); !errors.Is(err, types.ErrNil) {
		t.Fatal("expected miss after hdel")
	}

	e.SAdd(ctx, "set", "x", "y", "z")
	mem, _ := e.SMembers(ctx, "set")
	if len(mem) != 3 {
		t.Fatalf("smembers = %v", mem)
	}
	is, _ := e.SIsMember(ctx, "set", "y")
	if !is {
		t.Fatal("y should be a member")
	}
	e.SRem(ctx, "set", "y")
	is, _ = e.SIsMember(ctx, "set", "y")
	if is {
		t.Fatal("y should be removed")
	}
}

func TestEmulatorScanKeysGlob(t *testing.T) {
	ctx := context.Background()
	e := newEm()
	_ = e.Set(ctx, "cpip:cache:room:1", "a", 0)
	_ = e.Set(ctx, "cpip:cache:room:2", "b", 0)
	_ = e.Set(ctx, "cpip:cache:doc:1", "c", 0)

	keys, _ := e.ScanKeys(ctx, "cpip:cache:room:*", 100)
	if len(keys) != 2 {
		t.Fatalf("scan matched %d keys: %v", len(keys), keys)
	}
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"*", "anything", true},
		{"a*c", "abc", true},
		{"a*c", "ac", true},
		{"a*c", "abd", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h?llo", "heello", false},
		{"room:[0-9]", "room:5", true},
		{"room:[0-9]", "room:x", false},
		{"room:[^0-9]", "room:x", true},
		{"a\\*c", "a*c", true},
		{"a\\*c", "abc", false},
	}
	for _, c := range cases {
		if got := redis.MatchGlob(c.pat, c.s); got != c.want {
			t.Errorf("MatchGlob(%q,%q) = %v, want %v", c.pat, c.s, got, c.want)
		}
	}
}

func TestEmulatorPubSub(t *testing.T) {
	ctx := context.Background()
	e := newEm()

	sub, err := e.Subscribe(ctx, "chan")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if _, err := e.Publish(ctx, "chan", "hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-sub.Channel():
		if msg.Payload != "hello" || msg.Channel != "chan" {
			t.Fatalf("unexpected message %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestEmulatorPatternPubSub(t *testing.T) {
	ctx := context.Background()
	e := newEm()

	sub, _ := e.PSubscribe(ctx, "room:*")
	defer sub.Close()

	e.Publish(ctx, "room:42", "hi")
	select {
	case msg := <-sub.Channel():
		if msg.Pattern != "room:*" || msg.Channel != "room:42" {
			t.Fatalf("unexpected %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestEmulatorConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	e := newEm()
	const goroutines = 64
	const ops = 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k:%d:%d", g, i%10)
				_ = e.Set(ctx, key, "v", time.Minute)
				_, _ = e.Get(ctx, key)
				_, _ = e.Incr(ctx, fmt.Sprintf("counter:%d", g))
			}
		}(g)
	}
	wg.Wait()

	for g := 0; g < goroutines; g++ {
		v, _ := e.Get(ctx, fmt.Sprintf("counter:%d", g))
		if v != fmt.Sprintf("%d", ops) {
			t.Fatalf("counter %d = %q, want %d", g, v, ops)
		}
	}
}
