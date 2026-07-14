package sessions_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/sessions"
	"cpip/internal/cache/types"
)

func itoa(n int) string { return strconv.Itoa(n) }

func newStore(t *testing.T) (*sessions.Store, *redis.Emulator) {
	t.Helper()
	em := redis.NewEmulator()
	s := sessions.New(sessions.Params{
		Client: em,
		Keys:   keys.New("cpip"),
		Config: config.Default().TTL,
	})
	return s, em
}

func TestSessionCreateGet(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)

	sess, err := s.Create(ctx, sessions.CreateParams{UserID: "u1", DeviceID: "laptop"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.UserID != "u1" || got.DeviceID != "laptop" {
		t.Fatalf("got %+v", got)
	}
	if _, err := s.Get(ctx, "sess_missing"); !errors.Is(err, types.ErrSessionNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestSessionRenew(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	sess, _ := s.Create(ctx, sessions.CreateParams{UserID: "u1", TTL: time.Hour})
	orig := sess.ExpiresAt

	time.Sleep(5 * time.Millisecond)
	renewed, err := s.Renew(ctx, sess.ID, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !renewed.ExpiresAt.After(orig) {
		t.Fatalf("expiry not extended: %v vs %v", renewed.ExpiresAt, orig)
	}
	if renewed.Version <= sess.Version {
		t.Fatalf("version not incremented")
	}
}

func TestSessionMultiDevice(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)

	for _, dev := range []string{"laptop", "phone", "tablet"} {
		if _, err := s.Create(ctx, sessions.CreateParams{UserID: "u1", DeviceID: dev}); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.ListByUser(ctx, "u1")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 sessions, got %d", len(list))
	}

	n, err := s.InvalidateUser(ctx, "u1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("invalidated %d, want 3", n)
	}
	list, _ = s.ListByUser(ctx, "u1")
	if len(list) != 0 {
		t.Fatalf("expected 0 sessions after sign-out-everywhere, got %d", len(list))
	}
}

func TestSessionExpiry(t *testing.T) {
	ctx := context.Background()
	s, em := newStore(t)
	now := time.Now()
	em.SetClock(func() time.Time { return now })
	s.SetClock(func() time.Time { return now })

	sess, _ := s.Create(ctx, sessions.CreateParams{UserID: "u1", TTL: time.Second})
	now = now.Add(2 * time.Second)
	if _, err := s.Get(ctx, sess.ID); !errors.Is(err, types.ErrSessionExpired) && !errors.Is(err, types.ErrSessionNotFound) {
		t.Fatalf("expected expired/not-found, got %v", err)
	}
}

// TestConcurrentSessionUpdates verifies optimistic concurrency: N goroutines
// each increment a counter in session data; with CAS retry, no update is lost.
func TestConcurrentSessionUpdates(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	sess, _ := s.Create(ctx, sessions.CreateParams{UserID: "u1", TTL: time.Hour, Data: map[string]string{"count": "0"}})

	const workers = 20
	var wg sync.WaitGroup
	var conflicts atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, err := s.Update(ctx, sess.ID, func(cur *sessions.Session) error {
					n := 0
					if cur.Data != nil {
						fmt.Sscanf(cur.Data["count"], "%d", &n)
					} else {
						cur.Data = map[string]string{}
					}
					cur.Data["count"] = itoa(n + 1)
					return nil
				})
				if err == nil {
					return
				}
				if errors.Is(err, types.ErrSessionConflict) {
					conflicts.Add(1)
					continue // retry
				}
				t.Errorf("update: %v", err)
				return
			}
		}()
	}
	wg.Wait()

	final, _ := s.Get(ctx, sess.ID)
	if final.Data["count"] != itoa(workers) {
		t.Fatalf("count = %q, want %d (lost updates!)", final.Data["count"], workers)
	}
}
