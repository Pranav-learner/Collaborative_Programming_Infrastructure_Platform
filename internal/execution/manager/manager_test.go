package manager

import (
	stdctx "context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cpip/internal/execution/config"
	"cpip/internal/execution/job"
	"cpip/internal/execution/logger"
	"cpip/internal/execution/validation"
)

func newManager(t *testing.T, mutate func(*config.Config)) *Manager {
	t.Helper()
	cfg := config.Default()
	if mutate != nil {
		mutate(&cfg)
	}
	m, err := NewManager(Params{Config: cfg, Logger: logger.Discard()})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return m
}

func goodReq() job.Request {
	return job.Request{
		UserID: "u1", RoomID: "r1", SessionID: "s1",
		Language: "python3", Source: "print(1)", Priority: job.PriorityNormal,
		Authenticated: true,
	}
}

func TestManagerServiceInterface(t *testing.T) {
	var _ Service = newManager(t, nil)
}

func TestManagerEndToEnd(t *testing.T) {
	m := newManager(t, nil)
	ctx := stdctx.Background()

	j, err := m.Submit(ctx, goodReq())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if j.State != job.StateQueued {
		t.Fatalf("state = %s", j.State)
	}

	// Full lifecycle via the public Service API.
	for _, step := range []func() error{
		func() error { return m.MarkDispatched(ctx, j.ID, "worker-1") },
		func() error { return m.MarkStarted(ctx, j.ID) },
		func() error { return m.MarkCompleted(ctx, j.ID) },
	} {
		if err := step(); err != nil {
			t.Fatalf("lifecycle step: %v", err)
		}
	}

	got, _ := m.Status(j.ID)
	if got.Outcome != job.OutcomeSuccess {
		t.Fatalf("outcome = %s", got.Outcome)
	}
	stats, _ := m.Statistics(j.ID)
	if stats.State != job.StateCompleted {
		t.Fatalf("stats state = %s", stats.State)
	}
	if len(m.ByUser("u1")) != 1 || len(m.ByRoom("r1")) != 1 || len(m.ByLanguage("python3")) != 1 {
		t.Fatal("index queries wrong")
	}
	if len(m.Languages()) == 0 {
		t.Fatal("languages empty")
	}
}

func TestManagerRejectsInvalid(t *testing.T) {
	m := newManager(t, func(c *config.Config) { c.RequireAuthentication = true })
	req := goodReq()
	req.Authenticated = false
	if _, err := m.Submit(stdctx.Background(), req); !errors.Is(err, job.ErrValidationFailed) {
		t.Fatalf("err = %v, want ErrValidationFailed", err)
	}
}

func TestManagerCancelAndRetry(t *testing.T) {
	m := newManager(t, nil)
	ctx := stdctx.Background()

	j, _ := m.Submit(ctx, goodReq())
	if err := m.Cancel(ctx, j.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got, _ := m.Status(j.ID); got.State != job.StateCancelled {
		t.Fatalf("state = %s, want cancelled", got.State)
	}

	// Fresh job to exercise retry.
	j2, _ := m.Submit(ctx, goodReq())
	_ = m.MarkDispatched(ctx, j2.ID, "w")
	_ = m.MarkStarted(ctx, j2.ID)
	_ = m.MarkTimedOut(ctx, j2.ID)
	if err := m.Retry(ctx, j2.ID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got, _ := m.Status(j2.ID); got.RetryCount != 1 || got.State != job.StateQueued {
		t.Fatalf("retry state=%s count=%d", got.State, got.RetryCount)
	}
}

func TestManagerCustomValidatorAndAuthorizer(t *testing.T) {
	cfg := config.Default()
	cfg.EnableAuthorization = true
	denied := validation.AuthorizerFunc(func(_ stdctx.Context, req *job.Request) (bool, error) {
		return req.UserID == "admin", nil
	})
	m, err := NewManager(Params{Config: cfg, Logger: logger.Discard(), Authorizer: denied})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	req := goodReq()
	req.UserID = "not-admin"
	if _, err := m.Submit(stdctx.Background(), req); !errors.Is(err, job.ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
	req.UserID = "admin"
	if _, err := m.Submit(stdctx.Background(), req); err != nil {
		t.Fatalf("admin submit: %v", err)
	}
}

func TestManagerArchivalLoop(t *testing.T) {
	base := time.Now()
	var mu sync.Mutex
	clock := base
	now := func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }

	cfg := config.Default()
	cfg.ArchiveRetention = 50 * time.Millisecond
	cfg.ArchiveSweepInterval = 10 * time.Millisecond
	m, err := NewManager(Params{Config: cfg, Logger: logger.Discard(), NowFunc: now})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	m.Start()
	defer m.Stop()

	ctx := stdctx.Background()
	j, _ := m.Submit(ctx, goodReq())
	_ = m.MarkDispatched(ctx, j.ID, "w")
	_ = m.MarkStarted(ctx, j.ID)
	_ = m.MarkCompleted(ctx, j.ID)

	// Advance the clock beyond retention so the sweep archives the finished job.
	mu.Lock()
	clock = base.Add(time.Second)
	mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := m.Status(j.ID); errors.Is(err, job.ErrJobNotFound) {
			return // archived and removed from the live registry
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job was not archived by the background sweep")
}

func TestManagerConcurrentStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	m := newManager(t, nil)
	ctx := stdctx.Background()
	const n = 400

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := goodReq()
			req.UserID = fmt.Sprintf("u%d", i%25)
			j, err := m.Submit(ctx, req)
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
				return
			}
			switch i % 4 {
			case 0:
				_ = m.Cancel(ctx, j.ID)
			case 1:
				_ = m.MarkDispatched(ctx, j.ID, "w")
				_ = m.MarkStarted(ctx, j.ID)
				_ = m.MarkFailed(ctx, j.ID, "boom")
				_ = m.Retry(ctx, j.ID)
			default:
				_ = m.MarkDispatched(ctx, j.ID, "w")
				_ = m.MarkStarted(ctx, j.ID)
				_ = m.MarkCompleted(ctx, j.ID)
			}
		}(i)
	}
	wg.Wait()

	if got := m.Stats().Total; got != n {
		t.Fatalf("total jobs = %d, want %d", got, n)
	}
}
