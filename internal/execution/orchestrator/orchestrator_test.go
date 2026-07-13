package orchestrator

import (
	stdctx "context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cpip/internal/execution/config"
	execctx "cpip/internal/execution/context"
	"cpip/internal/execution/events"
	"cpip/internal/execution/job"
	"cpip/internal/execution/language"
	"cpip/internal/execution/registry"
	"cpip/internal/execution/scheduler"
	"cpip/internal/execution/storage"
	"cpip/internal/execution/validation"
)

type harness struct {
	orch  *Orchestrator
	sched *scheduler.Memory
	bus   *events.Bus
	reg   *registry.Registry
}

func newHarness(t *testing.T, mutate func(*config.Config)) *harness {
	t.Helper()
	cfg := config.Default()
	if mutate != nil {
		mutate(&cfg)
	}
	cfg, err := cfg.Validate()
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	langs := language.Default()
	reg := registry.New()
	sched := scheduler.NewMemory(0)
	bus := events.New(events.Options{})
	pipeline := validation.NewPipeline(validation.DefaultValidators(cfg, langs, validation.AllowAll))

	orch := New(Deps{
		Config: cfg, Registry: reg, Language: langs, Pipeline: pipeline,
		Context: execctx.NewManager(nil), Scheduler: sched, Bus: bus,
		Store: storage.NewMemoryRepository(),
	})
	return &harness{orch: orch, sched: sched, bus: bus, reg: reg}
}

func goodReq() job.Request {
	return job.Request{
		UserID: "u1", RoomID: "r1", SessionID: "s1",
		Language: "go", Source: "package main", Priority: job.PriorityNormal,
		Authenticated: true,
	}
}

func drainTypes(ch chan events.Event) map[events.Type]int {
	counts := map[events.Type]int{}
	for {
		select {
		case e := <-ch:
			counts[e.Type]++
		default:
			return counts
		}
	}
}

func TestSubmitHappyPath(t *testing.T) {
	h := newHarness(t, nil)
	ch := h.bus.Subscribe(64)
	defer h.bus.Unsubscribe(ch)

	j, err := h.orch.SubmitExecution(stdctx.Background(), goodReq())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if j.State != job.StateQueued {
		t.Fatalf("state = %s, want queued", j.State)
	}
	if !h.sched.Has(j.ID) {
		t.Fatal("job not handed to scheduler")
	}
	if j.RequestID == "" || j.CorrelationID == "" {
		t.Fatal("correlation identifiers not assigned")
	}
	if j.Resources.MemoryBytes == 0 {
		t.Fatal("language resource profile not applied")
	}

	counts := drainTypes(ch)
	for _, want := range []events.Type{events.ExecutionRequested, events.ExecutionValidated, events.JobCreated, events.JobQueued} {
		if counts[want] == 0 {
			t.Errorf("missing event %s", want)
		}
	}
}

func TestSubmitValidationRejected(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.RequireAuthentication = true })
	ch := h.bus.Subscribe(16)
	defer h.bus.Unsubscribe(ch)

	req := goodReq()
	req.Authenticated = false
	_, err := h.orch.SubmitExecution(stdctx.Background(), req)
	if !errors.Is(err, job.ErrValidationFailed) || !errors.Is(err, job.ErrUnauthenticated) {
		t.Fatalf("err = %v, want validation+unauthenticated", err)
	}
	if h.reg.Count() != 0 {
		t.Fatal("no job should be created on rejection")
	}
	if drainTypes(ch)[events.ExecutionRejected] == 0 {
		t.Fatal("missing ExecutionRejected event")
	}
}

func TestUnsupportedLanguageRejected(t *testing.T) {
	h := newHarness(t, nil)
	req := goodReq()
	req.Language = "cobol"
	_, err := h.orch.SubmitExecution(stdctx.Background(), req)
	if !errors.Is(err, job.ErrUnsupportedLanguage) {
		t.Fatalf("err = %v, want ErrUnsupportedLanguage", err)
	}
}

func TestFullLifecycle(t *testing.T) {
	h := newHarness(t, nil)
	ctx := stdctx.Background()
	j, _ := h.orch.SubmitExecution(ctx, goodReq())

	steps := []struct {
		do   func() error
		want job.State
	}{
		{func() error { return h.orch.MarkDispatched(ctx, j.ID, "worker-1") }, job.StateDispatched},
		{func() error { return h.orch.MarkStarted(ctx, j.ID) }, job.StateRunning},
		{func() error { return h.orch.MarkStreaming(ctx, j.ID) }, job.StateStreaming},
		{func() error { return h.orch.MarkCompleted(ctx, j.ID) }, job.StateCompleted},
	}
	for i, s := range steps {
		if err := s.do(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		got, _ := h.orch.Status(j.ID)
		if got.State != s.want {
			t.Fatalf("step %d: state = %s, want %s", i, got.State, s.want)
		}
	}
	final, _ := h.orch.Status(j.ID)
	if final.Outcome != job.OutcomeSuccess {
		t.Fatalf("outcome = %s, want success", final.Outcome)
	}
	if final.WorkerID != "worker-1" {
		t.Fatalf("worker assignment lost: %q", final.WorkerID)
	}
	// Execution context released on completion.
	if _, ok := h.orch.ExecutionContext(j.ID); ok {
		t.Fatal("execution context not released after completion")
	}
}

func TestIllegalTransitionRejected(t *testing.T) {
	h := newHarness(t, nil)
	ctx := stdctx.Background()
	j, _ := h.orch.SubmitExecution(ctx, goodReq())
	// Queued → Running is illegal (must dispatch first).
	if err := h.orch.MarkStarted(ctx, j.ID); !errors.Is(err, job.ErrIllegalTransition) {
		t.Fatalf("err = %v, want ErrIllegalTransition", err)
	}
}

func TestCancel(t *testing.T) {
	h := newHarness(t, nil)
	ctx := stdctx.Background()
	j, _ := h.orch.SubmitExecution(ctx, goodReq())

	ec, _ := h.orch.ExecutionContext(j.ID)
	if err := h.orch.Cancel(ctx, j.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	got, _ := h.orch.Status(j.ID)
	if got.State != job.StateCancelled || got.Outcome != job.OutcomeCancelled {
		t.Fatalf("state=%s outcome=%s", got.State, got.Outcome)
	}
	if !got.CancelRequested {
		t.Fatal("CancelRequested not set")
	}
	if h.sched.Has(j.ID) {
		t.Fatal("cancelled job still scheduled")
	}
	// The execution context must have been cancelled.
	select {
	case <-ec.Context.Done():
	default:
		t.Fatal("execution context not cancelled")
	}
}

func TestCancelConflictAfterCompletion(t *testing.T) {
	h := newHarness(t, nil)
	ctx := stdctx.Background()
	j, _ := h.orch.SubmitExecution(ctx, goodReq())
	_ = h.orch.MarkDispatched(ctx, j.ID, "w")
	_ = h.orch.MarkStarted(ctx, j.ID)
	_ = h.orch.MarkCompleted(ctx, j.ID)

	if err := h.orch.Cancel(ctx, j.ID); !errors.Is(err, job.ErrCancellationConflict) {
		t.Fatalf("err = %v, want ErrCancellationConflict", err)
	}
}

func TestCancelUnknownJob(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.orch.Cancel(stdctx.Background(), "ghost"); !errors.Is(err, job.ErrJobNotFound) {
		t.Fatalf("err = %v, want ErrJobNotFound", err)
	}
}

func TestRetry(t *testing.T) {
	h := newHarness(t, nil)
	ctx := stdctx.Background()
	j, _ := h.orch.SubmitExecution(ctx, goodReq())
	_ = h.orch.MarkDispatched(ctx, j.ID, "w")
	_ = h.orch.MarkStarted(ctx, j.ID)
	_ = h.orch.MarkFailed(ctx, j.ID, "boom")

	if err := h.orch.Retry(ctx, j.ID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	got, _ := h.orch.Status(j.ID)
	if got.State != job.StateQueued || got.RetryCount != 1 {
		t.Fatalf("state=%s retry=%d, want queued/1", got.State, got.RetryCount)
	}
	if got.Outcome != job.OutcomeNone {
		t.Fatalf("retry should reset outcome, got %s", got.Outcome)
	}
}

func TestRetryConflictWhenNotFailed(t *testing.T) {
	h := newHarness(t, nil)
	ctx := stdctx.Background()
	j, _ := h.orch.SubmitExecution(ctx, goodReq())
	if err := h.orch.Retry(ctx, j.ID); !errors.Is(err, job.ErrRetryConflict) {
		t.Fatalf("err = %v, want ErrRetryConflict", err)
	}
}

func TestRetriesExhausted(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.MaxRetries = 1 })
	ctx := stdctx.Background()
	j, _ := h.orch.SubmitExecution(ctx, goodReq())

	fail := func() {
		_ = h.orch.MarkDispatched(ctx, j.ID, "w")
		_ = h.orch.MarkStarted(ctx, j.ID)
		_ = h.orch.MarkFailed(ctx, j.ID, "boom")
	}
	fail()
	if err := h.orch.Retry(ctx, j.ID); err != nil {
		t.Fatalf("first retry: %v", err)
	}
	fail()
	if err := h.orch.Retry(ctx, j.ID); !errors.Is(err, job.ErrRetriesExhausted) {
		t.Fatalf("err = %v, want ErrRetriesExhausted", err)
	}
}

type failingScheduler struct{}

func (failingScheduler) Schedule(stdctx.Context, job.Job) error                  { return job.ErrSchedulerUnavailable }
func (failingScheduler) Cancel(stdctx.Context, string) error                     { return nil }
func (failingScheduler) Retry(stdctx.Context, job.Job) error                     { return job.ErrSchedulerUnavailable }
func (failingScheduler) Reprioritize(stdctx.Context, string, job.Priority) error { return nil }

func TestSchedulerUnavailable(t *testing.T) {
	cfg, _ := config.Default().Validate()
	langs := language.Default()
	reg := registry.New()
	orch := New(Deps{
		Config: cfg, Registry: reg, Language: langs,
		Pipeline: validation.NewPipeline(validation.DefaultValidators(cfg, langs, validation.AllowAll)),
		Context:  execctx.NewManager(nil), Scheduler: failingScheduler{},
		Bus: events.New(events.Options{}), Store: storage.NewMemoryRepository(),
	})

	j, err := orch.SubmitExecution(stdctx.Background(), goodReq())
	if !errors.Is(err, job.ErrSchedulerUnavailable) {
		t.Fatalf("err = %v, want ErrSchedulerUnavailable", err)
	}
	_ = j
	// The job should have been rolled to Failed.
	if got := len(reg.ByState(job.StateFailed)); got != 1 {
		t.Fatalf("failed jobs = %d, want 1", got)
	}
}

func TestConcurrentSubmissionsAndCancellations(t *testing.T) {
	h := newHarness(t, nil)
	ctx := stdctx.Background()
	const n = 300

	var wg sync.WaitGroup
	ids := make([]string, n)
	var mu sync.Mutex

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := goodReq()
			req.UserID = fmt.Sprintf("u%d", i%20)
			req.RoomID = fmt.Sprintf("r%d", i%10)
			j, err := h.orch.SubmitExecution(ctx, req)
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
				return
			}
			mu.Lock()
			ids[i] = j.ID
			mu.Unlock()
			if i%3 == 0 {
				_ = h.orch.Cancel(ctx, j.ID)
			} else {
				_ = h.orch.MarkDispatched(ctx, j.ID, "w")
			}
		}(i)
	}
	wg.Wait()

	if h.reg.Count() != n {
		t.Fatalf("registry count = %d, want %d", h.reg.Count(), n)
	}
	stats := h.orch.Stats()
	if stats.Total != n {
		t.Fatalf("stats total = %d, want %d", stats.Total, n)
	}
	// Every job is either cancelled or dispatched.
	cancelled := len(h.reg.ByState(job.StateCancelled))
	dispatched := len(h.reg.ByState(job.StateDispatched))
	if cancelled+dispatched != n {
		t.Fatalf("cancelled(%d)+dispatched(%d) != %d", cancelled, dispatched, n)
	}
}

func TestArchiveFinished(t *testing.T) {
	base := time.Now()
	clock := base
	h := newHarness(t, func(c *config.Config) { c.ArchiveRetention = time.Minute })
	h.orch.now = func() time.Time { return clock }

	ctx := stdctx.Background()
	j, _ := h.orch.SubmitExecution(ctx, goodReq())
	_ = h.orch.MarkDispatched(ctx, j.ID, "w")
	_ = h.orch.MarkStarted(ctx, j.ID)
	_ = h.orch.MarkCompleted(ctx, j.ID)

	sink := storage.NewMemoryArchive()
	// Not yet past retention.
	if n := h.orch.ArchiveFinished(ctx, sink); n != 0 {
		t.Fatalf("premature archival: %d", n)
	}
	// Advance past retention.
	clock = base.Add(2 * time.Minute)
	if n := h.orch.ArchiveFinished(ctx, sink); n != 1 {
		t.Fatalf("archived = %d, want 1", n)
	}
	if _, err := h.orch.Status(j.ID); !errors.Is(err, job.ErrJobNotFound) {
		t.Fatal("archived job should be removed from live registry")
	}
	if c, _ := sink.Count(ctx); c != 1 {
		t.Fatalf("archive sink count = %d, want 1", c)
	}
}
