package manager

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cpip/internal/execution/job"
	"cpip/internal/queue/config"
	"cpip/internal/queue/events"
	"cpip/internal/queue/redisstream"
	"cpip/internal/queue/types"
)

// mockOrchestrator tracks state changes reported by the queue.
type mockOrchestrator struct {
	mu         sync.Mutex
	dispatched map[string]string
	started    map[string]bool
	completed  map[string]bool
	failed     map[string]string
}

func newMockOrchestrator() *mockOrchestrator {
	return &mockOrchestrator{
		dispatched: make(map[string]string),
		started:    make(map[string]bool),
		completed:  make(map[string]bool),
		failed:     make(map[string]string),
	}
}

func (m *mockOrchestrator) MarkDispatched(_ context.Context, jobID, workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatched[jobID] = workerID
	return nil
}

func (m *mockOrchestrator) MarkStarted(_ context.Context, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started[jobID] = true
	return nil
}

func (m *mockOrchestrator) MarkCompleted(_ context.Context, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed[jobID] = true
	return nil
}

func (m *mockOrchestrator) MarkFailed(_ context.Context, jobID, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed[jobID] = reason
	return nil
}

func TestManager_SubmitAndExecute(t *testing.T) {
	emulator := redisstream.NewEmulator()
	orch := newMockOrchestrator()

	cfg := config.Default()
	cfg.WorkerCount = 2
	cfg.ConsumerBlock = 50 * time.Millisecond
	cfg.PendingCheckInterval = 100 * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(1)

	// Handler that reports success.
	handler := func(ctx context.Context, msg types.Message) error {
		defer wg.Done()
		if msg.JobID != "job-test-1" {
			t.Errorf("expected job ID job-test-1, got %s", msg.JobID)
		}
		return nil
	}

	mgr, err := NewManager(Params{
		Config:  cfg,
		Client:  emulator,
		Orch:    orch,
		Handler: handler,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer mgr.Stop()

	// Schedule job.
	j := job.Job{
		ID:         "job-test-1",
		Language:   "python",
		MaxRetries: 1,
	}

	if err := mgr.Schedule(ctx, j); err != nil {
		t.Fatalf("failed to schedule job: %v", err)
	}

	// Wait for handler execution.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for job execution")
	}

	// Verify orchestrator transitions were marked.
	orch.mu.Lock()
	defer orch.mu.Unlock()

	if _, ok := orch.dispatched["job-test-1"]; !ok {
		t.Error("expected job-test-1 to be marked dispatched")
	}
	if !orch.started["job-test-1"] {
		t.Error("expected job-test-1 to be marked started")
	}
	if !orch.completed["job-test-1"] {
		t.Error("expected job-test-1 to be marked completed")
	}
}

func TestManager_WorkerHeartbeatTimeout(t *testing.T) {
	emulator := redisstream.NewEmulator()
	orch := newMockOrchestrator()

	cfg := config.Default()
	cfg.WorkerCount = 1
	cfg.HeartbeatCheckInterval = 50 * time.Millisecond
	cfg.HeartbeatTimeout = 150 * time.Millisecond
	cfg.HeartbeatInterval = 20 * time.Millisecond

	mgr, err := NewManager(Params{
		Config:  cfg,
		Client:  emulator,
		Orch:    orch,
		Handler: func(context.Context, types.Message) error { return nil },
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer mgr.Stop()

	// Wait for worker startup registration.
	time.Sleep(50 * time.Millisecond)

	// Inject a dummy offline worker manually into registry.
	dummyWorker := types.Worker{
		ID:           "dummy-zombie",
		Capabilities: []string{"test"},
		Health:       types.HealthHealthy,
		LastHeartbeat: time.Now().Add(-1 * time.Second), // Registered in the past
	}

	if err := mgr.Registry().Register(dummyWorker); err != nil {
		t.Fatalf("failed to register dummy worker: %v", err)
	}
	// Make it Idle so it triggers monitor state check.
	_ = mgr.Registry().UpdateState("dummy-zombie", types.WorkerIdle)

	// Start listening for the offline event.
	eventCh := mgr.Events().Subscribe(100)
	defer mgr.Events().Unsubscribe(eventCh)

	offlineDetected := false
	timeout := time.After(2 * time.Second)

	for !offlineDetected {
		select {
		case ev := <-eventCh:
			if ev.Type == events.WorkerOffline && ev.WorkerID == "dummy-zombie" {
				offlineDetected = true
			}
		case <-timeout:
			t.Fatal("timeout waiting for zombie worker offline transition")
		}
	}

	// Verify registry reflects state.
	w, err := mgr.Registry().Get("dummy-zombie")
	if err != nil {
		t.Fatalf("failed to get dummy worker: %v", err)
	}
	if w.State != types.WorkerOffline {
		t.Errorf("expected dummy worker to be Offline, got %s", w.State)
	}
	if w.Health != types.HealthUnhealthy {
		t.Errorf("expected dummy worker health to be Unhealthy, got %s", w.Health)
	}
}

func TestManager_JobRetryAndDLQ(t *testing.T) {
	emulator := redisstream.NewEmulator()
	orch := newMockOrchestrator()

	cfg := config.Default()
	cfg.WorkerCount = 1
	cfg.ConsumerBlock = 50 * time.Millisecond
	cfg.RetryBaseDelay = 10 * time.Millisecond
	cfg.RetryMaxDelay = 50 * time.Millisecond
	cfg.RetryJitter = 0.0
	cfg.MaxRetries = 2

	var mu sync.Mutex
	attempts := 0

	// Handler that always fails.
	handler := func(ctx context.Context, msg types.Message) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		return errors.New("execution error")
	}

	mgr, err := NewManager(Params{
		Config:  cfg,
		Client:  emulator,
		Orch:    orch,
		Handler: handler,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer mgr.Stop()

	eventCh := mgr.Events().Subscribe(100)
	defer mgr.Events().Unsubscribe(eventCh)

	j := job.Job{
		ID:         "job-fail-test",
		Language:   "go",
		MaxRetries: 2,
	}

	if err := mgr.Schedule(ctx, j); err != nil {
		t.Fatalf("failed to schedule job: %v", err)
	}

	// Wait until it reaches DLQ.
	dlqDetected := false
	timeout := time.After(3 * time.Second)

	for !dlqDetected {
		select {
		case ev := <-eventCh:
			if ev.Type == events.MovedToDeadLetter && ev.JobID == "job-fail-test" {
				dlqDetected = true
			}
		case <-timeout:
			t.Fatal("timeout waiting for job to move to DLQ")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	// Should attempt once initially, then retry 1, retry 2. Total attempts = 3.
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}

	orch.mu.Lock()
	defer orch.mu.Unlock()
	if orch.failed["job-fail-test"] == "" {
		t.Error("expected job-fail-test to be marked failed in orchestrator")
	}
}

func TestManager_GracefulShutdown(t *testing.T) {
	emulator := redisstream.NewEmulator()
	orch := newMockOrchestrator()

	cfg := config.Default()
	cfg.WorkerCount = 1
	cfg.ConsumerBlock = 50 * time.Millisecond

	var executed sync.Map

	// Handler that takes time to execute.
	handler := func(ctx context.Context, msg types.Message) error {
		time.Sleep(100 * time.Millisecond)
		executed.Store(msg.JobID, true)
		return nil
	}

	mgr, err := NewManager(Params{
		Config:  cfg,
		Client:  emulator,
		Orch:    orch,
		Handler: handler,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}

	j := job.Job{
		ID:       "job-graceful-1",
		Language: "python",
	}

	if err := mgr.Schedule(ctx, j); err != nil {
		t.Fatalf("failed to schedule job: %v", err)
	}

	// Wait briefly for job to start executing.
	time.Sleep(30 * time.Millisecond)

	// Stop manager. Should wait for handler to finish.
	mgr.Stop()

	// Verify that job completed execution.
	if _, ok := executed.Load("job-graceful-1"); !ok {
		t.Error("expected job-graceful-1 to complete processing before worker shutdown")
	}
}

func TestManager_CancelPendingJob(t *testing.T) {
	emulator := redisstream.NewEmulator()
	orch := newMockOrchestrator()

	cfg := config.Default()
	// Zero workers so consumer cannot process the job, leaving it pending.
	cfg.WorkerCount = 0

	mgr, err := NewManager(Params{
		Config:  cfg,
		Client:  emulator,
		Orch:    orch,
		Handler: func(context.Context, types.Message) error { return nil },
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer mgr.Stop()

	j := job.Job{
		ID:       "job-cancel-test",
		Language: "python",
	}

	if err := mgr.Schedule(ctx, j); err != nil {
		t.Fatalf("failed to schedule job: %v", err)
	}

	// Check stream length is 1.
	length, err := emulator.Len(ctx, cfg.Streams.Execution)
	if err != nil {
		t.Fatalf("failed to get stream len: %v", err)
	}
	if length != 1 {
		t.Errorf("expected stream length 1, got %d", length)
	}

	// Cancel the job.
	if err := mgr.Cancel(ctx, "job-cancel-test"); err != nil {
		t.Fatalf("failed to cancel job: %v", err)
	}

	// Check stream length is 0.
	length, err = emulator.Len(ctx, cfg.Streams.Execution)
	if err != nil {
		t.Fatalf("failed to get stream len: %v", err)
	}
	if length != 0 {
		t.Errorf("expected stream length 0, got %d", length)
	}
}
