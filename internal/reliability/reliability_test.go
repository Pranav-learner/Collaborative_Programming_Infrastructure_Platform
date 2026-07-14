package reliability_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/reliability/backoff"
	"cpip/internal/reliability/backpressure"
	"cpip/internal/reliability/backup"
	"cpip/internal/reliability/bulkhead"
	"cpip/internal/reliability/circuitbreaker"
	"cpip/internal/reliability/config"
	"cpip/internal/reliability/events"
	"cpip/internal/reliability/health"
	"cpip/internal/reliability/logger"
	"cpip/internal/reliability/manager"
	"cpip/internal/reliability/metrics"
	"cpip/internal/reliability/ratelimit"
	"cpip/internal/reliability/recovery"
	"cpip/internal/reliability/retry"
	"cpip/internal/reliability/sdk"
	"cpip/internal/reliability/shutdown"
)

func TestBackoffStrategies(t *testing.T) {
	fixed := &backoff.FixedStrategy{}
	linear := &backoff.LinearStrategy{}
	exp := &backoff.ExponentialStrategy{}
	jitter := &backoff.ExponentialJitterStrategy{}
	decorr := &backoff.DecorrelatedJitterStrategy{}

	base := 10 * time.Millisecond
	max := 100 * time.Millisecond

	// Test Fixed
	if d := fixed.NextDelay(1, base, max, 0); d != base {
		t.Errorf("expected fixed delay %v, got %v", base, d)
	}

	// Test Linear
	if d := linear.NextDelay(3, base, max, 0); d != 30*time.Millisecond {
		t.Errorf("expected linear delay 30ms, got %v", d)
	}
	if d := linear.NextDelay(20, base, max, 0); d != max {
		t.Errorf("expected linear delay capped at max %v, got %v", max, d)
	}

	// Test Exponential
	if d := exp.NextDelay(3, base, max, 0); d != 40*time.Millisecond { // 10 * 2^2
		t.Errorf("expected exp delay 40ms, got %v", d)
	}

	// Test Jitter (should fall within base and exp limits)
	dJitter := jitter.NextDelay(3, base, max, 0)
	if dJitter < base || dJitter > 40*time.Millisecond {
		t.Errorf("jitter delay out of bounds: %v", dJitter)
	}

	// Test Decorrelated Jitter
	dDecorr := decorr.NextDelay(3, base, max, 10*time.Millisecond)
	if dDecorr < base || dDecorr > max {
		t.Errorf("decorrelated jitter out of bounds: %v", dDecorr)
	}
}

func TestRetryFramework(t *testing.T) {
	cfg := config.RetryConfig{
		MaxAttempts:     3,
		InitialInterval: 2 * time.Millisecond,
		MaxInterval:     10 * time.Millisecond,
		BackoffType:     config.BackoffFixed,
	}

	rec := metrics.NewInMemoryRecorder()
	bus := events.NewBus()
	defer bus.Close()

	// 1. Success first attempt
	exec := retry.NewRetryExecutor(cfg, &backoff.FixedStrategy{}, retry.DefaultClassifier, bus, rec)
	calls := 0
	err := exec.Execute(context.Background(), "test", func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected retry execution error: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}

	// 2. Transients retries and exhaustions
	calls = 0
	err = exec.Execute(context.Background(), "test", func() error {
		calls++
		return errors.New("transient error")
	})
	if err == nil {
		t.Fatal("expected retry exhaustion error, got nil")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}

	// 3. Fatal errors (no retry)
	calls = 0
	err = exec.Execute(context.Background(), "test", func() error {
		calls++
		return retry.NewFatalError(errors.New("fatal problem"))
	})
	if err == nil {
		t.Fatal("expected fatal error, got nil")
	}
	if calls != 1 {
		t.Errorf("expected only 1 call before aborting on fatal error, got %d", calls)
	}
}

func TestCircuitBreakerStates(t *testing.T) {
	cfg := config.CircuitBreakerConfig{
		FailureThreshold: 0.5,
		RecoveryTimeout:  10 * time.Millisecond,
		SuccessThreshold: 2,
		FailureWindow:    50 * time.Millisecond,
		MinRequests:      4,
	}

	rec := metrics.NewInMemoryRecorder()
	bus := events.NewBus()
	defer bus.Close()

	cb := circuitbreaker.NewCircuitBreaker("test-cb", cfg, bus, rec)

	// CLOSED initially
	if cb.State() != circuitbreaker.StateClosed {
		t.Errorf("expected closed state, got %s", cb.State())
	}

	// Make 2 successes, 2 failures (50% failure rate)
	for i := 0; i < 2; i++ {
		done, err := cb.Allow()
		if err != nil {
			t.Fatalf("unexpected allow error: %v", err)
		}
		done(true)
	}
	for i := 0; i < 2; i++ {
		done, err := cb.Allow()
		if err != nil {
			t.Fatalf("unexpected allow error: %v", err)
		}
		done(false)
	}

	// Breaker should trip OPEN
	if cb.State() != circuitbreaker.StateOpen {
		t.Errorf("expected open state, got %s", cb.State())
	}

	// Allow should fail immediately
	_, err := cb.Allow()
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}

	// Wait for Recovery Timeout
	time.Sleep(15 * time.Millisecond)

	// State should transition to HALF-OPEN on evaluate
	if cb.State() != circuitbreaker.StateHalfOpen {
		t.Errorf("expected half-open state, got %s", cb.State())
	}

	// 2 consecutive successes should CLOSE the circuit
	done, err := cb.Allow()
	if err != nil {
		t.Fatalf("unexpected allow error in half-open: %v", err)
	}
	done(true)

	if cb.State() != circuitbreaker.StateHalfOpen {
		t.Errorf("expected still half-open state, got %s", cb.State())
	}

	done, err = cb.Allow()
	if err != nil {
		t.Fatalf("unexpected allow error: %v", err)
	}
	done(true)

	if cb.State() != circuitbreaker.StateClosed {
		t.Errorf("expected closed state after successes, got %s", cb.State())
	}
}

func TestBulkheadIsolation(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	rec := metrics.NewInMemoryRecorder()

	// 1. Semaphore Bulkhead
	semBh := bulkhead.NewSemaphoreBulkhead("sem-bh", 2, bus, rec)
	r1, err := semBh.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire r1: %v", err)
	}
	r2, err := semBh.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire r2: %v", err)
	}

	// Third attempt should fail immediately
	_, err = semBh.Acquire(context.Background())
	if !errors.Is(err, bulkhead.ErrBulkheadFull) {
		t.Errorf("expected ErrBulkheadFull, got %v", err)
	}

	r1()
	r2()

	// 2. Pool Bulkhead
	poolBh := bulkhead.NewPoolBulkhead("pool-bh", 1, 1, bus, rec)
	defer poolBh.Close()

	// Run long-running tasks
	var taskRunning sync.WaitGroup
	var taskComplete sync.WaitGroup
	taskRunning.Add(1)
	taskComplete.Add(1)

	go func() {
		_ = poolBh.Execute(context.Background(), func() error {
			taskRunning.Done()
			time.Sleep(20 * time.Millisecond)
			taskComplete.Done()
			return nil
		})
	}()

	taskRunning.Wait()

	// Next task should wait in queue (queue capacity 1, total 1 running, 1 queue slots).
	// Let's execute a third task which should fail due to queue limits
	err = poolBh.Execute(context.Background(), func() error {
		return nil
	})

	// Give task time to execute or reject
	if err != nil && !errors.Is(err, bulkhead.ErrBulkheadFull) {
		t.Errorf("expected success or ErrBulkheadFull, got %v", err)
	}

	taskComplete.Wait()
}

func TestRateLimiters(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	rec := metrics.NewInMemoryRecorder()

	// 1. Token Bucket
	tb := ratelimit.NewTokenBucket("tb", 100.0, 2, bus, rec)
	if !tb.Allow() {
		t.Error("expected first permit allowed")
	}
	if !tb.Allow() {
		t.Error("expected second permit allowed")
	}
	if tb.Allow() {
		t.Error("expected third permit blocked (burst exceeded)")
	}

	// 2. Sliding Window
	sw := ratelimit.NewSlidingWindow("sw", 50*time.Millisecond, 2, bus, rec)
	if !sw.Allow() {
		t.Error("expected window allow 1")
	}
	if !sw.Allow() {
		t.Error("expected window allow 2")
	}
	if sw.Allow() {
		t.Error("expected window blocked")
	}

	time.Sleep(60 * time.Millisecond)
	if !sw.Allow() {
		t.Error("expected window allowed after slide window elapsed")
	}
}

func TestBackpressureAdmissionAndShedding(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	rec := metrics.NewInMemoryRecorder()

	// Max 2 active tasks queue limits
	bp := backpressure.NewBackpressureManager(2, 5*time.Millisecond, 10*time.Millisecond, bus, rec)

	err := bp.Acquire(context.Background(), backpressure.PriorityNormal)
	if err != nil {
		t.Fatalf("failed to acquire task 1: %v", err)
	}
	err = bp.Acquire(context.Background(), backpressure.PriorityNormal)
	if err != nil {
		t.Fatalf("failed to acquire task 2: %v", err)
	}

	// Normal task 3 should get shed
	err = bp.Acquire(context.Background(), backpressure.PriorityNormal)
	if !errors.Is(err, backpressure.ErrBackpressureShed) {
		t.Errorf("expected ErrBackpressureShed, got %v", err)
	}

	// High priority task should be allowed bypass up to maxQueueSize + 5
	err = bp.Acquire(context.Background(), backpressure.PriorityHigh)
	if err != nil {
		t.Errorf("expected High priority to bypass load shedding, got %v", err)
	}
}

func TestGracefulShutdown(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	log := logger.New(nil)

	m := shutdown.NewManager(100*time.Millisecond, bus, log)

	var order []string
	var mu sync.Mutex

	m.Register("network", 2, func(ctx context.Context) error {
		mu.Lock()
		order = append(order, "network")
		mu.Unlock()
		return nil
	})

	m.Register("database", 1, func(ctx context.Context) error {
		mu.Lock()
		order = append(order, "database")
		mu.Unlock()
		return nil
	})

	err := m.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	if len(order) != 2 || order[0] != "database" || order[1] != "network" {
		t.Errorf("expected order [database, network], got %v", order)
	}
}

func TestBackupPruningAndVerification(t *testing.T) {
	os.RemoveAll("./scratch/backup_test")
	defer os.RemoveAll("./scratch/backup_test")

	bus := events.NewBus()
	defer bus.Close()
	rec := metrics.NewInMemoryRecorder()

	cfg := config.BackupPolicy{
		Schedule:          "0 0 * * *",
		RetentionLimit:    2,
		ValidationEnabled: true,
	}

	bm, err := backup.NewBackupManager("./scratch/backup_test", cfg, bus, rec)
	if err != nil {
		t.Fatalf("failed to create backup manager: %v", err)
	}

	comps := []backup.BackupComponent{backup.ComponentPostgres, backup.ComponentRedis}

	// 1. Create backups
	m1, err := bm.CreateBackup(context.Background(), comps)
	if err != nil {
		t.Fatalf("backup 1 failed: %v", err)
	}
	m2, err := bm.CreateBackup(context.Background(), comps)
	if err != nil {
		t.Fatalf("backup 2 failed: %v", err)
	}
	m3, err := bm.CreateBackup(context.Background(), comps)
	if err != nil {
		t.Fatalf("backup 3 failed: %v", err)
	}

	// Check catalog count due to retention limit (2)
	history := bm.History()
	if len(history) != 2 {
		t.Errorf("expected catalog pruned to 2, got %d", len(history))
	}

	// 2. Restore validation
	err = bm.RestoreBackup(context.Background(), m3)
	if err != nil {
		t.Fatalf("restore backup failed: %v", err)
	}

	// Verify old file m1 was deleted
	if _, err := os.Stat(m1.Path); !os.IsNotExist(err) {
		t.Error("expected old backup archive file to be deleted")
	}
	// Verify m2 and m3 files exist
	if _, err := os.Stat(m2.Path); os.IsNotExist(err) {
		t.Error("expected backup 2 file to exist")
	}
	if _, err := os.Stat(m3.Path); os.IsNotExist(err) {
		t.Error("expected backup 3 file to exist")
	}
}

func TestDisasterRecoveryWorkflows(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	rec := metrics.NewInMemoryRecorder()

	planner := recovery.NewDisasterRecoveryPlanner(bus, rec)

	var calls []string
	var mu sync.Mutex

	// Build a plan with dependencies: A -> B -> C (A run first, then B, then C)
	plan := recovery.RecoveryPlan{
		ID:   "restore-cluster",
		Name: "Restore cluster components",
		Steps: []recovery.RecoveryStep{
			{
				Name: "Step-C",
				Action: func(ctx context.Context) error {
					mu.Lock()
					calls = append(calls, "C")
					mu.Unlock()
					return nil
				},
				Dependencies: []string{"Step-B"},
			},
			{
				Name: "Step-A",
				Action: func(ctx context.Context) error {
					mu.Lock()
					calls = append(calls, "A")
					mu.Unlock()
					return nil
				},
				Dependencies: nil,
			},
			{
				Name: "Step-B",
				Action: func(ctx context.Context) error {
					mu.Lock()
					calls = append(calls, "B")
					mu.Unlock()
					return nil
				},
				Dependencies: []string{"Step-A"},
			},
		},
	}

	report, err := planner.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("plan execution failed: %v", err)
	}

	if !report.Success {
		t.Error("expected plan success report")
	}

	if len(calls) != 3 || calls[0] != "A" || calls[1] != "B" || calls[2] != "C" {
		t.Errorf("expected execution order [A, B, C], got %v", calls)
	}
}

func TestHealthRecoveryBreakerResets(t *testing.T) {
	bus := events.NewBus()
	defer bus.Close()
	log := logger.New(nil)

	re := health.NewRecoveryEngine(bus, log)

	var hookCalled bool
	re.Register("api", func(ctx context.Context) error {
		hookCalled = true
		return nil
	}, func(ctx context.Context) error {
		return nil // returns healthy
	})

	cb := circuitbreaker.NewCircuitBreaker("test-cb", config.CircuitBreakerConfig{
		FailureThreshold: 0.1,
		RecoveryTimeout:  10 * time.Second,
		SuccessThreshold: 1,
		FailureWindow:    time.Second,
		MinRequests:      1,
	}, bus, nil)

	// Trip the breaker
	done, _ := cb.Allow()
	done(false)

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("breaker not tripped open")
	}

	re.RegisterBreaker("api", cb)

	// Trigger Recovery
	err := re.AttemptRecovery(context.Background(), "api")
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if !hookCalled {
		t.Error("recovery hook not invoked")
	}

	// Breaker should be forced closed now
	if cb.State() != circuitbreaker.StateClosed {
		t.Errorf("circuit breaker state not reset to closed, state: %s", cb.State())
	}
}

func TestReliabilityOrchestrationIntegration(t *testing.T) {
	cfg := config.DefaultPlatformConfig()
	bus := events.NewBus()
	defer bus.Close()
	rec := metrics.NewInMemoryRecorder()
	log := logger.New(nil)

	mgr, err := manager.NewManager(cfg, bus, rec, log, "./scratch/integration_test")
	if err != nil {
		t.Fatalf("failed to initialize manager: %v", err)
	}
	defer mgr.Close()
	defer os.RemoveAll("./scratch/integration_test")

	client := sdk.NewClient(mgr)

	// Test basic Protect execution
	calls := 0
	err = client.Protect(context.Background(), "default", func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("Protect call failed: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestStressConcurrencyAndRaceSafety(t *testing.T) {
	cfg := config.DefaultPlatformConfig()
	bus := events.NewBus()
	defer bus.Close()
	rec := metrics.NewInMemoryRecorder()
	log := logger.New(nil)

	mgr, err := manager.NewManager(cfg, bus, rec, log, "./scratch/stress_test")
	if err != nil {
		t.Fatalf("failed to initialize manager: %v", err)
	}
	defer mgr.Close()
	defer os.RemoveAll("./scratch/stress_test")

	client := sdk.NewClient(mgr)

	var wg sync.WaitGroup
	workers := 100
	var successCount int64

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				// Run client Protect concurrently
				err := client.Protect(context.Background(), "default", func() error {
					return nil
				})
				if err == nil {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	if successCount == 0 {
		t.Error("expected at least some successful protected executions")
	}
}
