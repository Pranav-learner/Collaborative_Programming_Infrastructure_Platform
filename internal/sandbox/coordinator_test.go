package sandbox_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/sandbox/cleanup"
	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/manager"
	"cpip/internal/sandbox/metrics"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

// CoordinatorMockAdapter implements custom behaviors for testing recovery and timeouts.
type CoordinatorMockAdapter struct {
	mu           sync.Mutex
	running      bool
	state        string
	stats        types.Stats
	stopCalls    int32
	stopTimeout  time.Duration
	stopErr      error
	inspectCalls int32
	inspectErr   error
	startCalls   int32
}

func (m *CoordinatorMockAdapter) CreateContainer(ctx context.Context, cfg runtime.ContainerConfig) (string, error) {
	return "test-container-id", nil
}

func (m *CoordinatorMockAdapter) StartContainer(ctx context.Context, containerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.AddInt32(&m.startCalls, 1)
	m.running = true
	m.state = "running"
	return nil
}

func (m *CoordinatorMockAdapter) StopContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.AddInt32(&m.stopCalls, 1)
	m.stopTimeout = timeout
	if m.stopErr != nil {
		return m.stopErr
	}
	m.running = false
	m.state = "exited"
	return nil
}

func (m *CoordinatorMockAdapter) RemoveContainer(ctx context.Context, containerID string) error {
	return nil
}

func (m *CoordinatorMockAdapter) InspectContainer(ctx context.Context, containerID string) (runtime.ContainerInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.AddInt32(&m.inspectCalls, 1)
	if m.inspectErr != nil {
		return runtime.ContainerInfo{}, m.inspectErr
	}
	return runtime.ContainerInfo{
		ID:      containerID,
		Running: m.running,
		State:   m.state,
	}, nil
}

func (m *CoordinatorMockAdapter) PullImage(ctx context.Context, image string) error {
	return nil
}

func (m *CoordinatorMockAdapter) ImageExists(ctx context.Context, image string) (bool, error) {
	return true, nil
}

func (m *CoordinatorMockAdapter) GetContainerLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error {
	return nil
}

func (m *CoordinatorMockAdapter) GetContainerStats(ctx context.Context, containerID string) (types.Stats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats, nil
}

func TestTransitionHooks(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := &CoordinatorMockAdapter{running: true, state: "running"}
	mgr := manager.NewSandboxManager(cfg, adapter, rec)
	defer mgr.GetLifecycleCoordinator().Stop()

	ctx := context.Background()
	sess, err := mgr.CreateSandbox(ctx, "job-hooks", "python3", 10*time.Second, "", "", nil)
	if err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}

	lm := mgr.Lifecycle()
	if lm == nil {
		t.Fatal("LifecycleManager is nil")
	}

	beforeCalled := false
	afterCalled := false
	rollbackCalled := false

	// Register hooks
	lm.RegisterBeforeHook(func(sess *types.SandboxSession, next types.State) error {
		if next == types.StateExecuting {
			beforeCalled = true
		}
		return nil
	})

	lm.RegisterAfterHook(func(sess *types.SandboxSession, next types.State) {
		if next == types.StateExecuting {
			afterCalled = true
		}
	})

	lm.RegisterRollbackHook(func(sess *types.SandboxSession, prev types.State) {
		if prev == types.StateExecuting {
			rollbackCalled = true
		}
	})

	// 1. Successful transition
	err = mgr.Start(ctx, sess.ID)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !beforeCalled || !afterCalled {
		t.Errorf("Hooks not invoked. Before: %v, After: %v", beforeCalled, afterCalled)
	}

	// 2. Aborted transition with rollback
	lm.RegisterBeforeHook(func(sess *types.SandboxSession, next types.State) error {
		if next == types.StateReady {
			return errors.New("abort transition")
		}
		return nil
	})

	err = mgr.Stop(ctx, sess.ID, 1*time.Second)
	if err == nil {
		t.Error("Expected transition to fail due to hook rejection")
	}

	if !rollbackCalled {
		t.Error("Expected Rollback hook to be invoked")
	}
}

func TestThresholdAlertsAndTermination(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := &CoordinatorMockAdapter{
		running: true,
		state:   "running",
		stats: types.Stats{
			CPUPercentage:    10.0,
			MemoryUsageBytes: 98 * 1024 * 1024, // 98MB (Default limit in policy Resolver is 64MB, or custom)
		},
	}
	mgr := manager.NewSandboxManager(cfg, adapter, rec)
	defer mgr.GetLifecycleCoordinator().Stop()

	ctx := context.Background()
	// Set memory limit to 50MB so 98MB is a clear violation
	sess, err := mgr.CreateSandbox(ctx, "job-threshold", "python3", 10*time.Second, "", "", map[string]any{"memory_limit_bytes": int64(50 * 1024 * 1024)})
	if err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}

	// Transition to executing
	err = mgr.Start(ctx, sess.ID)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	eventCh := mgr.EventBus().Subscribe(100)
	defer mgr.EventBus().Unsubscribe(eventCh)

	// Trigger watcher manually
	mgr.GetLifecycleCoordinator().TickWatcher(ctx)

	// Wait for event and termination to propagate
	time.Sleep(100 * time.Millisecond)

	foundThresholdExceeded := false
	for len(eventCh) > 0 {
		e := <-eventCh
		if e.Type == events.ResourceThresholdExceeded {
			foundThresholdExceeded = true
		}
	}

	if !foundThresholdExceeded {
		t.Error("Expected ResourceThresholdExceeded event to be published")
	}

	// Check if session got terminated/cleaned up
	_, err = mgr.Registry().Get(sess.ID)
	if err == nil {
		t.Error("Expected sandbox to be cleaned up after resource violation")
	}
}

func TestMultiPhaseRecoveryFlow(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := &CoordinatorMockAdapter{
		running: true, // Start running
		state:   "running",
	}
	mgr := manager.NewSandboxManager(cfg, adapter, rec)
	defer mgr.GetLifecycleCoordinator().Stop()

	ctx := context.Background()
	sess, err := mgr.CreateSandbox(ctx, "job-recovery", "python3", 10*time.Second, "", "", nil)
	if err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}

	// Transition to executing
	err = mgr.Start(ctx, sess.ID)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Simulate container crash
	adapter.mu.Lock()
	adapter.running = false
	adapter.state = "exited"
	adapter.mu.Unlock()

	eventCh := mgr.EventBus().Subscribe(100)
	defer mgr.EventBus().Unsubscribe(eventCh)

	// Trigger health check manually (will detect crashed container and trigger recovery)
	mgr.GetLifecycleCoordinator().TickHealth(ctx)

	// Wait for recovery goroutine
	time.Sleep(150 * time.Millisecond)

	if atomic.LoadInt32(&adapter.startCalls) <= 1 { // 1 for manager.Start, 1 for recovery
		t.Errorf("Expected StartContainer to be called during recovery, total calls: %d", atomic.LoadInt32(&adapter.startCalls))
	}

	foundRecovered := false
	for len(eventCh) > 0 {
		e := <-eventCh
		if e.Type == events.SandboxHealthy && e.Origin == "recovery" {
			foundRecovered = true
		}
	}

	if !foundRecovered {
		t.Error("Expected recovery success notification event")
	}
}

func TestPolicyDrivenCleanup(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := &CoordinatorMockAdapter{running: true, state: "running"}
	mgr := manager.NewSandboxManager(cfg, adapter, rec)
	defer mgr.GetLifecycleCoordinator().Stop()

	ctx := context.Background()
	sess, err := mgr.CreateSandbox(ctx, "job-cleanup-policy", "python3", 1*time.Millisecond, "", "", nil)
	if err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}

	// Apply GracePeriod policy
	policy := cleanup.CleanupPolicy{
		Type:        cleanup.PolicyGracePeriod,
		GracePeriod: 2 * time.Second, // Long enough to not clean up immediately
	}
	mgr.GetCleanupManager().SetPolicy(policy)

	// Tick cleanup (should not delete yet because of GracePeriod)
	mgr.GetLifecycleCoordinator().TickCleanup(ctx)
	time.Sleep(50 * time.Millisecond)

	_, err = mgr.Registry().Get(sess.ID)
	if err != nil {
		t.Error("Sandbox should not be cleaned up during grace period")
	}

	// Apply Immediate policy to verify it cleans up
	mgr.GetCleanupManager().SetPolicy(cleanup.DefaultImmediatePolicy)
	mgr.GetLifecycleCoordinator().TickCleanup(ctx)
	time.Sleep(100 * time.Millisecond)

	_, err = mgr.Registry().Get(sess.ID)
	if err == nil {
		t.Error("Sandbox should be cleaned up immediately under immediate policy")
	}
}

func TestTimeoutGracefulToForcefulEscalation(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := &CoordinatorMockAdapter{
		running: true,
		state:   "running",
		stopErr: errors.New("graceful stop timeout"), // Make graceful stop fail to trigger force kill
	}
	mgr := manager.NewSandboxManager(cfg, adapter, rec)
	defer mgr.GetLifecycleCoordinator().Stop()

	ctx := context.Background()
	// Create sandbox that is already expired
	_, err := mgr.CreateSandbox(ctx, "job-timeout", "python3", -1*time.Second, "", "", nil)
	if err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}

	// Tick timeouts
	mgr.GetLifecycleCoordinator().TickTimeout(ctx)
	time.Sleep(100 * time.Millisecond)

	// Verify StopContainer was called twice (graceful stop, then SIGKILL with timeout 0)
	calls := atomic.LoadInt32(&adapter.stopCalls)
	if calls != 2 {
		t.Errorf("Expected StopContainer to be called exactly 2 times (graceful and force), got %d", calls)
	}

	mTime := adapter.stopTimeout
	if mTime != 0 {
		t.Errorf("Expected final StopContainer call with timeout 0 (SIGKILL), got %v", mTime)
	}
}
