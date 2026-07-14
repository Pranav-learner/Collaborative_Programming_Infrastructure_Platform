package coordinator

import (
	"context"
	"fmt"

	"cpip/internal/sandbox/audit"
	"cpip/internal/sandbox/cleanup"
	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/health"
	"cpip/internal/sandbox/lifecycle"
	"cpip/internal/sandbox/recovery"
	"cpip/internal/sandbox/registry"
	"cpip/internal/sandbox/scheduler"
	"cpip/internal/sandbox/statistics"
	"cpip/internal/sandbox/timeout"
	"cpip/internal/sandbox/types"
	"cpip/internal/sandbox/watcher"
)

// LifecycleCoordinator acts as the central orchestrator across all passive monitoring,
// health, timeout, recovery, stats, audit and cleanup subsystems.
type LifecycleCoordinator struct {
	reg       *registry.SandboxRegistry
	bus       *events.Bus
	lifecycle *lifecycle.LifecycleManager
	scheduler *scheduler.SandboxScheduler
	health    *health.HealthMonitor
	watcher   *watcher.ResourceWatcher
	timeout   *timeout.TimeoutController
	cleanup   *cleanup.CleanupManager
	recovery  *recovery.RecoveryManager
	stats     *statistics.StatisticsCollector
	audit     *audit.AuditLogger
}

// NewLifecycleCoordinator instantiates and wires the LifecycleCoordinator.
func NewLifecycleCoordinator(
	reg *registry.SandboxRegistry,
	bus *events.Bus,
	lc *lifecycle.LifecycleManager,
	sched *scheduler.SandboxScheduler,
	hm *health.HealthMonitor,
	rw *watcher.ResourceWatcher,
	tc *timeout.TimeoutController,
	cm *cleanup.CleanupManager,
	rm *recovery.RecoveryManager,
	sc *statistics.StatisticsCollector,
	al *audit.AuditLogger,
) *LifecycleCoordinator {
	coord := &LifecycleCoordinator{
		reg:       reg,
		bus:       bus,
		lifecycle: lc,
		scheduler: sched,
		health:    hm,
		watcher:   rw,
		timeout:   tc,
		cleanup:   cm,
		recovery:  rm,
		stats:     sc,
		audit:     al,
	}

	// Register passive loops into scheduler
	sched.RegisterWatchTask(coord.TickWatcher)
	sched.RegisterHealthTask(coord.TickHealth)
	sched.RegisterCleanupTask(coord.TickCleanup)
	sched.RegisterTimeoutTask(coord.TickTimeout)

	// Wire watcher termination trigger
	rw.RegisterTerminationHandler(coord.HandleWatcherViolation)

	return coord
}

// TickWatcher passive resource checks.
func (c *LifecycleCoordinator) TickWatcher(ctx context.Context) {
	for _, sess := range c.reg.List() {
		st := sess.GetState()
		if st == types.StateExecuting || st == types.StateRunning {
			_ = c.watcher.Watch(ctx, sess)
		}
	}
}

// TickHealth Passive health verification, triggering recovery if crashed.
func (c *LifecycleCoordinator) TickHealth(ctx context.Context) {
	for _, sess := range c.reg.List() {
		st := sess.GetState()
		if st == types.StateExecuting || st == types.StateRunning {
			snap, err := c.health.CheckHealth(ctx, sess)
			if err != nil {
				c.audit.RecordCategorized(audit.CategorySecurity, "health_check_failed", sess.ID, sess.JobID, err.Error(), nil)
				continue
			}

			// If container crashed or has low health, trigger recovery
			if snap.ContainerHealth == "unhealthy" {
				c.TriggerRecovery(ctx, sess, fmt.Errorf("container health check failed"))
			}
		}
	}
}

// TickCleanup Passive sweeps of expired sessions.
func (c *LifecycleCoordinator) TickCleanup(ctx context.Context) {
	c.cleanup.Sweep(ctx)
}

// TickTimeout Passive timeout checks.
func (c *LifecycleCoordinator) TickTimeout(ctx context.Context) {
	for _, sess := range c.reg.List() {
		_ = c.timeout.CheckDeadlines(ctx, sess)
	}
}

// TriggerRecovery transitions a sandbox state and delegates to RecoveryManager.
func (c *LifecycleCoordinator) TriggerRecovery(ctx context.Context, sess *types.SandboxSession, err error) {
	_ = c.lifecycle.Transition(sess, types.StateRecovering)
	c.audit.RecordCategorized(audit.CategoryRecovery, "recovery_triggered", sess.ID, sess.JobID, "Attempting sandbox auto-recovery", nil)

	go func(s *types.SandboxSession) {
		recErr := c.recovery.AttemptRecovery(ctx, s, err)
		if recErr != nil {
			c.audit.RecordCategorized(audit.CategoryRecovery, "recovery_failed", s.ID, s.JobID, recErr.Error(), nil)
			_ = c.lifecycle.Transition(s, types.StateFailed)
		} else {
			c.audit.RecordCategorized(audit.CategoryRecovery, "recovery_succeeded", s.ID, s.JobID, "Sandbox recovery succeeded", nil)
			_ = c.lifecycle.Transition(s, types.StateRunning)
		}
	}(sess)
}

// HandleWatcherViolation handles critical watcher limit violations.
func (c *LifecycleCoordinator) HandleWatcherViolation(ctx context.Context, id string, reason string) {
	c.audit.RecordCategorized(audit.CategoryPolicy, "policy_violation_termination", id, "", reason, nil)
	sess, err := c.reg.Get(id)
	if err == nil {
		_ = c.lifecycle.Transition(sess, types.StateFailed)
		_ = c.cleanup.CleanupSandbox(ctx, id)
	}
}

// Start initiates all timing loops and stats capture.
func (c *LifecycleCoordinator) Start(ctx context.Context) {
	c.stats.Start()
	c.scheduler.Start(ctx)
}

// Stop halts all active scheduler timing threads.
func (c *LifecycleCoordinator) Stop() {
	c.scheduler.Stop()
	c.stats.Stop()
}
