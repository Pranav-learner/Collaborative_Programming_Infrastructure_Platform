package timeout

import (
	"context"
	"sync"
	"time"

	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/lifecycle"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

// TimeoutController manages state-based execution, idle, and setup timeouts.
type TimeoutController struct {
	mu           sync.RWMutex
	bus          *events.Bus
	adapter      runtime.RuntimeAdapter
	lifecycle    *lifecycle.LifecycleManager
	idleTimeout  time.Duration
	setupTimeout time.Duration
	lastActivity map[string]time.Time
}

// NewTimeoutController initializes a new TimeoutController.
func NewTimeoutController(bus *events.Bus, adapter runtime.RuntimeAdapter, lc *lifecycle.LifecycleManager, idle, setup time.Duration) *TimeoutController {
	if idle == 0 {
		idle = 5 * time.Minute
	}
	if setup == 0 {
		setup = 1 * time.Minute
	}
	return &TimeoutController{
		bus:          bus,
		adapter:      adapter,
		lifecycle:    lc,
		idleTimeout:  idle,
		setupTimeout: setup,
		lastActivity: make(map[string]time.Time),
	}
}

// RecordActivity registers interaction to reset idle timers.
func (tc *TimeoutController) RecordActivity(sandboxID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.lastActivity[sandboxID] = time.Now()
}

// CheckDeadlines passive check to run for a given sandbox session.
func (tc *TimeoutController) CheckDeadlines(ctx context.Context, sess *types.SandboxSession) error {
	st := sess.GetState()
	if st == types.StateTimedOut || st == types.StateFailed || st == types.StateCompleted || st == types.StateTerminating || st == types.StateCleaning || st == types.StateDestroyed {
		return nil
	}

	tc.mu.RLock()
	idleDur := tc.idleTimeout
	setupDur := tc.setupTimeout
	lastAct, hasActivity := tc.lastActivity[sess.ID]
	tc.mu.RUnlock()

	now := time.Now()

	// 1. Startup/Setup Timeout check (if stuck in Created/Preparing)
	if st == types.StateCreated || st == types.StatePreparing {
		if now.Sub(sess.CreatedAt) > setupDur {
			return tc.handleTimeout(ctx, sess, "Startup timeout reached")
		}
	}

	// 2. Idle Timeout check (if Ready for too long without activity)
	if st == types.StateReady {
		refTime := sess.CreatedAt
		if hasActivity {
			refTime = lastAct
		}
		if now.Sub(refTime) > idleDur {
			return tc.handleTimeout(ctx, sess, "Idle timeout reached")
		}
	}

	// 3. Execution/Lifetime Timeout check
	if now.After(sess.GetExpiresAt()) {
		return tc.handleTimeout(ctx, sess, "Execution deadline exceeded")
	}

	return nil
}

// handleTimeout initiates transition to StateTimedOut and escalates to SIGKILL if graceful termination stalls.
func (tc *TimeoutController) handleTimeout(ctx context.Context, sess *types.SandboxSession, reason string) error {
	// Transition to StateTimedOut
	_ = tc.lifecycle.Transition(sess, types.StateTimedOut)

	cID := sess.GetContainerID()
	if cID == "" {
		return nil
	}

	if tc.bus != nil {
		tc.bus.Publish(events.Event{
			Type:           events.ExecutionTimedOut,
			SandboxID:      sess.ID,
			JobID:          sess.JobID,
			Timestamp:      time.Now(),
			LifecycleState: string(types.StateTimedOut),
			Severity:       "Critical",
			Origin:         "timeout",
			Payload:        map[string]any{"reason": reason},
		})
	}

	// Graceful termination attempt
	graceCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- tc.adapter.StopContainer(graceCtx, cID, 2*time.Second)
	}()

	select {
	case err := <-done:
		if err == nil {
			return nil
		}
	case <-graceCtx.Done():
		// Graceful timeout expired - escalate to SIGKILL
	}

	// Escalation to SIGKILL (stop with 0 timeout)
	killCtx, killCancel := context.WithTimeout(ctx, 2*time.Second)
	defer killCancel()

	return tc.adapter.StopContainer(killCtx, cID, 0)
}

// Remove cleans up last activity entries for a destroyed sandbox.
func (tc *TimeoutController) Remove(sandboxID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	delete(tc.lastActivity, sandboxID)
}
