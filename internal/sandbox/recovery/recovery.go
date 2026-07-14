package recovery

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

// RecoveryManager manages the execution of recovery strategies when failures occur.
type RecoveryManager struct {
	mu         sync.RWMutex
	strategies map[Classification]RecoveryStrategy
	bus        *events.Bus
	adapter    runtime.RuntimeAdapter
	attempts   int64
	successes  int64
	failures   int64
}

// NewRecoveryManager initializes a new RecoveryManager.
func NewRecoveryManager(bus *events.Bus, adapter runtime.RuntimeAdapter) *RecoveryManager {
	rm := &RecoveryManager{
		strategies: make(map[Classification]RecoveryStrategy),
		bus:        bus,
		adapter:    adapter,
	}
	// Register default strategies
	rm.RegisterStrategy(&ContainerCrashStrategy{})
	rm.RegisterStrategy(&TimeoutStrategy{})
	rm.RegisterStrategy(&PolicyViolationStrategy{})
	rm.RegisterStrategy(&SecurityViolationStrategy{})
	rm.RegisterStrategy(&WorkspaceFailureStrategy{})
	rm.RegisterStrategy(&NonRecoverableStrategy{})
	return rm
}

// RegisterStrategy registers a strategy for a classification.
func (rm *RecoveryManager) RegisterStrategy(s RecoveryStrategy) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.strategies[s.Classification()] = s
}

// Classify determines the classification of a sandbox error or status.
func (rm *RecoveryManager) Classify(sess *types.SandboxSession, err error) Classification {
	if sess.GetState() == types.StateTimedOut {
		return Timeout
	}
	if sess.GetMemoryLimitBytes() > 0 && sess.GetStats().MemoryUsageBytes > sess.GetMemoryLimitBytes() {
		return PolicyViolation
	}
	if err != nil {
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "security") || strings.Contains(errStr, "escape") || strings.Contains(errStr, "denied") {
			return SecurityViolation
		}
		if strings.Contains(errStr, "mount") || strings.Contains(errStr, "workspace") || strings.Contains(errStr, "file") {
			return WorkspaceFailure
		}
	}
	if sess.GetStatus() == "exited" {
		return ContainerCrash
	}
	return NonRecoverable
}

// AttemptRecovery invokes the matching strategy's Recover method.
func (rm *RecoveryManager) AttemptRecovery(ctx context.Context, sess *types.SandboxSession, err error) error {
	atomic.AddInt64(&rm.attempts, 1)

	class := rm.Classify(sess, err)

	rm.mu.RLock()
	strat, exists := rm.strategies[class]
	rm.mu.RUnlock()

	if !exists {
		strat = &NonRecoverableStrategy{}
	}

	if rm.bus != nil {
		rm.bus.Publish(events.Event{
			Type:           events.SandboxRecovered, // indicating recovery run
			SandboxID:      sess.ID,
			JobID:          sess.JobID,
			Timestamp:      time.Now(),
			LifecycleState: string(sess.GetState()),
			Severity:       "Info",
			Origin:         "recovery",
			Payload:        map[string]any{"classification": class, "status": "started"},
		})
	}

	if !strat.CanRecover(ctx, sess, err) {
		atomic.AddInt64(&rm.failures, 1)
		return fmt.Errorf("recovery classification %s cannot recover from this error: %w", class, err)
	}

	recoveryErr := strat.Recover(ctx, sess, rm.adapter)
	if recoveryErr != nil {
		atomic.AddInt64(&rm.failures, 1)
		if rm.bus != nil {
			rm.bus.Publish(events.Event{
				Type:           events.ExecutionFailed,
				SandboxID:      sess.ID,
				JobID:          sess.JobID,
				Timestamp:      time.Now(),
				LifecycleState: string(sess.GetState()),
				Severity:       "Critical",
				Origin:         "recovery",
				Payload:        map[string]any{"classification": class, "status": "failed", "error": recoveryErr.Error()},
			})
		}
		return recoveryErr
	}

	atomic.AddInt64(&rm.successes, 1)
	if rm.bus != nil {
		rm.bus.Publish(events.Event{
			Type:           events.SandboxHealthy,
			SandboxID:      sess.ID,
			JobID:          sess.JobID,
			Timestamp:      time.Now(),
			LifecycleState: string(sess.GetState()),
			Severity:       "Info",
			Origin:         "recovery",
			Payload:        map[string]any{"classification": class, "status": "succeeded"},
		})
	}

	return nil
}

// GetTelemetry returns execution counts for recoveries.
func (rm *RecoveryManager) GetTelemetry() (attempts, successes, failures int64) {
	return atomic.LoadInt64(&rm.attempts), atomic.LoadInt64(&rm.successes), atomic.LoadInt64(&rm.failures)
}
