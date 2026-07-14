package lifecycle

import (
	"fmt"
	"sync"
	"time"

	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/metrics"
	"cpip/internal/sandbox/types"
)

type BeforeHook func(sess *types.SandboxSession, next types.State) error
type AfterHook func(sess *types.SandboxSession, next types.State)
type RollbackHook func(sess *types.SandboxSession, prev types.State)

// LifecycleManager manages the state machine transitions for sandbox sessions and fires events.
type LifecycleManager struct {
	bus           *events.Bus
	recorder      metrics.Recorder
	mu            sync.RWMutex
	beforeHooks   []BeforeHook
	afterHooks    []AfterHook
	rollbackHooks []RollbackHook
}

// NewLifecycleManager initializes a LifecycleManager instance.
func NewLifecycleManager(bus *events.Bus, rec metrics.Recorder) *LifecycleManager {
	return &LifecycleManager{
		bus:      bus,
		recorder: rec,
	}
}

func (lm *LifecycleManager) RegisterBeforeHook(h BeforeHook) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.beforeHooks = append(lm.beforeHooks, h)
}

func (lm *LifecycleManager) RegisterAfterHook(h AfterHook) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.afterHooks = append(lm.afterHooks, h)
}

func (lm *LifecycleManager) RegisterRollbackHook(h RollbackHook) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.rollbackHooks = append(lm.rollbackHooks, h)
}

// Transition moves a sandbox session to a new state thread-safely, firing metrics and events.
func (lm *LifecycleManager) Transition(sess *types.SandboxSession, next types.State) error {
	lm.mu.RLock()
	befores := make([]BeforeHook, len(lm.beforeHooks))
	copy(befores, lm.beforeHooks)
	afters := make([]AfterHook, len(lm.afterHooks))
	copy(afters, lm.afterHooks)
	rollbacks := make([]RollbackHook, len(lm.rollbackHooks))
	copy(rollbacks, lm.rollbackHooks)
	lm.mu.RUnlock()

	sess.Lock()
	current := sess.State
	if err := types.ValidateTransition(current, next); err != nil {
		sess.Unlock()
		lm.recorder.RecordFailure(sess.Language, string(current)+"-to-"+string(next))
		return err
	}
	sess.Unlock()

	// Execute before hooks
	for _, h := range befores {
		if err := h(sess, next); err != nil {
			// Trigger rollback hooks
			for _, rh := range rollbacks {
				rh(sess, current)
			}
			return fmt.Errorf("before transition hook failed: %w", err)
		}
	}

	sess.Lock()
	sess.State = next
	sess.Unlock()

	// Distribute event matching types
	var evType events.Type
	var severity = "Info"
	switch next {
	case types.StateCreated:
		evType = events.SandboxCreated
		lm.recorder.RecordCreate(sess.ID, sess.Language)
	case types.StatePreparing:
		evType = events.WorkspacePrepared
	case types.StateContainerCreated:
		evType = events.ContainerCreated
	case types.StateReady:
		evType = events.SandboxReady
	case types.StateExecuting:
		evType = events.ExecutionAttached
		lm.recorder.RecordExecution(sess.ID)
	case types.StateRunning:
		evType = events.SandboxRunning
		lm.recorder.RecordExecution(sess.ID)
	case types.StateCompleted:
		evType = events.ExecutionCompleted
	case types.StateFailed:
		evType = events.ExecutionFailed
		severity = "Warning"
	case types.StateRecovering:
		evType = events.SandboxRecovered
	case types.StateTimedOut:
		evType = events.ExecutionTimedOut
		severity = "Warning"
	case types.StateTerminating:
		evType = events.ContainerStopped
	case types.StateCleaning:
		evType = events.CleanupStarted
	case types.StateDestroyed:
		evType = events.SandboxDestroyed
		lm.recorder.RecordDestroy(sess.ID)
	default:
		evType = events.Type("state_" + string(next))
	}

	lm.bus.Publish(events.Event{
		Type:           evType,
		SandboxID:      sess.ID,
		JobID:          sess.JobID,
		Timestamp:      time.Now(),
		LifecycleState: string(next),
		Severity:       severity,
		Origin:         "lifecycle",
		Payload:        sess,
	})

	// Execute after hooks
	for _, h := range afters {
		h(sess, next)
	}

	return nil
}
