package lifecycle

import (
	"fmt"
	"time"

	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/metrics"
	"cpip/internal/sandbox/types"
)

// LifecycleManager manages the state machine transitions for sandbox sessions and fires events.
type LifecycleManager struct {
	bus      *events.Bus
	recorder metrics.Recorder
}

// NewLifecycleManager initializes a LifecycleManager instance.
func NewLifecycleManager(bus *events.Bus, rec metrics.Recorder) *LifecycleManager {
	return &LifecycleManager{
		bus:      bus,
		recorder: rec,
	}
}

// Transition moves a sandbox session to a new state thread-safely, firing metrics and events.
func (lm *LifecycleManager) Transition(sess *types.SandboxSession, next types.State) error {
	sess.Lock()
	defer sess.Unlock()

	current := sess.State
	if err := types.ValidateTransition(current, next); err != nil {
		lm.recorder.RecordFailure(sess.Language, string(current)+"-to-"+string(next))
		return err
	}

	sess.State = next

	// Distribute event matching types
	var evType events.Type
	switch next {
	case types.StateCreated:
		evType = events.SandboxCreated
		lm.recorder.RecordCreate(sess.ID, sess.Language)
	case types.StatePreparing:
		evType = events.WorkspacePrepared // Workspace preparation step
	case types.StateContainerCreated:
		evType = events.ContainerCreated
	case types.StateReady:
		evType = events.SandboxReady
	case types.StateExecuting:
		evType = events.ExecutionAttached
		lm.recorder.RecordExecution(sess.ID)
	case types.StateCleaning:
		evType = events.CleanupStarted
	case types.StateDestroyed:
		evType = events.SandboxDestroyed
		lm.recorder.RecordDestroy(sess.ID)
	default:
		return fmt.Errorf("unsupported transition state: %s", next)
	}

	lm.bus.Publish(events.Event{
		Type:      evType,
		SandboxID: sess.ID,
		JobID:     sess.JobID,
		Timestamp: time.Now(),
		Payload:   sess,
	})

	return nil
}
