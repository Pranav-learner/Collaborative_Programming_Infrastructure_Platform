package dispatcher

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cpip/internal/queue/events"
	"cpip/internal/queue/registry"
	"cpip/internal/queue/types"
)

// Orchestrator represents the minimal lifecycle marking interface from the execution orchestrator.
type Orchestrator interface {
	MarkDispatched(ctx context.Context, jobID, workerID string) error
}

// Dispatcher coordinates worker reservation and job assignment.
type Dispatcher struct {
	reg  *registry.Registry
	orch Orchestrator
	bus  *events.Bus
	log  *slog.Logger
}

// New constructs a Dispatcher.
func New(
	reg *registry.Registry,
	orch Orchestrator,
	bus *events.Bus,
	log *slog.Logger,
) *Dispatcher {
	if log == nil {
		log = slog.Default()
	}
	return &Dispatcher{
		reg:  reg,
		orch: orch,
		bus:  bus,
		log:  log.With("subsystem", "dispatcher"),
	}
}

// Dispatch reserves the specified worker, updates its assignment, marks the job as
// dispatched in the orchestrator, and returns the worker for delivery.
func (d *Dispatcher) Dispatch(ctx context.Context, msg types.Message, workerID string) error {
	// 1. Verify worker exists and is Idle.
	w, err := d.reg.Get(workerID)
	if err != nil {
		return fmt.Errorf("worker not found: %w", err)
	}

	if w.State != types.WorkerIdle {
		return fmt.Errorf("%w: worker %s is in state %s", types.ErrIllegalWorkerTransition, workerID, w.State)
	}

	// 2. Reserve worker in registry.
	if err := d.reg.UpdateState(workerID, types.WorkerReserved); err != nil {
		return fmt.Errorf("failed to reserve worker: %w", err)
	}

	// Rollback handler in case dispatch fails.
	rollback := func() {
		_ = d.reg.UpdateState(workerID, types.WorkerIdle)
		_ = d.reg.UpdateCurrentJob(workerID, "", "")
	}

	if err := d.reg.UpdateCurrentJob(workerID, msg.JobID, msg.MessageID); err != nil {
		rollback()
		return fmt.Errorf("failed to assign job to worker: %w", err)
	}

	// 3. Mark job as dispatched in Execution Orchestrator.
	// If the job was cancelled or deleted, this call will fail (e.g., job.ErrIllegalTransition).
	if d.orch != nil {
		if err := d.orch.MarkDispatched(ctx, msg.JobID, workerID); err != nil {
			rollback()
			return fmt.Errorf("orchestrator rejected dispatch: %w", err)
		}
	}

	d.log.Info("job dispatched to worker", "job_id", msg.JobID, "worker_id", workerID)

	d.bus.Publish(events.Event{
		Type:      events.MessageClaimed,
		MessageID: msg.MessageID,
		JobID:     msg.JobID,
		WorkerID:  workerID,
		State:     types.StateClaimed,
		Timestamp: time.Now(),
	})

	return nil
}
