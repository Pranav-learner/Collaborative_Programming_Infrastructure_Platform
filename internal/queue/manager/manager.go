package manager

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cpip/internal/execution/job"
	execscheduler "cpip/internal/execution/scheduler"
	"cpip/internal/queue/config"
	"cpip/internal/queue/consumer"
	"cpip/internal/queue/deadletter"
	"cpip/internal/queue/dispatcher"
	"cpip/internal/queue/events"
	"cpip/internal/queue/heartbeat"
	"cpip/internal/queue/metrics"
	"cpip/internal/queue/producer"
	"cpip/internal/queue/redisstream"
	"cpip/internal/queue/registry"
	"cpip/internal/queue/retry"
	"cpip/internal/queue/types"
	"cpip/internal/queue/workers"
)

// Manager is the composition root of the queue and worker subsystem.
// It implements the execution/scheduler.Scheduler interface to connect with the orchestrator.
type Manager struct {
	cfg          config.Config
	client       redisstream.Client
	bus          *events.Bus
	metrics      metrics.Recorder
	log          *slog.Logger
	reg          *registry.Registry
	monitor      *heartbeat.Monitor
	dlq          *deadletter.DeadLetterQueue
	retry        *retry.Manager
	producer     *producer.Producer
	dispatcher   *dispatcher.Dispatcher
	pool         *workers.Pool
	consumer     *consumer.Consumer
	jobStreamIDs sync.Map // jobID -> streamID (for Cancel lookup)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Compile-time assertion that Manager satisfies the Scheduler interface.
var _ execscheduler.Scheduler = (*Manager)(nil)

// Params contains the parameters to initialize the Queue Manager.
type Params struct {
	Config    config.Config
	Client    redisstream.Client
	Orch      dispatcher.Orchestrator
	Metrics   metrics.Recorder
	Logger    *slog.Logger
	Handler   workers.Handler
	EventBus  *events.Bus // optional override
}

// NewManager constructs and wires all sub-components.
func NewManager(p Params) (*Manager, error) {
	cfg, err := p.Config.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid queue config: %w", err)
	}

	log := p.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("module", "queue")

	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}

	bus := p.EventBus
	if bus == nil {
		bus = events.New(events.Options{})
	}

	reg := registry.New()
	monitor := heartbeat.NewMonitor(cfg, reg, bus, rec, log)
	dlq := deadletter.New(cfg, p.Client, rec, bus, log)
	ret := retry.NewManager(cfg, dlq, p.Client, rec, bus, log)
	prod := producer.New(cfg, p.Client, rec, bus, log)
	disp := dispatcher.New(reg, p.Orch, bus, log)

	// Cast the Orch to workers.Orchestrator if compatible.
	var workerOrch workers.Orchestrator
	if p.Orch != nil {
		if wo, ok := p.Orch.(workers.Orchestrator); ok {
			workerOrch = wo
		}
	}

	pool := workers.NewPool(cfg, reg, monitor, ret, p.Client, workerOrch, p.Handler, bus, log)
	cons := consumer.New(cfg, p.Client, disp, pool, dlq, rec, bus, log)

	return &Manager{
		cfg:        cfg,
		client:     p.Client,
		bus:        bus,
		metrics:    rec,
		log:        log,
		reg:        reg,
		monitor:    monitor,
		dlq:        dlq,
		retry:      ret,
		producer:   prod,
		dispatcher: disp,
		pool:       pool,
		consumer:   cons,
	}, nil
}

// Start launches the background daemons (heartbeat, workers, consumer loops).
func (m *Manager) Start(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)

	// Start sub-services.
	m.retry.Start(m.ctx)
	m.monitor.Start(m.ctx)
	m.pool.Start(m.ctx)

	if err := m.consumer.Start(m.ctx); err != nil {
		m.Stop()
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	// Listen to the event bus to clear processed/dead-lettered messages from the cancellation cache.
	m.wg.Add(1)
	go m.eventListener()

	m.log.Info("queue manager started successfully")
	return nil
}

// Stop halts all loops, worker goroutines, and cleans up client connections.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}

	// Stop consumer first (prevents reading new jobs).
	m.consumer.Stop()

	// Stop workers (drains in-flight jobs).
	m.pool.Stop()

	// Stop other background runtimes.
	m.monitor.Stop()
	m.retry.Stop()

	// Wait for event listener.
	m.wg.Wait()

	m.log.Info("queue manager stopped cleanly")
}

func (m *Manager) eventListener() {
	defer m.wg.Done()

	ch := m.bus.Subscribe(100)
	defer m.bus.Unsubscribe(ch)

	for {
		select {
		case <-m.ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			// When a message is claimed (dispatched) or moved to dead letter queue,
			// it is no longer pending execution and cannot be cancelled out-of-band via XDEL.
			if ev.Type == events.MessageClaimed || ev.Type == events.MovedToDeadLetter {
				m.jobStreamIDs.Delete(ev.JobID)
			}
		}
	}
}

// --- Scheduler interface implementation -------------------------------------

// Schedule enqueues a job for execution in the Redis Streams queue.
func (m *Manager) Schedule(ctx context.Context, j job.Job) error {
	msg := types.Message{
		MessageID:         j.ID,
		JobID:             j.ID,
		CorrelationID:     j.CorrelationID,
		RequestID:         j.RequestID,
		UserID:            j.UserID,
		RoomID:            j.RoomID,
		Language:          j.Language,
		Priority:          types.Priority(j.Priority),
		RetryCount:        j.RetryCount,
		MaxRetries:        j.MaxRetries,
		EnqueueTime:       time.Now(),
		VisibilityTimeout: m.cfg.VisibilityTimeout,
		ExecutionContext: types.ExecutionContext{
			Attempt: j.RetryCount,
			Env:     j.ExecutionOptions.Env,
		},
		Metadata: j.Metadata,
		Version:  types.CurrentVersion,
		State:    types.StateCreated,
	}

	streamID, err := m.producer.Publish(ctx, msg)
	if err != nil {
		m.log.Error("failed to schedule job", "job_id", j.ID, "err", err)
		return fmt.Errorf("%w: %v", job.ErrSchedulerUnavailable, err)
	}

	// Cache the stream ID to support XDEL cancellation before dispatch.
	m.jobStreamIDs.Store(j.ID, streamID)
	return nil
}

// Cancel deletes a not-yet-dispatched job from the Redis Stream.
func (m *Manager) Cancel(ctx context.Context, jobID string) error {
	val, ok := m.jobStreamIDs.Load(jobID)
	if !ok {
		// Not found in cache or already dispatched. Under scheduler contract, this is a benign no-op.
		return nil
	}

	streamID := val.(string)
	stream := m.cfg.Streams.Execution

	deleted, err := m.client.Delete(ctx, stream, streamID)
	if err != nil {
		m.log.Error("failed to delete cancelled job from Redis stream", "job_id", jobID, "stream_id", streamID, "err", err)
		return fmt.Errorf("failed to delete job from queue: %w", err)
	}

	if deleted > 0 {
		m.log.Info("successfully cancelled and deleted job from queue", "job_id", jobID, "stream_id", streamID)
	}

	m.jobStreamIDs.Delete(jobID)
	return nil
}

// Retry re-schedules a job for another attempt.
func (m *Manager) Retry(ctx context.Context, j job.Job) error {
	m.log.Info("re-scheduling job for manual retry", "job_id", j.ID)
	return m.Schedule(ctx, j)
}

// Reprioritize updates the scheduling priority of a pending job (best-effort/no-op on Redis Streams).
func (m *Manager) Reprioritize(ctx context.Context, jobID string, p job.Priority) error {
	m.log.Warn("reprioritization requested but not natively supported on Redis Streams; no-op", "job_id", jobID, "priority", p)
	return nil
}

// --- Query methods -----------------------------------------------------------

// Registry returns the worker registry.
func (m *Manager) Registry() *registry.Registry {
	return m.reg
}

// Events returns the event bus.
func (m *Manager) Events() *events.Bus {
	return m.bus
}

// DLQ returns the Dead Letter Queue manager.
func (m *Manager) DLQ() *deadletter.DeadLetterQueue {
	return m.dlq
}

// RetryManager returns the Retry Manager.
func (m *Manager) RetryManager() *retry.Manager {
	return m.retry
}
