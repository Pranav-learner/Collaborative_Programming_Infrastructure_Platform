package workers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cpip/internal/queue/config"
	"cpip/internal/queue/events"
	"cpip/internal/queue/heartbeat"
	"cpip/internal/queue/redisstream"
	"cpip/internal/queue/registry"
	"cpip/internal/queue/retry"
	"cpip/internal/queue/types"
)

// Handler represents the callback invoked to execute a job payload.
type Handler func(ctx context.Context, msg types.Message) error

// Orchestrator represents the minimal lifecycle marking interface for workers.
type Orchestrator interface {
	MarkStarted(ctx context.Context, jobID string) error
	MarkCompleted(ctx context.Context, jobID string) error
	MarkFailed(ctx context.Context, jobID, reason string) error
}

// Pool manages a concurrent set of worker goroutines and routes assignments.
type Pool struct {
	cfg     config.Config
	reg     *registry.Registry
	monitor *heartbeat.Monitor
	retry   *retry.Manager
	client  redisstream.Client
	orch    Orchestrator
	handler Handler
	bus     *events.Bus
	log     *slog.Logger

	workers []*workerThread
	idleCh  chan string // acts as both worker selector and backpressure gate

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPool constructs a Worker Pool.
func NewPool(
	cfg config.Config,
	reg *registry.Registry,
	monitor *heartbeat.Monitor,
	retry *retry.Manager,
	client redisstream.Client,
	orch Orchestrator,
	handler Handler,
	bus *events.Bus,
	log *slog.Logger,
) *Pool {
	if log == nil {
		log = slog.Default()
	}
	return &Pool{
		cfg:     cfg,
		reg:     reg,
		monitor: monitor,
		retry:   retry,
		client:  client,
		orch:    orch,
		handler: handler,
		bus:     bus,
		log:     log.With("subsystem", "workerpool"),
		idleCh:  make(chan string, cfg.WorkerCount),
	}
}

// Start spawns the worker threads and starts their loops.
func (p *Pool) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)

	p.log.Info("starting worker pool", "workers", p.cfg.WorkerCount)
	for i := 0; i < p.cfg.WorkerCount; i++ {
		wID := fmt.Sprintf("worker-%d-%d", time.Now().UnixNano()%100000, i)
		w := &workerThread{
			id:    wID,
			pool:  p,
			jobCh: make(chan types.Message, 1),
		}
		p.workers = append(p.workers, w)
		p.wg.Add(1)
		w.Start(p.ctx)
	}
}

// Stop initiates graceful shutdown, waiting for active workers to drain and stop.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}

	// Close channels to notify workers.
	for _, w := range p.workers {
		close(w.jobCh)
	}

	p.wg.Wait()
	p.log.Info("worker pool stopped cleanly")
}

// Submit hands off a message to a specific worker's channel.
func (p *Pool) Submit(msg types.Message, workerID string) {
	for _, w := range p.workers {
		if w.id == workerID {
			select {
			case w.jobCh <- msg:
			case <-p.ctx.Done():
			}
			return
		}
	}
	p.log.Error("submitted job to unknown worker ID", "worker_id", workerID)
}

// IdleChan returns the worker selector channel.
func (p *Pool) IdleChan() <-chan string {
	return p.idleCh
}

// ReleaseWorker returns a worker ID back to the idle selector channel.
func (p *Pool) ReleaseWorker(workerID string) {
	select {
	case p.idleCh <- workerID:
	default:
		p.log.Warn("failed to release worker; channel full", "worker_id", workerID)
	}
}

// workerThread represents an active worker goroutine.
type workerThread struct {
	id    string
	pool  *Pool
	jobCh chan types.Message

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (w *workerThread) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancel(ctx)

	// Register worker in registry.
	workerObj := types.Worker{
		ID:           w.id,
		Capabilities: []string{"go", "python", "js"}, // Default capabilities
		Health:       types.HealthHealthy,
	}

	if err := w.pool.reg.Register(workerObj); err != nil {
		w.pool.log.Error("failed to register worker in registry", "worker_id", w.id, "err", err)
		return
	}

	w.wg.Add(2)
	go w.run()
	go w.heartbeatLoop()
}

func (w *workerThread) heartbeatLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.pool.cfg.HeartbeatInterval)
	defer ticker.Stop()

	// Initial heartbeat.
	_ = w.pool.monitor.Heartbeat(w.ctx, w.id)

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			_ = w.pool.monitor.Heartbeat(w.ctx, w.id)
		}
	}
}

func (w *workerThread) run() {
	defer w.pool.wg.Done()
	defer w.wg.Done()
	defer func() {
		w.cancel()
		// Deregister worker on exit.
		_ = w.pool.reg.Deregister(w.id)
	}()

	// Move worker to Idle.
	if err := w.pool.reg.UpdateState(w.id, types.WorkerIdle); err != nil {
		w.pool.log.Error("failed to update worker state to Idle", "worker_id", w.id, "err", err)
		return
	}

	// Publish register event.
	w.pool.bus.Publish(events.Event{
		Type:      events.WorkerRegistered,
		WorkerID:  w.id,
		Timestamp: time.Now(),
	})

	// Initial push to idle selector.
	select {
	case w.pool.idleCh <- w.id:
	case <-w.ctx.Done():
		return
	}

	for {
		select {
		case <-w.ctx.Done():
			return
		case msg, ok := <-w.jobCh:
			if !ok {
				return
			}
			w.execute(msg)

			// Push back to idle selector.
			select {
			case w.pool.idleCh <- w.id:
			case <-w.ctx.Done():
				return
			}
		}
	}
}

func (w *workerThread) execute(msg types.Message) {
	// 1. Transition worker to Executing.
	if err := w.pool.reg.UpdateState(w.id, types.WorkerExecuting); err != nil {
		w.pool.log.Error("failed to transition worker to executing", "worker_id", w.id, "err", err)
		return
	}

	// Increment started jobs counter.
	_ = w.pool.reg.UpdateStats(w.id, func(s *types.WorkerStats) {
		s.Processed++
	})

	w.pool.log.Info("worker executing job", "worker_id", w.id, "job_id", msg.JobID)

	// 2. Mark started in Execution Orchestrator.
	if w.pool.orch != nil {
		if err := w.pool.orch.MarkStarted(w.ctx, msg.JobID); err != nil {
			w.pool.log.Warn("failed to mark job started in orchestrator", "job_id", msg.JobID, "err", err)
		}
	}

	// 3. Invoke handler.
	start := time.Now()
	err := w.pool.handler(w.ctx, msg)
	duration := time.Since(start)

	// Clean up assignment and transition.
	stream := w.pool.cfg.Streams.Execution
	group := w.pool.cfg.Streams.Group

	if err == nil {
		// Success path.
		_ = w.pool.reg.UpdateState(w.id, types.WorkerCompleted)
		_ = w.pool.reg.UpdateCurrentJob(w.id, "", "")

		if w.pool.orch != nil {
			_ = w.pool.orch.MarkCompleted(w.ctx, msg.JobID)
		}

		// Acknowledge & delete from Stream.
		_, _ = w.pool.client.Ack(w.ctx, stream, group, msg.StreamID)
		_, _ = w.pool.client.Delete(w.ctx, stream, msg.StreamID)

		_ = w.pool.reg.UpdateStats(w.id, func(s *types.WorkerStats) {
			s.Succeeded++
			s.BusyTime += duration
		})

		w.pool.log.Info("worker completed job successfully", "worker_id", w.id, "job_id", msg.JobID, "duration_ms", duration.Milliseconds())

		w.pool.bus.Publish(events.Event{
			Type:      events.MessageAcknowledged,
			MessageID: msg.MessageID,
			JobID:     msg.JobID,
			WorkerID:  w.id,
			State:     types.StateCompleted,
			Timestamp: time.Now(),
		})
	} else {
		// Failure path.
		_ = w.pool.reg.UpdateState(w.id, types.WorkerFailed)
		_ = w.pool.reg.UpdateCurrentJob(w.id, "", "")

		if w.pool.orch != nil {
			_ = w.pool.orch.MarkFailed(w.ctx, msg.JobID, err.Error())
		}

		// Reschedule / retry message.
		// Note: The Retry Manager will route to DLQ if max retries are exceeded.
		_ = w.pool.retry.ScheduleRetry(w.ctx, msg, err)

		// Acknowledge and delete current stream entry since we've handled retry/DLQ reschedule.
		_, _ = w.pool.client.Ack(w.ctx, stream, group, msg.StreamID)
		_, _ = w.pool.client.Delete(w.ctx, stream, msg.StreamID)

		_ = w.pool.reg.UpdateStats(w.id, func(s *types.WorkerStats) {
			s.Failed++
			s.BusyTime += duration
		})

		w.pool.log.Warn("worker job failed", "worker_id", w.id, "job_id", msg.JobID, "err", err, "duration_ms", duration.Milliseconds())

		// Transition to Recovering and back to Idle.
		_ = w.pool.reg.UpdateState(w.id, types.WorkerRecovering)
		time.Sleep(100 * time.Millisecond) // Simulated recovery/cooldown period
	}

	// Transition back to Idle.
	_ = w.pool.reg.UpdateState(w.id, types.WorkerIdle)
}
