package retry

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"cpip/internal/queue/config"
	"cpip/internal/queue/deadletter"
	"cpip/internal/queue/events"
	"cpip/internal/queue/metrics"
	"cpip/internal/queue/redisstream"
	"cpip/internal/queue/types"
)

// Manager coordinates job retries using exponential backoff.
type Manager struct {
	cfg     config.Config
	dlq     *deadletter.DeadLetterQueue
	client  redisstream.Client
	metrics metrics.Recorder
	bus     *events.Bus
	log     *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	rngMu sync.Mutex
	rng   *rand.Rand
}

// NewManager constructs a Retry Manager.
func NewManager(
	cfg config.Config,
	dlq *deadletter.DeadLetterQueue,
	client redisstream.Client,
	rec metrics.Recorder,
	bus *events.Bus,
	log *slog.Logger,
) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		cfg:     cfg,
		dlq:     dlq,
		client:  client,
		metrics: rec,
		bus:     bus,
		log:     log.With("subsystem", "retry"),
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Start initializes the manager context.
func (r *Manager) Start(ctx context.Context) {
	r.ctx, r.cancel = context.WithCancel(ctx)
}

// Stop cancels all pending retries and waits for outstanding goroutines.
func (r *Manager) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

// ScheduleRetry schedules a message for retry after calculating exponential backoff.
// If the message has exceeded maximum retries, it is forwarded to the DLQ.
func (r *Manager) ScheduleRetry(ctx context.Context, msg types.Message, err error) error {
	maxRetries := msg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = r.cfg.MaxRetries
	}

	if msg.RetryCount >= maxRetries {
		reason := fmt.Sprintf("max retries exceeded (%d/%d). last error: %v", msg.RetryCount, maxRetries, err)
		r.log.Info("message exceeded max retries, forwarding to DLQ", "message_id", msg.MessageID, "retry_count", msg.RetryCount)
		return r.dlq.Send(ctx, msg, reason)
	}

	// Calculate exponential backoff: delay = BaseDelay * 2^attempt
	attempt := msg.RetryCount
	backoffFactor := math.Pow(2, float64(attempt))
	delay := time.Duration(float64(r.cfg.RetryBaseDelay) * backoffFactor)

	if delay > r.cfg.RetryMaxDelay {
		delay = r.cfg.RetryMaxDelay
	}

	// Apply jitter (plus or minus 15%)
	r.rngMu.Lock()
	jitterPercent := 0.85 + r.rng.Float64()*0.30 // range [0.85, 1.15]
	r.rngMu.Unlock()
	delay = time.Duration(float64(delay) * jitterPercent)

	// Increment retry count for the next attempt.
	msg.RetryCount++
	msg.State = types.StateRetry
	msg.ScheduleTime = time.Now().Add(delay)

	r.log.Info("scheduling retry for job",
		"message_id", msg.MessageID,
		"job_id", msg.JobID,
		"attempt", msg.RetryCount,
		"delay_seconds", delay.Seconds(),
		"err", err,
	)

	r.metrics.RetryScheduled(msg.RetryCount)

	r.bus.Publish(events.Event{
		Type:      events.RetryScheduled,
		MessageID: msg.MessageID,
		JobID:     msg.JobID,
		State:     types.StateRetry,
		Reason:    err.Error(),
		Timestamp: time.Now(),
	})

	r.wg.Add(1)
	go r.waitAndRequeue(msg, delay)

	return nil
}

func (r *Manager) waitAndRequeue(msg types.Message, delay time.Duration) {
	defer r.wg.Done()

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-r.ctx.Done():
		r.log.Info("aborting retry wait due to manager shutdown", "message_id", msg.MessageID)
		return
	case <-timer.C:
	}

	// Re-enqueue the message into the primary execution stream.
	msg.State = types.StateQueued
	msg.EnqueueTime = time.Now()
	msg.ScheduleTime = time.Time{}
	msg.WorkerID = ""

	fields, err := types.Marshal(msg)
	if err != nil {
		r.log.Error("failed to marshal message for retry requeue", "message_id", msg.MessageID, "err", err)
		return
	}

	// Perform requeue.
	// Note: We use background context here because r.ctx may be closing down, but we want to finish enqueuing if possible
	// or let it fail naturally. To be safe, we'll use a short context timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream := r.cfg.Streams.Execution
	newStreamID, err := r.client.Add(ctx, stream, fields)
	if err != nil {
		r.log.Error("failed to republish message during retry", "message_id", msg.MessageID, "err", err)
		// Send to DLQ since we cannot requeue
		_ = r.dlq.Send(ctx, msg, fmt.Sprintf("failed to enqueue retry: %v", err))
		return
	}

	r.log.Info("job successfully requeued for retry", "message_id", msg.MessageID, "job_id", msg.JobID, "new_stream_id", newStreamID)

	r.bus.Publish(events.Event{
		Type:      events.MessageQueued,
		MessageID: msg.MessageID,
		JobID:     msg.JobID,
		Stream:    stream,
		State:     types.StateQueued,
		Timestamp: time.Now(),
	})
}
