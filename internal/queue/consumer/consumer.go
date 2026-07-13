package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cpip/internal/queue/config"
	"cpip/internal/queue/deadletter"
	"cpip/internal/queue/dispatcher"
	"cpip/internal/queue/events"
	"cpip/internal/queue/metrics"
	"cpip/internal/queue/redisstream"
	"cpip/internal/queue/types"
	"cpip/internal/queue/workers"
)

// Consumer manages consumer group creation, read loop, and pending message recovery.
type Consumer struct {
	cfg        config.Config
	client     redisstream.Client
	dispatcher *dispatcher.Dispatcher
	pool       *workers.Pool
	dlq        *deadletter.DeadLetterQueue
	metrics    metrics.Recorder
	bus        *events.Bus
	log        *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New constructs a new Consumer group manager.
func New(
	cfg config.Config,
	client redisstream.Client,
	disp *dispatcher.Dispatcher,
	pool *workers.Pool,
	dlq *deadletter.DeadLetterQueue,
	rec metrics.Recorder,
	bus *events.Bus,
	log *slog.Logger,
) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		cfg:        cfg,
		client:     client,
		dispatcher: disp,
		pool:       pool,
		dlq:        dlq,
		metrics:    rec,
		bus:        bus,
		log:        log.With("subsystem", "consumer"),
	}
}

// Start registers the consumer group on the Redis stream and spawns the fetch and recovery loops.
func (c *Consumer) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	stream := c.cfg.Streams.Execution
	group := c.cfg.Streams.Group

	// 1. Create consumer group if not exists.
	// We read from "$" (new messages only) by default, or "0" if we want to read historical.
	// Production configuration is usually "$" for worker group starts.
	if err := c.client.CreateGroup(c.ctx, stream, group, "$"); err != nil {
		return fmt.Errorf("failed to create consumer group %s: %w", group, err)
	}

	c.wg.Add(2)
	go c.fetchLoop()
	go c.recoveryLoop()

	c.log.Info("consumer group manager started", "stream", stream, "group", group)
	return nil
}

// Stop stops the loops and blocks until they finish processing.
func (c *Consumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.log.Info("consumer group manager stopped")
}

func (c *Consumer) fetchLoop() {
	defer c.wg.Done()
	stream := c.cfg.Streams.Execution
	group := c.cfg.Streams.Group
	consumerName := fmt.Sprintf("consumer-%d", time.Now().UnixNano()%100000)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Backpressure: Wait until a worker is available.
		var workerID string
		select {
		case <-c.ctx.Done():
			return
		case wID, ok := <-c.pool.IdleChan():
			if !ok {
				return
			}
			workerID = wID
		}

		// Read next message.
		entries, err := c.client.ReadGroup(c.ctx, redisstream.ReadGroupArgs{
			Stream:   stream,
			Group:    group,
			Consumer: consumerName,
			Count:    1,
			Block:    c.cfg.ConsumerBlock,
		})

		if err != nil {
			c.log.Error("failed to read from execution stream", "err", err)
			// Return worker back to the idle selector.
			c.pool.ReleaseWorker(workerID)
			time.Sleep(1 * time.Second) // cooldown before retrying
			continue
		}

		if len(entries) == 0 {
			// No messages. Return worker back to the idle selector.
			c.pool.ReleaseWorker(workerID)
			continue
		}

		entry := entries[0]
		msg, err := types.Unmarshal(entry.ID, entry.Fields)
		if err != nil {
			c.log.Error("failed to unmarshal claimed message", "entry_id", entry.ID, "err", err)
			// Acknowledge and delete unmarshalable message to prevent infinite loops.
			_, _ = c.client.Ack(c.ctx, stream, group, entry.ID)
			_, _ = c.client.Delete(c.ctx, stream, entry.ID)

			c.pool.ReleaseWorker(workerID)
			continue
		}

		// Dispatch job to reserved worker.
		if err := c.dispatcher.Dispatch(c.ctx, msg, workerID); err != nil {
			c.log.Warn("dispatch rejected claimed message", "job_id", msg.JobID, "err", err)
			// Rejection (e.g. cancelled) means we discard.
			_, _ = c.client.Ack(c.ctx, stream, group, entry.ID)
			_, _ = c.client.Delete(c.ctx, stream, entry.ID)

			c.pool.ReleaseWorker(workerID)
			continue
		}

		// Submit to worker pool.
		c.pool.Submit(msg, workerID)
	}
}

func (c *Consumer) recoveryLoop() {
	defer c.wg.Done()
	consumerName := fmt.Sprintf("consumer-recovery-%d", time.Now().UnixNano()%100000)

	ticker := time.NewTicker(c.cfg.PendingCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.recoverPending(consumerName)
		}
	}
}

func (c *Consumer) recoverPending(consumerName string) {
	stream := c.cfg.Streams.Execution
	group := c.cfg.Streams.Group
	startCursor := "0-0"

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		nextCursor, entries, err := c.client.AutoClaim(c.ctx, redisstream.AutoClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: consumerName,
			MinIdle:  c.cfg.VisibilityTimeout,
			Start:    startCursor,
			Count:    10,
		})

		if err != nil {
			c.log.Error("failed to autoclaim pending entries", "err", err)
			return
		}

		if len(entries) == 0 {
			break
		}

		for _, entry := range entries {
			var workerID string
			select {
			case <-c.ctx.Done():
				return
			case wID, ok := <-c.pool.IdleChan():
				if !ok {
					return
				}
				workerID = wID
			}

			msg, err := types.Unmarshal(entry.ID, entry.Fields)
			if err != nil {
				c.log.Error("failed to unmarshal recovered message", "entry_id", entry.ID, "err", err)
				_, _ = c.client.Ack(c.ctx, stream, group, entry.ID)
				_, _ = c.client.Delete(c.ctx, stream, entry.ID)

				c.pool.ReleaseWorker(workerID)
				continue
			}

			// Verify actual delivery count.
			deliveryCount := int64(1)
			pendingList, err := c.client.Pending(c.ctx, redisstream.PendingArgs{
				Stream: stream,
				Group:  group,
				Start:  entry.ID,
				End:    entry.ID,
				Count:  1,
			})
			if err == nil && len(pendingList) > 0 {
				deliveryCount = pendingList[0].DeliveryCount
			}

			maxRetries := msg.MaxRetries
			if maxRetries <= 0 {
				maxRetries = c.cfg.MaxRetries
			}

			if deliveryCount > int64(maxRetries) {
				reason := fmt.Sprintf("poison message: delivery count %d exceeded max retries %d", deliveryCount, maxRetries)
				c.log.Warn("reclaimed message exceeded max delivery count, sending to DLQ", "message_id", msg.MessageID, "delivery_count", deliveryCount)

				_ = c.dlq.Send(c.ctx, msg, reason)
				_, _ = c.client.Ack(c.ctx, stream, group, entry.ID)
				_, _ = c.client.Delete(c.ctx, stream, entry.ID)

				c.pool.ReleaseWorker(workerID)
				continue
			}

			// Update msg's retry count based on delivery count from Redis Streams
			if int(deliveryCount)-1 > msg.RetryCount {
				msg.RetryCount = int(deliveryCount) - 1
			}

			c.log.Info("reclaiming message for processing", "message_id", msg.MessageID, "job_id", msg.JobID, "delivery_count", deliveryCount)

			if err := c.dispatcher.Dispatch(c.ctx, msg, workerID); err != nil {
				c.log.Warn("dispatch rejected recovered message", "job_id", msg.JobID, "err", err)
				_, _ = c.client.Ack(c.ctx, stream, group, entry.ID)
				_, _ = c.client.Delete(c.ctx, stream, entry.ID)

				c.pool.ReleaseWorker(workerID)
				continue
			}

			c.pool.Submit(msg, workerID)
		}

		if nextCursor == "0-0" || nextCursor == startCursor {
			break
		}
		startCursor = nextCursor
	}
}
