package deadletter

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cpip/internal/queue/config"
	"cpip/internal/queue/events"
	"cpip/internal/queue/metrics"
	"cpip/internal/queue/redisstream"
	"cpip/internal/queue/types"
)

// DeadLetterQueue manages failed messages that have exhausted their retries.
type DeadLetterQueue struct {
	cfg     config.Config
	client  redisstream.Client
	metrics metrics.Recorder
	bus     *events.Bus
	log     *slog.Logger
}

// New constructs a DeadLetterQueue manager.
func New(
	cfg config.Config,
	client redisstream.Client,
	rec metrics.Recorder,
	bus *events.Bus,
	log *slog.Logger,
) *DeadLetterQueue {
	if log == nil {
		log = slog.Default()
	}
	return &DeadLetterQueue{
		cfg:     cfg,
		client:  client,
		metrics: rec,
		bus:     bus,
		log:     log.With("subsystem", "deadletter"),
	}
}

// Send routes a failed message to the DLQ stream.
func (d *DeadLetterQueue) Send(ctx context.Context, msg types.Message, reason string) error {
	msg.State = types.StateDeadLetter
	if msg.Metadata == nil {
		msg.Metadata = make(map[string]string)
	}
	msg.Metadata["dead_letter_reason"] = reason
	msg.Metadata["dead_letter_time"] = time.Now().Format(time.RFC3339)

	fields, err := types.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal dead-letter message: %w", err)
	}

	stream := d.cfg.Streams.DeadLetter
	streamID, err := d.client.Add(ctx, stream, fields)
	if err != nil {
		d.metrics.RetryFailed() // DLQ write failure
		return fmt.Errorf("%w: failed to publish to DLQ: %v", types.ErrRedisUnavailable, err)
	}

	d.log.Warn("message moved to dead-letter queue", "message_id", msg.MessageID, "job_id", msg.JobID, "reason", reason, "stream_id", streamID)
	d.metrics.MovedToDeadLetter(reason)

	d.bus.Publish(events.Event{
		Type:      events.MovedToDeadLetter,
		MessageID: msg.MessageID,
		JobID:     msg.JobID,
		Stream:    stream,
		State:     types.StateDeadLetter,
		Reason:    reason,
		Timestamp: time.Now(),
	})

	// Best-effort length trimming (if configured)
	if d.cfg.DeadLetterMaxLen > 0 {
		if length, err := d.client.Len(ctx, stream); err == nil && length > d.cfg.DeadLetterMaxLen {
			// In production, Redis XADD with MAXLEN handles this atomically.
			// Here we log or do best effort, but since the Client interface doesn't expose
			// range queries directly, we skip manual truncation to keep things simple.
			d.log.Debug("DLQ length exceeds cap, trimming required", "len", length, "cap", d.cfg.DeadLetterMaxLen)
		}
	}

	return nil
}

// Replay reads a message from the DLQ stream, republishes it to the primary stream,
// and deletes/acks it from the DLQ stream.
func (d *DeadLetterQueue) Replay(ctx context.Context, msgID string, targetStream string) error {
	dlqStream := d.cfg.Streams.DeadLetter
	group := "cpip-dlq-replay-group"

	// Create group on DLQ if not exists.
	_ = d.client.CreateGroup(ctx, dlqStream, group, "0")

	// Read entries from DLQ stream using consumer group.
	// Since we don't know where the entry is, we read in batches.
	consumer := "dlq-replayer"
	found := false
	var foundEntry redisstream.Entry

	for {
		entries, err := d.client.ReadGroup(ctx, redisstream.ReadGroupArgs{
			Stream:   dlqStream,
			Group:    group,
			Consumer: consumer,
			Count:    50,
			Block:    100 * time.Millisecond,
		})
		if err != nil {
			return fmt.Errorf("failed to read DLQ stream for replay: %w", err)
		}
		if len(entries) == 0 {
			break
		}

		for _, entry := range entries {
			msg, err := types.Unmarshal(entry.ID, entry.Fields)
			if err != nil {
				continue
			}

			if msg.MessageID == msgID {
				found = true
				foundEntry = entry
				break
			}
		}

		if found {
			break
		}
	}

	if !found {
		return fmt.Errorf("%w: message %s not found in DLQ", types.ErrMessageNotFound, msgID)
	}

	// Reconstruct and republish message.
	msg, err := types.Unmarshal(foundEntry.ID, foundEntry.Fields)
	if err != nil {
		return fmt.Errorf("failed to unmarshal message from DLQ: %w", err)
	}

	// Reset execution state.
	msg.State = types.StateQueued
	msg.RetryCount = 0
	msg.EnqueueTime = time.Now()
	msg.ScheduleTime = time.Time{}
	msg.WorkerID = ""
	delete(msg.Metadata, "dead_letter_reason")
	delete(msg.Metadata, "dead_letter_time")

	fields, err := types.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message for replay: %w", err)
	}

	// Publish back to target stream.
	newStreamID, err := d.client.Add(ctx, targetStream, fields)
	if err != nil {
		return fmt.Errorf("failed to republish message to target stream: %w", err)
	}

	d.log.Info("replayed message from DLQ", "message_id", msgID, "new_stream_id", newStreamID)

	// Clean up from DLQ: ACK and XDEL.
	_, _ = d.client.Ack(ctx, dlqStream, group, foundEntry.ID)
	_, _ = d.client.Delete(ctx, dlqStream, foundEntry.ID)

	return nil
}
