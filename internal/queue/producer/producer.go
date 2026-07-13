package producer

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

// Producer serializes and publishes messages to the Redis Streams queue.
type Producer struct {
	cfg     config.Config
	client  redisstream.Client
	metrics metrics.Recorder
	bus     *events.Bus
	log     *slog.Logger
}

// New constructs a new Producer.
func New(
	cfg config.Config,
	client redisstream.Client,
	rec metrics.Recorder,
	bus *events.Bus,
	log *slog.Logger,
) *Producer {
	if log == nil {
		log = slog.Default()
	}
	return &Producer{
		cfg:     cfg,
		client:  client,
		metrics: rec,
		bus:     bus,
		log:     log.With("subsystem", "producer"),
	}
}

// Publish enqueues a message in the Redis Stream.
func (p *Producer) Publish(ctx context.Context, msg types.Message) (string, error) {
	// Validate basic requirements.
	if msg.JobID == "" {
		return "", fmt.Errorf("%w: job ID cannot be empty", types.ErrInvalidMessage)
	}
	if msg.MessageID == "" {
		msg.MessageID = msg.JobID // default to JobID if not specified
	}

	msg.State = types.StateQueued
	msg.EnqueueTime = time.Now()

	fields, err := types.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("failed to marshal queue message: %w", err)
	}

	stream := p.cfg.Streams.Execution
	streamID, err := p.client.Add(ctx, stream, fields)
	if err != nil {
		return "", fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
	}

	p.log.Debug("published message to stream", "message_id", msg.MessageID, "job_id", msg.JobID, "stream_id", streamID)

	p.metrics.MessagePublished(msg.Priority.String())

	p.bus.Publish(events.Event{
		Type:      events.MessageQueued,
		MessageID: msg.MessageID,
		JobID:     msg.JobID,
		Stream:    stream,
		State:     types.StateQueued,
		Timestamp: time.Now(),
	})

	return streamID, nil
}
