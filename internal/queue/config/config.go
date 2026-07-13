// Package config defines the configuration surface for the queue and worker
// infrastructure. Configuration is injected at construction time; there is no
// global state.
package config

import (
	"time"

	"cpip/internal/queue/types"
)

// Streams names the Redis streams and consumer group used by the queue.
type Streams struct {
	// Execution is the primary work stream.
	Execution string `json:"execution"`
	// Retry is the stream that holds messages scheduled for another attempt.
	Retry string `json:"retry"`
	// DeadLetter is the stream that holds messages that exhausted retries.
	DeadLetter string `json:"dead_letter"`
	// Group is the consumer group name shared by all workers (horizontal scaling).
	Group string `json:"group"`
	// PriorityStreams optionally maps a priority to a dedicated stream (future-ready).
	PriorityStreams map[types.Priority]string `json:"priority_streams,omitempty"`
}

// Config defines the tunable parameters of the queue subsystem.
type Config struct {
	// --- Topology ---
	Streams Streams `json:"streams"`

	// --- Workers ---
	// WorkerCount is the initial number of workers in the pool.
	WorkerCount int `json:"worker_count"`
	// MaxWorkerCount bounds dynamic scaling of the pool.
	MaxWorkerCount int `json:"max_worker_count"`

	// --- Heartbeats ---
	// HeartbeatInterval is how often a worker heartbeats.
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	// HeartbeatTimeout is the age after which a worker is considered dead.
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"`
	// HeartbeatCheckInterval is the monitor's scan interval.
	HeartbeatCheckInterval time.Duration `json:"heartbeat_check_interval"`

	// --- Delivery ---
	// VisibilityTimeout is how long a claimed message may remain unacknowledged
	// before it becomes eligible for reclaim by another consumer.
	VisibilityTimeout time.Duration `json:"visibility_timeout"`
	// ConsumerBatchSize is the maximum number of messages read per XREADGROUP.
	ConsumerBatchSize int `json:"consumer_batch_size"`
	// ConsumerBlock is the blocking duration for a single XREADGROUP read.
	ConsumerBlock time.Duration `json:"consumer_block"`
	// PendingCheckInterval is how often the recovery loop scans for idle pending
	// entries to reclaim (XAUTOCLAIM).
	PendingCheckInterval time.Duration `json:"pending_check_interval"`

	// --- Retry ---
	// MaxRetries is the maximum delivery attempts before dead-lettering.
	MaxRetries int `json:"max_retries"`
	// RetryBaseDelay is the base delay for exponential backoff.
	RetryBaseDelay time.Duration `json:"retry_base_delay"`
	// RetryMaxDelay caps the exponential backoff.
	RetryMaxDelay time.Duration `json:"retry_max_delay"`
	// RetryJitter is the fractional jitter (0..1) applied to backoff.
	RetryJitter float64 `json:"retry_jitter"`

	// --- Dead letter ---
	// DeadLetterMaxLen caps the DLQ stream length (0 = unbounded).
	DeadLetterMaxLen int64 `json:"dead_letter_max_len"`

	// --- Shutdown ---
	// ShutdownGrace bounds how long graceful shutdown waits for workers to drain.
	ShutdownGrace time.Duration `json:"shutdown_grace"`
}

// Default returns a production-sensible default configuration.
func Default() Config {
	return Config{
		Streams: Streams{
			Execution:  "cpip:exec:stream",
			Retry:      "cpip:exec:retry",
			DeadLetter: "cpip:exec:dlq",
			Group:      "cpip-executors",
		},
		WorkerCount:    8,
		MaxWorkerCount: 64,

		HeartbeatInterval:      2 * time.Second,
		HeartbeatTimeout:       10 * time.Second,
		HeartbeatCheckInterval: 2 * time.Second,

		VisibilityTimeout:    30 * time.Second,
		ConsumerBatchSize:    16,
		ConsumerBlock:        2 * time.Second,
		PendingCheckInterval: 5 * time.Second,

		MaxRetries:     3,
		RetryBaseDelay: 500 * time.Millisecond,
		RetryMaxDelay:  30 * time.Second,
		RetryJitter:    0.2,

		DeadLetterMaxLen: 10000,

		ShutdownGrace: 15 * time.Second,
	}
}

// Validate normalizes zero-valued fields to their defaults and rejects
// nonsensical values, returning a normalized copy.
func (c Config) Validate() (Config, error) {
	d := Default()

	if c.Streams.Execution == "" {
		c.Streams.Execution = d.Streams.Execution
	}
	if c.Streams.Retry == "" {
		c.Streams.Retry = d.Streams.Retry
	}
	if c.Streams.DeadLetter == "" {
		c.Streams.DeadLetter = d.Streams.DeadLetter
	}
	if c.Streams.Group == "" {
		c.Streams.Group = d.Streams.Group
	}
	if c.WorkerCount <= 0 {
		c.WorkerCount = d.WorkerCount
	}
	if c.MaxWorkerCount <= 0 {
		c.MaxWorkerCount = d.MaxWorkerCount
	}
	if c.MaxWorkerCount < c.WorkerCount {
		c.MaxWorkerCount = c.WorkerCount
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = d.HeartbeatInterval
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = d.HeartbeatTimeout
	}
	if c.HeartbeatTimeout <= c.HeartbeatInterval {
		return Config{}, wrap("heartbeat_timeout must exceed heartbeat_interval")
	}
	if c.HeartbeatCheckInterval <= 0 {
		c.HeartbeatCheckInterval = d.HeartbeatCheckInterval
	}
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = d.VisibilityTimeout
	}
	if c.ConsumerBatchSize <= 0 {
		c.ConsumerBatchSize = d.ConsumerBatchSize
	}
	if c.ConsumerBlock < 0 {
		c.ConsumerBlock = d.ConsumerBlock
	}
	if c.PendingCheckInterval <= 0 {
		c.PendingCheckInterval = d.PendingCheckInterval
	}
	if c.MaxRetries < 0 {
		return Config{}, wrap("max_retries must be >= 0")
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = d.MaxRetries
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = d.RetryBaseDelay
	}
	if c.RetryMaxDelay <= 0 {
		c.RetryMaxDelay = d.RetryMaxDelay
	}
	if c.RetryMaxDelay < c.RetryBaseDelay {
		c.RetryMaxDelay = c.RetryBaseDelay
	}
	if c.RetryJitter < 0 || c.RetryJitter > 1 {
		return Config{}, wrap("retry_jitter must be in [0,1]")
	}
	if c.ShutdownGrace <= 0 {
		c.ShutdownGrace = d.ShutdownGrace
	}
	return c, nil
}

func wrap(msg string) error {
	return &configError{msg: msg}
}

type configError struct{ msg string }

func (e *configError) Error() string { return "queue/config: " + e.msg }

// Is lets callers match config errors against types.ErrConfig.
func (e *configError) Is(target error) bool { return target == types.ErrConfig }
