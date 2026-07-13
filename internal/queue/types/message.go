// Package types defines the dependency-free domain model for the distributed
// queue and worker infrastructure: the queue Message and its lifecycle state
// machine, the Worker model and its lifecycle state machine, priorities, health,
// and the canonical error set.
//
// This package imports only the standard library (plus encoding/json for the
// wire codec) so every other queue package may depend on it without risking an
// import cycle. It contains data, pure functions, and codecs only — no
// goroutines, no I/O, no locks.
package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// MessageState is a position in the queue-message lifecycle state machine.
//
// The nominal path is:
//
//	Created → Queued → Claimed → Acknowledged → Completed → Archived
//
// with Retry and DeadLetter as the failure branches out of Claimed, Retry
// re-entering at Queued, and Archived as the terminal sink.
type MessageState uint8

const (
	// StateCreated is a message constructed by the producer but not yet published.
	StateCreated MessageState = iota
	// StateQueued is a message published to the Redis stream and awaiting a consumer.
	StateQueued
	// StateClaimed is a message read into a consumer group's pending-entries list.
	StateClaimed
	// StateAcknowledged is a successfully processed message that has been XACK'd.
	StateAcknowledged
	// StateCompleted is a terminally successful message.
	StateCompleted
	// StateRetry is a failed message scheduled for another attempt.
	StateRetry
	// StateDeadLetter is a message moved to the dead-letter queue.
	StateDeadLetter
	// StateArchived is the terminal sink for finished messages.
	StateArchived
)

// String returns the lowercase snake_case name of the state.
func (s MessageState) String() string {
	switch s {
	case StateCreated:
		return "created"
	case StateQueued:
		return "queued"
	case StateClaimed:
		return "claimed"
	case StateAcknowledged:
		return "acknowledged"
	case StateCompleted:
		return "completed"
	case StateRetry:
		return "retry"
	case StateDeadLetter:
		return "dead_letter"
	case StateArchived:
		return "archived"
	default:
		return "unknown"
	}
}

var messageTransitions = map[MessageState]map[MessageState]struct{}{
	StateCreated:      setOfMsg(StateQueued),
	StateQueued:       setOfMsg(StateClaimed, StateDeadLetter),
	StateClaimed:      setOfMsg(StateAcknowledged, StateRetry, StateDeadLetter),
	StateAcknowledged: setOfMsg(StateCompleted, StateRetry, StateDeadLetter),
	StateCompleted:    setOfMsg(StateArchived),
	StateRetry:        setOfMsg(StateQueued, StateDeadLetter),
	StateDeadLetter:   setOfMsg(StateArchived),
	StateArchived:     setOfMsg(),
}

func setOfMsg(states ...MessageState) map[MessageState]struct{} {
	m := make(map[MessageState]struct{}, len(states))
	for _, s := range states {
		m[s] = struct{}{}
	}
	return m
}

// CanTransitionMessage reports whether from → to is a legal message transition.
// Self-transitions are rejected so spurious re-entry surfaces as illegal.
func CanTransitionMessage(from, to MessageState) bool {
	next, ok := messageTransitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

// IsTerminal reports whether the message state admits no further transitions.
func (s MessageState) IsTerminal() bool { return s == StateArchived }

// IsFinished reports whether the message has left the active pipeline.
func (s MessageState) IsFinished() bool {
	switch s {
	case StateCompleted, StateDeadLetter, StateArchived:
		return true
	default:
		return false
	}
}

// Priority orders messages; higher values are more urgent. Priority routing is
// future-ready: the producer may map priorities onto distinct streams.
type Priority int8

const (
	PriorityLow      Priority = 0
	PriorityNormal   Priority = 1
	PriorityHigh     Priority = 2
	PriorityCritical Priority = 3
)

// String returns the lowercase name of the priority.
func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	case PriorityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// TraceContext carries correlation identifiers threaded through the queue.
type TraceContext struct {
	TraceID string `json:"trace_id,omitempty"`
	SpanID  string `json:"span_id,omitempty"`
}

// ExecutionContext is the compact, serializable execution envelope carried with
// a queue message. It is intentionally decoupled from the orchestrator's live
// execution context: only what a worker needs to run the job travels on the wire.
type ExecutionContext struct {
	Deadline  time.Time         `json:"deadline,omitempty"`
	Attempt   int               `json:"attempt"`
	Resources map[string]string `json:"resources,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
}

// Message is the unit enqueued, delivered, and acknowledged by the queue. Values
// are plain data; the registry/PEL hold the authoritative copy and callers work
// with copies via Clone.
type Message struct {
	// MessageID is the queue's own stable identifier, independent of the stream.
	MessageID string `json:"message_id"`
	// StreamID is the Redis stream entry ID assigned on publish (empty until queued).
	StreamID string `json:"stream_id,omitempty"`

	JobID         string `json:"job_id"`
	CorrelationID string `json:"correlation_id"`
	RequestID     string `json:"request_id"`
	UserID        string `json:"user_id"`
	RoomID        string `json:"room_id"`
	Language      string `json:"language"`

	Priority   Priority `json:"priority"`
	RetryCount int      `json:"retry_count"`
	MaxRetries int      `json:"max_retries"`

	EnqueueTime       time.Time     `json:"enqueue_time"`
	ScheduleTime      time.Time     `json:"schedule_time,omitempty"`
	VisibilityTimeout time.Duration `json:"visibility_timeout"`

	ExecutionContext ExecutionContext  `json:"execution_context"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	Trace            TraceContext      `json:"trace"`

	// Version is the message schema version, for forward-compatible evolution.
	Version int `json:"version"`

	// State is the lifecycle position. Not serialized onto the primary stream
	// (Redis owns delivery state); tracked in-process and on the DLQ.
	State MessageState `json:"state"`

	// WorkerID is the assigned worker once dispatched (empty until then).
	WorkerID string `json:"worker_id,omitempty"`

	// Source and consumer bookkeeping (populated on read).
	Stream   string `json:"-"`
	Group    string `json:"-"`
	Consumer string `json:"-"`
}

// Clone returns a deep copy of the message, safe to hand to callers.
func (m Message) Clone() Message {
	cp := m
	if m.Metadata != nil {
		cp.Metadata = make(map[string]string, len(m.Metadata))
		for k, v := range m.Metadata {
			cp.Metadata[k] = v
		}
	}
	if m.ExecutionContext.Resources != nil {
		cp.ExecutionContext.Resources = make(map[string]string, len(m.ExecutionContext.Resources))
		for k, v := range m.ExecutionContext.Resources {
			cp.ExecutionContext.Resources[k] = v
		}
	}
	if m.ExecutionContext.Env != nil {
		cp.ExecutionContext.Env = make(map[string]string, len(m.ExecutionContext.Env))
		for k, v := range m.ExecutionContext.Env {
			cp.ExecutionContext.Env[k] = v
		}
	}
	return cp
}

// CurrentVersion is the wire schema version emitted by this build.
const CurrentVersion = 1

// Field keys used in the Redis stream entry.
const (
	fieldVersion = "v"
	fieldData    = "data"
)

// Marshal serializes a message to the Redis stream field map. The whole message
// travels as JSON under a single field, with the schema version alongside for
// forward-compatible decoding.
func Marshal(m Message) (map[string]string, error) {
	if m.Version == 0 {
		m.Version = CurrentVersion
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return map[string]string{
		fieldVersion: fmt.Sprintf("%d", m.Version),
		fieldData:    string(data),
	}, nil
}

// Unmarshal reconstructs a message from a Redis stream field map. The stream
// entry ID is threaded in so the decoded message carries its StreamID.
func Unmarshal(streamID string, fields map[string]string) (Message, error) {
	raw, ok := fields[fieldData]
	if !ok {
		return Message{}, fmt.Errorf("%w: missing data field", ErrDeserialize)
	}
	var m Message
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrDeserialize, err)
	}
	m.StreamID = streamID
	return m, nil
}
