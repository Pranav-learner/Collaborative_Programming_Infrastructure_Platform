package events

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"
)

type Type string

const (
	SandboxCreated    Type = "sandbox_created"
	WorkspacePrepared Type = "workspace_prepared"
	ContainerCreated  Type = "container_created"
	ContainerStarted  Type = "container_started"
	SandboxReady      Type = "sandbox_ready"
	ExecutionAttached Type = "execution_attached"
	ContainerStopped  Type = "container_stopped"
	CleanupStarted    Type = "cleanup_started"
	SandboxDestroyed  Type = "sandbox_destroyed"
	ImagePulled       Type = "image_pulled"
	ImageValidated    Type = "image_validated"

	// Stage 3 Module 2 Security/Resource events
	PolicyResolved         Type = "policy_resolved"
	PolicyValidated        Type = "policy_validated"
	SecurityProfileApplied Type = "security_profile_applied"
	ResourceProfileApplied Type = "resource_profile_applied"
	LimitExceeded          Type = "limit_exceeded"
	ExecutionDenied        Type = "execution_denied"
	FilesystemPrepared     Type = "filesystem_prepared"
	NetworkPrepared        Type = "network_prepared"
	AuditRecorded          Type = "audit_recorded"
	ResourceViolation      Type = "resource_violation"

	// Stage 3 Module 3 Lifecycle/Monitoring events
	SandboxStarted            Type = "sandbox_started"
	SandboxRunning            Type = "sandbox_running"
	SandboxHealthy            Type = "sandbox_healthy"
	SandboxUnhealthy          Type = "sandbox_unhealthy"
	ExecutionCompleted        Type = "execution_completed"
	ExecutionFailed           Type = "execution_failed"
	ExecutionTimedOut         Type = "execution_timed_out"
	CleanupCompleted          Type = "cleanup_completed"
	SandboxRecovered          Type = "sandbox_recovered"
	ResourceThresholdExceeded Type = "resource_threshold_exceeded"
)

// Event describes a structured sandbox lifecycle event.
type Event struct {
	EventID        string        `json:"event_id"`
	CorrelationID  string        `json:"correlation_id"`
	SequenceNumber int64         `json:"sequence_number"`
	Type           Type          `json:"type"`
	SandboxID      string        `json:"sandbox_id"`
	JobID          string        `json:"job_id,omitempty"`
	LifecycleState string        `json:"lifecycle_state,omitempty"`
	Severity       string        `json:"severity,omitempty"` // "Info", "Warning", "Critical"
	Duration       time.Duration `json:"duration,omitempty"`
	Origin         string        `json:"origin,omitempty"`
	Timestamp      time.Time     `json:"timestamp"`
	Payload        any           `json:"payload,omitempty"`
}

// Bus is a thread-safe local event bus for sandbox instances.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[chan Event]struct{}
	seqCounter  int64
}

// NewBus initializes a new sandbox event bus.
func NewBus() *Bus {
	return &Bus{
		subscribers: make(map[chan Event]struct{}),
	}
}

// Subscribe adds a subscriber channel to receive event notifications.
func (b *Bus) Subscribe(bufSize int) chan Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Event, bufSize)
	b.subscribers[ch] = struct{}{}
	return ch
}

// Unsubscribe removes and closes a subscriber channel.
func (b *Bus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.subscribers[ch]; exists {
		delete(b.subscribers, ch)
		close(ch)
	}
}

// Publish broadcasts the event to all active subscribers.
func (b *Bus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}
	if e.EventID == "" {
		e.EventID = generateEventID()
	}
	if e.SequenceNumber == 0 {
		e.SequenceNumber = atomic.AddInt64(&b.seqCounter, 1)
	}

	for ch := range b.subscribers {
		select {
		case ch <- e:
		default:
			// Non-blocking write to avoid hanging if subscriber buffer is full
		}
	}
}

func generateEventID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
