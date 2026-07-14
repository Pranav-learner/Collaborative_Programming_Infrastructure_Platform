package events

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"
)

// EventType is the type of a runtime event.
type EventType string

const (
	RuntimeRegistered         EventType = "RuntimeRegistered"
	RuntimeLoaded             EventType = "RuntimeLoaded"
	RuntimeSelected           EventType = "RuntimeSelected"
	RuntimeStarted            EventType = "RuntimeStarted"
	RuntimeStopped            EventType = "RuntimeStopped"
	RuntimeHealthChanged      EventType = "RuntimeHealthChanged"
	RuntimeMigrationStarted   EventType = "RuntimeMigrationStarted"
	RuntimeMigrationCompleted EventType = "RuntimeMigrationCompleted"
	RuntimeBenchmarkCompleted EventType = "RuntimeBenchmarkCompleted"
	CompatibilityCheckPassed  EventType = "CompatibilityCheckPassed"
	CompatibilityCheckFailed  EventType = "CompatibilityCheckFailed"
)

// RuntimeEvent carries full telemetry for tracing runtime changes.
type RuntimeEvent struct {
	EventID         string            `json:"event_id"`
	CorrelationID   string            `json:"correlation_id"`
	SequenceNumber  int64             `json:"sequence_number"`
	Timestamp       time.Time         `json:"timestamp"`
	Type            EventType         `json:"type"`
	RuntimeID       string            `json:"runtime_id"`
	RuntimeVersion  string            `json:"runtime_version"`
	HostIdentifier  string            `json:"host_identifier"`
	Duration        time.Duration     `json:"duration"`
	Severity        string            `json:"severity"` // "Info", "Warning", "Critical"
	Origin          string            `json:"origin"`   // Component/Package emitting
	Metadata        map[string]string `json:"metadata,omitempty"`
}

var sequenceCounter int64

// NewRuntimeEvent generates an event with pre-populated unique fields.
func NewRuntimeEvent(eventType EventType, runtimeID string, runtimeVersion string, severity string, origin string) RuntimeEvent {
	seq := atomic.AddInt64(&sequenceCounter, 1)

	b := make([]byte, 16)
	_, _ = rand.Read(b)
	eventID := hex.EncodeToString(b)

	return RuntimeEvent{
		EventID:        eventID,
		SequenceNumber: seq,
		Timestamp:      time.Now(),
		Type:           eventType,
		RuntimeID:      runtimeID,
		RuntimeVersion: runtimeVersion,
		HostIdentifier: "localhost", // Can be dynamic
		Severity:       severity,
		Origin:         origin,
		Metadata:       make(map[string]string),
	}
}
