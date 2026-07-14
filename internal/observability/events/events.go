// Package events is the observability platform's internal event bus. Every
// subsystem publishes typed lifecycle events here; future modules (config,
// deployment, reliability) and in-process consumers subscribe without any
// coupling to the emitting subsystem.
//
// The bus is in-process and best-effort: it fans out to synchronous handlers and
// buffered subscriber channels, dropping for slow subscribers so a stalled
// consumer can never block a metric record or a log emit. It is deliberately
// separate from the telemetry signals themselves — this is control-plane
// notification, not a data-plane pipeline.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Type classifies an observability platform event.
type Type string

const (
	LogEmitted         Type = "log_emitted"
	MetricRecorded     Type = "metric_recorded"
	TraceStarted       Type = "trace_started"
	TraceFinished      Type = "trace_finished"
	HealthChanged      Type = "health_changed"
	AlertTriggered     Type = "alert_triggered"
	AlertResolved      Type = "alert_resolved"
	ExporterRegistered Type = "exporter_registered"
	ExporterFailed     Type = "exporter_failed"
	DashboardUpdated   Type = "dashboard_updated"
	EventsDropped      Type = "events_dropped"
)

// Event carries structured details for observability and cross-subsystem
// coordination.
type Event struct {
	EventID   string    `json:"event_id"`
	Type      Type      `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	// Subsystem names the emitter (e.g. "logging", "metrics", "exporters").
	Subsystem string `json:"subsystem"`
	// Name identifies the affected entity (metric name, exporter name, check name).
	Name string `json:"name,omitempty"`
	// Payload holds event-specific detail.
	Payload any `json:"payload,omitempty"`
}

// New creates a populated Event with a random ID and current timestamp.
func New(t Type, subsystem string) Event {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return Event{
		EventID:   hex.EncodeToString(b),
		Type:      t,
		Timestamp: time.Now().UTC(),
		Subsystem: subsystem,
	}
}

// Handler is a synchronous callback invoked on every Publish.
type Handler func(Event)

// Bus is a thread-safe best-effort event broker.
type Bus struct {
	mu          sync.RWMutex
	handlers    []Handler
	subscribers []chan Event
	closed      bool
}

// NewBus creates an empty event bus.
func NewBus() *Bus { return &Bus{} }

// Subscribe registers a buffered channel that receives all future events until
// the bus is closed. A slow subscriber loses events rather than blocking others.
func (b *Bus) Subscribe(bufSize int) <-chan Event {
	if b == nil {
		ch := make(chan Event)
		close(ch)
		return ch
	}
	if bufSize <= 0 {
		bufSize = 256
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Event, bufSize)
	b.subscribers = append(b.subscribers, ch)
	return ch
}

// OnEvent registers a synchronous handler that fires on every Publish. Handlers
// must not block; heavy work should be dispatched to a goroutine by the handler.
func (b *Bus) OnEvent(h Handler) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, h)
}

// OnType registers a handler that fires only for a specific event type.
func (b *Bus) OnType(t Type, h Handler) {
	b.OnEvent(func(e Event) {
		if e.Type == t {
			h(e)
		}
	})
}

// Publish emits an event to all handlers and subscribers.
func (b *Bus) Publish(e Event) {
	if b == nil {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	for _, h := range b.handlers {
		h(e)
	}
	for _, ch := range b.subscribers {
		select {
		case ch <- e:
		default:
		}
	}
}

// Emit builds and publishes an event in one call, applying mutate to populate
// context fields.
func (b *Bus) Emit(t Type, subsystem string, mutate func(*Event)) {
	if b == nil {
		return
	}
	e := New(t, subsystem)
	if mutate != nil {
		mutate(&e)
	}
	b.Publish(e)
}

// Close drains and closes all subscriber channels. Idempotent.
func (b *Bus) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, ch := range b.subscribers {
		close(ch)
	}
	b.subscribers = nil
	b.handlers = nil
}
