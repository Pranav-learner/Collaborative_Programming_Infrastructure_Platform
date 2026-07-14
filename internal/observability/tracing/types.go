// Package tracing implements the Distributed Tracing Framework: traces, spans,
// nested spans via context propagation, span events, attributes, links, error
// recording, and timing. It is modeled on the OpenTelemetry data model (so an
// OTLP exporter is a faithful mapping) but defines its own vendor-neutral types,
// so business code depends only on the Span/Tracer interfaces and never on an
// OTel SDK.
//
// Spans form a tree through the request context: StartSpan reads the current span
// from ctx as the parent and returns a child plus a new ctx carrying it. Ended
// spans are handed to a pluggable SpanExporter (the exporter framework batches
// and ships them). A head-based Sampler decides per trace whether spans are
// recorded, keeping overhead bounded under load.
package tracing

import (
	"time"
)

// Status is a span's completion status.
type Status string

const (
	StatusUnset Status = "unset"
	StatusOK    Status = "ok"
	StatusError Status = "error"
)

// Kind classifies a span's role (mirrors OTel SpanKind).
type Kind string

const (
	KindInternal Kind = "internal"
	KindServer   Kind = "server"
	KindClient   Kind = "client"
	KindProducer Kind = "producer"
	KindConsumer Kind = "consumer"
)

// SpanContext is the immutable, propagatable identity of a span.
type SpanContext struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
	Sampled bool   `json:"sampled"`
	// Remote indicates the context was extracted from an upstream process.
	Remote bool `json:"remote,omitempty"`
}

// IsValid reports whether the context carries a trace and span id.
func (sc SpanContext) IsValid() bool { return sc.TraceID != "" && sc.SpanID != "" }

// Event is a timestamped annotation on a span.
type Event struct {
	Name       string         `json:"name"`
	Time       time.Time      `json:"time"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// Link references another span (e.g. a batch's originating requests).
type Link struct {
	Context    SpanContext    `json:"context"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// SpanData is the exported, read-only form of a completed span — the neutral unit
// a SpanExporter serializes.
type SpanData struct {
	Name          string         `json:"name"`
	Kind          Kind           `json:"kind"`
	Context       SpanContext    `json:"context"`
	ParentSpanID  string         `json:"parent_span_id,omitempty"`
	StartTime     time.Time      `json:"start_time"`
	EndTime       time.Time      `json:"end_time"`
	Attributes    map[string]any `json:"attributes,omitempty"`
	Events        []Event        `json:"events,omitempty"`
	Links         []Link         `json:"links,omitempty"`
	Status        Status         `json:"status"`
	StatusMessage string         `json:"status_message,omitempty"`
	// DroppedAttributes/Events count what was elided by the per-span caps.
	DroppedAttributes int `json:"dropped_attributes,omitempty"`
	DroppedEvents     int `json:"dropped_events,omitempty"`
	// Resource identifies the emitting service (service.name, environment, …).
	Resource map[string]string `json:"resource,omitempty"`
}

// Duration returns the span's wall-clock duration.
func (s SpanData) Duration() time.Duration { return s.EndTime.Sub(s.StartTime) }

// SpanExporter is the seam a completed span is shipped through. Implementations
// (the exporter framework) must be safe for concurrent Export calls.
type SpanExporter interface {
	ExportSpans(spans []SpanData)
}

// SpanExporterFunc adapts a function to a SpanExporter.
type SpanExporterFunc func([]SpanData)

// ExportSpans implements SpanExporter.
func (f SpanExporterFunc) ExportSpans(spans []SpanData) { f(spans) }

// Sampler decides, at span start, whether a span is recorded and exported. A
// head-based sampler makes the decision once per trace (propagated via the
// parent's Sampled flag).
type Sampler interface {
	// ShouldSample returns the sampling decision for a new span given its parent
	// context (zero value if root) and the new trace id.
	ShouldSample(parent SpanContext, traceID string) bool
	Description() string
}

// AlwaysOn samples every trace.
type AlwaysOn struct{}

func (AlwaysOn) ShouldSample(SpanContext, string) bool { return true }
func (AlwaysOn) Description() string                   { return "AlwaysOn" }

// AlwaysOff samples no trace.
type AlwaysOff struct{}

func (AlwaysOff) ShouldSample(SpanContext, string) bool { return false }
func (AlwaysOff) Description() string                   { return "AlwaysOff" }

// ParentRatioSampler honors a valid parent's decision; for root spans it samples
// a deterministic fraction of trace ids (so the same trace id always decides the
// same way — stable across processes). ratio is clamped to [0,1].
type ParentRatioSampler struct {
	ratio     float64
	threshold uint64
}

// NewParentRatioSampler constructs a ParentRatioSampler.
func NewParentRatioSampler(ratio float64) *ParentRatioSampler {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	return &ParentRatioSampler{ratio: ratio, threshold: uint64(ratio * float64(^uint64(0)))}
}

// ShouldSample honors the parent decision, else samples by trace-id hash.
func (s *ParentRatioSampler) ShouldSample(parent SpanContext, traceID string) bool {
	if parent.IsValid() {
		return parent.Sampled
	}
	if s.ratio <= 0 {
		return false
	}
	if s.ratio >= 1 {
		return true
	}
	return traceIDHash(traceID) < s.threshold
}

// Description names the sampler.
func (s *ParentRatioSampler) Description() string { return "ParentRatioSampler" }

// traceIDHash derives a uniform 64-bit value from the last 16 hex chars of a
// trace id (FNV-1a), so sampling is deterministic per trace.
func traceIDHash(traceID string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(traceID); i++ {
		h ^= uint64(traceID[i])
		h *= prime64
	}
	return h
}
