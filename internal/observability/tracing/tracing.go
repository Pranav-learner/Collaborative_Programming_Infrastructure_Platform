package tracing

import (
	"context"
	"sync"
	"time"
)

// Span is the interface business code manipulates. All methods are safe to call
// concurrently and are no-ops after End (double-End is safe).
type Span interface {
	// Context returns the span's immutable identity.
	Context() SpanContext
	// SetName updates the span name.
	SetName(name string)
	// SetAttribute records a single attribute (respecting the per-span cap).
	SetAttribute(key string, value any)
	// SetAttributes records several attributes.
	SetAttributes(attrs map[string]any)
	// AddEvent records a timestamped event.
	AddEvent(name string, attrs map[string]any)
	// AddLink records a link to another span.
	AddLink(link Link)
	// RecordError records an error as an event and (by default) sets Error status.
	RecordError(err error, attrs map[string]any)
	// SetStatus sets the completion status.
	SetStatus(status Status, message string)
	// End finalizes the span and hands it to the exporter (once).
	End()
	// IsRecording reports whether the span records data (false for unsampled spans).
	IsRecording() bool
	// TraceID / SpanID are convenience accessors.
	TraceID() string
	SpanID() string
}

// StartOption customizes a span at creation.
type StartOption func(*startConfig)

type startConfig struct {
	kind       Kind
	attributes map[string]any
	links      []Link
	newRoot    bool
}

// WithKind sets the span kind.
func WithKind(k Kind) StartOption { return func(c *startConfig) { c.kind = k } }

// WithAttributes sets initial attributes.
func WithAttributes(attrs map[string]any) StartOption {
	return func(c *startConfig) { c.attributes = attrs }
}

// WithLinks sets initial links.
func WithLinks(links ...Link) StartOption { return func(c *startConfig) { c.links = links } }

// WithNewRoot forces a new trace even if a parent is present in context.
func WithNewRoot() StartOption { return func(c *startConfig) { c.newRoot = true } }

type spanCtxKey struct{}

// SpanFromContext returns the active span in ctx, or a no-op span.
func SpanFromContext(ctx context.Context) Span {
	if s, ok := ctx.Value(spanCtxKey{}).(*span); ok {
		return s
	}
	return noopSpan{}
}

// ContextWithSpan returns a context carrying s as the active span.
func ContextWithSpan(ctx context.Context, s Span) context.Context {
	return context.WithValue(ctx, spanCtxKey{}, s)
}

// span is the concrete recording span.
type span struct {
	tracer *Tracer
	name   string
	kind   Kind
	sc     SpanContext
	parent string
	start  time.Time

	mu            sync.Mutex
	attributes    map[string]any
	events        []Event
	links         []Link
	status        Status
	statusMsg     string
	droppedAttrs  int
	droppedEvents int
	ended         bool
}

func (s *span) Context() SpanContext { return s.sc }
func (s *span) TraceID() string      { return s.sc.TraceID }
func (s *span) SpanID() string       { return s.sc.SpanID }
func (s *span) IsRecording() bool    { return s.sc.Sampled }

func (s *span) SetName(name string) {
	s.mu.Lock()
	s.name = name
	s.mu.Unlock()
}

func (s *span) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	if _, exists := s.attributes[key]; !exists && len(s.attributes) >= s.tracer.maxAttrs {
		s.droppedAttrs++
		return
	}
	s.attributes[key] = value
}

func (s *span) SetAttributes(attrs map[string]any) {
	for k, v := range attrs {
		s.SetAttribute(k, v)
	}
}

func (s *span) AddEvent(name string, attrs map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	if len(s.events) >= s.tracer.maxEvents {
		s.droppedEvents++
		return
	}
	s.events = append(s.events, Event{Name: name, Time: time.Now(), Attributes: attrs})
}

func (s *span) AddLink(link Link) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.links = append(s.links, link)
}

func (s *span) RecordError(err error, attrs map[string]any) {
	if err == nil {
		return
	}
	ev := map[string]any{"error.message": err.Error()}
	for k, v := range attrs {
		ev[k] = v
	}
	s.AddEvent("exception", ev)
	s.SetStatus(StatusError, err.Error())
}

func (s *span) SetStatus(status Status, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.status = status
	s.statusMsg = message
}

func (s *span) End() {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	end := time.Now()
	data := SpanData{
		Name:              s.name,
		Kind:              s.kind,
		Context:           s.sc,
		ParentSpanID:      s.parent,
		StartTime:         s.start,
		EndTime:           end,
		Attributes:        copyAttrs(s.attributes),
		Events:            append([]Event(nil), s.events...),
		Links:             append([]Link(nil), s.links...),
		Status:            s.status,
		StatusMessage:     s.statusMsg,
		DroppedAttributes: s.droppedAttrs,
		DroppedEvents:     s.droppedEvents,
		Resource:          s.tracer.resource,
	}
	s.mu.Unlock()

	s.tracer.registry.finish(s.sc)
	s.tracer.onEnd(data)
}

func copyAttrs(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// noopSpan is returned for unsampled or absent spans; every method is a no-op but
// it still carries a valid context so IDs propagate.
type noopSpan struct{ sc SpanContext }

func (n noopSpan) Context() SpanContext              { return n.sc }
func (n noopSpan) SetName(string)                    {}
func (n noopSpan) SetAttribute(string, any)          {}
func (n noopSpan) SetAttributes(map[string]any)      {}
func (n noopSpan) AddEvent(string, map[string]any)   {}
func (n noopSpan) AddLink(Link)                      {}
func (n noopSpan) RecordError(error, map[string]any) {}
func (n noopSpan) SetStatus(Status, string)          {}
func (n noopSpan) End()                              {}
func (n noopSpan) IsRecording() bool                 { return false }
func (n noopSpan) TraceID() string                   { return n.sc.TraceID }
func (n noopSpan) SpanID() string                    { return n.sc.SpanID }
