package tracing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/observability/config"
	"cpip/internal/observability/correlation"
)

// Tracer creates spans. It is the front door business services depend on. It
// wires the sampler, per-span limits, the exporter, and the Trace Registry, and
// propagates trace/span ids into both the span context and the correlation IDs on
// the returned context (so logs emitted within the span carry its ids).
type Tracer struct {
	sampler   Sampler
	exporter  SpanExporter
	registry  *Registry
	resource  map[string]string
	maxAttrs  int
	maxEvents int

	onEndHook atomic.Pointer[func(SpanData)]
}

// Params configures a Tracer.
type Params struct {
	Config   config.Tracing
	Sampler  Sampler
	Exporter SpanExporter
	Registry *Registry
	Resource map[string]string
}

// NewTracer constructs a Tracer.
func NewTracer(p Params) *Tracer {
	sampler := p.Sampler
	if sampler == nil {
		sampler = NewParentRatioSampler(p.Config.SampleRatio)
	}
	reg := p.Registry
	if reg == nil {
		reg = NewRegistry()
	}
	maxAttrs := p.Config.MaxAttributes
	if maxAttrs <= 0 {
		maxAttrs = 128
	}
	maxEvents := p.Config.MaxEvents
	if maxEvents <= 0 {
		maxEvents = 128
	}
	return &Tracer{
		sampler:   sampler,
		exporter:  p.Exporter,
		registry:  reg,
		resource:  p.Resource,
		maxAttrs:  maxAttrs,
		maxEvents: maxEvents,
	}
}

// SetExporter installs/replaces the span exporter.
func (t *Tracer) SetExporter(e SpanExporter) { t.exporter = e }

// SetOnEnd installs a hook invoked for every ended span (used by telemetry for
// throughput metrics + TraceFinished events), in addition to the exporter.
func (t *Tracer) SetOnEnd(h func(SpanData)) {
	if h == nil {
		t.onEndHook.Store(nil)
		return
	}
	t.onEndHook.Store(&h)
}

// Registry returns the Trace Registry.
func (t *Tracer) Registry() *Registry { return t.registry }

// StartSpan begins a span as a child of the active span in ctx (or a new root),
// returning a context carrying the new span. The returned context also carries
// updated correlation IDs (TraceID/SpanID) so logs join the trace.
func (t *Tracer) StartSpan(ctx context.Context, name string, opts ...StartOption) (context.Context, Span) {
	cfg := startConfig{kind: KindInternal}
	for _, o := range opts {
		o(&cfg)
	}

	parent := t.parentContext(ctx, cfg.newRoot)
	traceID := parent.TraceID
	if traceID == "" {
		traceID = correlation.NewTraceID()
	}
	spanID := correlation.NewSpanID()
	sampled := t.sampler.ShouldSample(parent, traceID)

	sc := SpanContext{TraceID: traceID, SpanID: spanID, Sampled: sampled}

	// Always propagate ids on the context, even when unsampled, so correlation is
	// preserved end-to-end regardless of the sampling decision.
	ctx = correlation.Update(ctx, correlation.IDs{TraceID: traceID, SpanID: spanID})

	if !sampled {
		ns := noopSpan{sc: sc}
		return ContextWithSpan(ctx, ns), ns
	}

	s := &span{
		tracer:     t,
		name:       name,
		kind:       cfg.kind,
		sc:         sc,
		parent:     parent.SpanID,
		start:      time.Now(),
		attributes: make(map[string]any, len(cfg.attributes)),
		links:      cfg.links,
		status:     StatusUnset,
	}
	for k, v := range cfg.attributes {
		s.attributes[k] = v
	}
	t.registry.start(sc, name)
	return ContextWithSpan(ctx, s), s
}

// parentContext resolves the parent span context from the active span or the
// correlation IDs already in ctx (e.g. extracted from an upstream request).
func (t *Tracer) parentContext(ctx context.Context, newRoot bool) SpanContext {
	if newRoot {
		return SpanContext{}
	}
	if s, ok := ctx.Value(spanCtxKey{}).(*span); ok {
		return s.sc
	}
	ids := correlation.From(ctx)
	if ids.TraceID != "" && ids.SpanID != "" {
		// A remote parent extracted from upstream; assume sampled if propagated.
		return SpanContext{TraceID: ids.TraceID, SpanID: ids.SpanID, Sampled: true, Remote: true}
	}
	return SpanContext{}
}

// onEnd ships a finished span to the exporter and the hook.
func (t *Tracer) onEnd(data SpanData) {
	if t.exporter != nil {
		t.exporter.ExportSpans([]SpanData{data})
	}
	if h := t.onEndHook.Load(); h != nil {
		(*h)(data)
	}
}

// --- Trace Registry ---

// Registry is the Trace Registry: it tracks active spans, the span hierarchy per
// trace, and sampling metadata. It answers "what is in flight right now?" for
// self-observability and future deep diagnostics.
type Registry struct {
	mu     sync.Mutex
	active map[string]activeSpan // spanID → info
	traces map[string]int        // traceID → active span count
	// lifetime counters
	started  atomic.Int64
	finished atomic.Int64
}

type activeSpan struct {
	traceID string
	name    string
	start   time.Time
}

// NewRegistry constructs an empty Trace Registry.
func NewRegistry() *Registry {
	return &Registry{active: make(map[string]activeSpan), traces: make(map[string]int)}
}

func (r *Registry) start(sc SpanContext, name string) {
	r.mu.Lock()
	r.active[sc.SpanID] = activeSpan{traceID: sc.TraceID, name: name, start: time.Now()}
	r.traces[sc.TraceID]++
	r.mu.Unlock()
	r.started.Add(1)
}

func (r *Registry) finish(sc SpanContext) {
	r.mu.Lock()
	if _, ok := r.active[sc.SpanID]; ok {
		delete(r.active, sc.SpanID)
		if r.traces[sc.TraceID]--; r.traces[sc.TraceID] <= 0 {
			delete(r.traces, sc.TraceID)
		}
	}
	r.mu.Unlock()
	r.finished.Add(1)
}

// Stats is a snapshot of the Trace Registry.
type Stats struct {
	ActiveSpans   int   `json:"active_spans"`
	ActiveTraces  int   `json:"active_traces"`
	StartedTotal  int64 `json:"started_total"`
	FinishedTotal int64 `json:"finished_total"`
}

// Stats returns current registry statistics.
func (r *Registry) Stats() Stats {
	r.mu.Lock()
	as, at := len(r.active), len(r.traces)
	r.mu.Unlock()
	return Stats{
		ActiveSpans:   as,
		ActiveTraces:  at,
		StartedTotal:  r.started.Load(),
		FinishedTotal: r.finished.Load(),
	}
}
