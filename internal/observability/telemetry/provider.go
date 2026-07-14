package telemetry

import (
	"context"

	"cpip/internal/observability/alerts"
	"cpip/internal/observability/config"
	"cpip/internal/observability/correlation"
	"cpip/internal/observability/dashboard"
	"cpip/internal/observability/events"
	"cpip/internal/observability/exporters"
	"cpip/internal/observability/health"
	"cpip/internal/observability/logging"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/sdk"
	"cpip/internal/observability/tracing"
)

// --- sdk.Telemetry implementation ---

// Logger returns the root structured logger.
func (p *Provider) Logger() logging.Logger { return p.logging.Logger() }

// EmitLog logs at an explicit level.
func (p *Provider) EmitLog(ctx context.Context, level logging.Level, msg string, fields ...logging.Field) {
	p.logging.Logger().Log(ctx, level, msg, fields...)
}

// Meter returns the metrics front door.
func (p *Provider) Meter() *metrics.Meter { return p.meter }

// RegisterMetric pre-registers a metric definition, emitting MetricRecorded.
func (p *Provider) RegisterMetric(def metrics.Def) error {
	var err error
	switch def.Kind {
	case metrics.KindCounter:
		_, err = p.meter.TryCounter(def)
	case metrics.KindGauge:
		p.meter.Gauge(def)
	case metrics.KindHistogram:
		p.meter.Histogram(def)
	case metrics.KindSummary:
		p.meter.Summary(def)
	default:
		_, err = p.meter.TryCounter(def)
	}
	if err == nil {
		p.bus.Emit(events.MetricRecorded, "metrics", func(e *events.Event) { e.Name = def.Name })
	}
	return err
}

// EmitMetric records one sample against the named metric (convenience path).
func (p *Provider) EmitMetric(_ context.Context, s sdk.MetricSample) {
	def := metrics.Def{Name: s.Name, Kind: s.Kind, Help: s.Help, Labels: labelNames(s.Labels)}
	switch s.Kind {
	case metrics.KindGauge:
		p.meter.Gauge(def).With(s.Labels).Set(s.Value)
	case metrics.KindHistogram:
		p.meter.Histogram(def).With(s.Labels).Observe(s.Value)
	case metrics.KindSummary:
		p.meter.Summary(def).With(s.Labels).Observe(s.Value)
	default: // counter
		def.Kind = metrics.KindCounter
		p.meter.Counter(def).With(s.Labels).Add(s.Value)
	}
}

// Tracer returns the tracer.
func (p *Provider) Tracer() *tracing.Tracer { return p.tracer }

// StartSpan begins a span as a child of the active span in ctx. It also emits
// TraceStarted for root spans.
func (p *Provider) StartSpan(ctx context.Context, name string, opts ...tracing.StartOption) (context.Context, tracing.Span) {
	ctx, span := p.tracer.StartSpan(ctx, name, opts...)
	if span.IsRecording() && span.Context().SpanID != "" {
		p.bus.Emit(events.TraceStarted, "tracing", func(e *events.Event) { e.Name = name })
	}
	return ctx, span
}

// EndSpan ends a span.
func (p *Provider) EndSpan(span tracing.Span) {
	if span != nil {
		span.End()
	}
}

// RecordEvent adds an event to the active span in ctx.
func (p *Provider) RecordEvent(ctx context.Context, name string, attrs map[string]any) {
	tracing.SpanFromContext(ctx).AddEvent(name, attrs)
}

// RecordError records an error on the active span in ctx.
func (p *Provider) RecordError(ctx context.Context, err error, attrs map[string]any) {
	tracing.SpanFromContext(ctx).RecordError(err, attrs)
}

// Health returns the health registry.
func (p *Provider) Health() *health.Registry { return p.health }

// RegisterHealthCheck registers a health check.
func (p *Provider) RegisterHealthCheck(check health.Check, opts health.Options) {
	p.health.Register(check, opts)
}

// Correlate ensures ctx carries a correlation id.
func (p *Provider) Correlate(ctx context.Context) (context.Context, correlation.IDs) {
	ctx, _ = correlation.EnsureCorrelationID(ctx)
	return ctx, correlation.From(ctx)
}

// --- Lifecycle ---

// Start launches all background loops.
func (p *Provider) Start(ctx context.Context) error {
	p.exporters.Start(ctx)
	p.health.Start(ctx)
	p.dashboard.Start(ctx)
	p.alerts.Start(ctx)
	p.internal.Info(ctx, "observability_started", "service", p.cfg.ServiceName, "exporters", p.exporters.Health())
	return nil
}

// Shutdown flushes and releases all resources, in reverse dependency order.
func (p *Provider) Shutdown(ctx context.Context) error {
	p.alerts.Stop()
	p.dashboard.Stop()
	p.health.Stop()
	_ = p.logging.Flush()
	err := p.exporters.Shutdown(ctx) // drains queues + flushes exporters
	_ = p.logging.Close()
	p.bus.Close()
	return err
}

// --- Platform accessors (subsystem seams beyond the SDK) ---

// Events returns the platform event bus (future modules subscribe here).
func (p *Provider) Events() *events.Bus { return p.bus }

// MetricsRegistry returns the metrics registry (for exporters/scrapers).
func (p *Provider) MetricsRegistry() *metrics.Registry { return p.metricReg }

// Exporters returns the exporter manager.
func (p *Provider) Exporters() *exporters.Manager { return p.exporters }

// Dashboard builds and returns the current dashboard snapshot.
func (p *Provider) Dashboard(ctx context.Context) dashboard.Dashboard { return p.dashboard.Build(ctx) }

// DashboardBuilder returns the dashboard builder.
func (p *Provider) DashboardBuilder() *dashboard.Builder { return p.dashboard }

// Alerts returns the alert evaluator.
func (p *Provider) Alerts() *alerts.Evaluator { return p.alerts }

// AddAlertRule registers an alert rule at runtime.
func (p *Provider) AddAlertRule(r alerts.Rule) { p.alerts.AddRule(r) }

// PrometheusText renders the current metrics in Prometheus exposition format
// (empty string if the prometheus exporter is not enabled).
func (p *Provider) PrometheusText() string {
	if p.prom == nil {
		return ""
	}
	return p.prom.Render()
}

// Config returns the validated configuration in effect.
func (p *Provider) Config() config.Config { return p.cfg }

func labelNames(l metrics.Labels) []string {
	if len(l) == 0 {
		return nil
	}
	out := make([]string, 0, len(l))
	for k := range l {
		out = append(out, k)
	}
	return out
}

var _ sdk.Telemetry = (*Provider)(nil)
