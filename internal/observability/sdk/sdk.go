// Package sdk defines the Telemetry SDK: the single, vendor-neutral abstraction
// every CPIP service uses for logging, metrics, tracing, and health. It is THE
// seam of the observability platform — business logic depends only on the
// Telemetry interface (and the neutral logging/metrics/tracing/health types it
// references), never on OpenTelemetry, Prometheus, or any exporter.
//
// The concrete implementation lives in the telemetry package (the composition
// root); this package is a pure interface + option surface so it can be imported
// anywhere without pulling in the wiring.
package sdk

import (
	"context"

	"cpip/internal/observability/correlation"
	"cpip/internal/observability/health"
	"cpip/internal/observability/logging"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/tracing"
)

// MetricSample is a one-shot metric record for the convenience EmitMetric path.
// For hot loops, hold an instrument from Meter() instead.
type MetricSample struct {
	Name   string
	Kind   metrics.Kind
	Value  float64
	Labels metrics.Labels
	Help   string
}

// Telemetry is the unified observability facade. All methods are safe for
// concurrent use and never panic on misuse (a bad metric kind yields a no-op, an
// unsampled span yields a no-op span).
type Telemetry interface {
	// --- Logging ---
	// Logger returns the root structured logger.
	Logger() logging.Logger
	// EmitLog logs a message at level with fields (correlation IDs are pulled from ctx).
	EmitLog(ctx context.Context, level logging.Level, msg string, fields ...logging.Field)

	// --- Metrics ---
	// Meter returns the metrics front door for creating instruments.
	Meter() *metrics.Meter
	// RegisterMetric pre-registers a metric definition (idempotent).
	RegisterMetric(def metrics.Def) error
	// EmitMetric records a single sample against the named metric (convenience).
	EmitMetric(ctx context.Context, sample MetricSample)

	// --- Tracing ---
	// Tracer returns the tracer for advanced span control.
	Tracer() *tracing.Tracer
	// StartSpan begins a span as a child of the active span in ctx.
	StartSpan(ctx context.Context, name string, opts ...tracing.StartOption) (context.Context, tracing.Span)
	// EndSpan ends a span (convenience; equivalent to span.End()).
	EndSpan(span tracing.Span)
	// RecordEvent adds an event to the active span in ctx.
	RecordEvent(ctx context.Context, name string, attrs map[string]any)
	// RecordError records an error on the active span in ctx.
	RecordError(ctx context.Context, err error, attrs map[string]any)

	// --- Health ---
	// Health returns the health registry.
	Health() *health.Registry
	// RegisterHealthCheck registers a health check.
	RegisterHealthCheck(check health.Check, opts health.Options)

	// --- Correlation ---
	// Correlate ensures ctx carries a correlation id, returning it.
	Correlate(ctx context.Context) (context.Context, correlation.IDs)

	// --- Lifecycle ---
	// Start launches background loops (exporters, health, dashboard, alerts).
	Start(ctx context.Context) error
	// Shutdown flushes and releases all resources.
	Shutdown(ctx context.Context) error
}
