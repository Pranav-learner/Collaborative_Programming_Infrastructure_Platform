// Package exporters implements the Exporter Framework: the single place where the
// platform's neutral telemetry (log records, span data, metric families) is
// rendered to a concrete destination. Business logic — and even the logging /
// metrics / tracing frameworks — never name a vendor; they hand signals to the
// Manager, which fans them out to every registered Exporter with per-exporter
// isolation, async batching, back-pressure accounting, and self-observability.
//
// Shipped adapters: Console (human-readable), OTLP (OpenTelemetry-shaped JSON —
// the default telemetry representation), Prometheus (text exposition, pull-based),
// and No-op. Datadog / New Relic / CloudWatch / Azure Monitor are future adapters
// that implement the same Exporter interface with zero change to callers.
package exporters

import (
	"context"
	"errors"

	"cpip/internal/observability/logging"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/tracing"
)

// ErrUnsupported is returned by an exporter for a signal it does not handle
// (e.g. Prometheus for logs). The Manager treats it as a benign skip, not a
// failure.
var ErrUnsupported = errors.New("observability/exporters: signal not supported")

// Exporter renders neutral telemetry to a destination. Implementations must be
// safe for concurrent Export* calls. A method returning ErrUnsupported signals
// the exporter does not handle that signal.
type Exporter interface {
	// Name uniquely identifies the exporter ("console","otlp","prometheus","noop").
	Name() string
	// ExportLogs ships a batch of log records.
	ExportLogs(ctx context.Context, records []logging.Record) error
	// ExportSpans ships a batch of completed spans.
	ExportSpans(ctx context.Context, spans []tracing.SpanData) error
	// ExportMetrics ships a snapshot of metric families.
	ExportMetrics(ctx context.Context, families []metrics.Family) error
	// Shutdown flushes and releases resources.
	Shutdown(ctx context.Context) error
}

// Resource carries the identifying attributes stamped on exported telemetry
// (service.name, environment, version, instance) — the OTel "resource".
type Resource struct {
	ServiceName string            `json:"service.name"`
	Environment string            `json:"environment"`
	Version     string            `json:"service.version"`
	InstanceID  string            `json:"service.instance.id"`
	Extra       map[string]string `json:"-"`
}

// Map renders the resource as a flat attribute map.
func (r Resource) Map() map[string]string {
	m := map[string]string{
		"service.name":        r.ServiceName,
		"environment":         r.Environment,
		"service.version":     r.Version,
		"service.instance.id": r.InstanceID,
	}
	for k, v := range r.Extra {
		m[k] = v
	}
	return m
}
