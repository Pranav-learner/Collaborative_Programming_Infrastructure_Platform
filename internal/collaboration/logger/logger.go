// Package logger provides structured-logging and tracing seams for the
// collaboration engine. It wraps log/slog with a component-scoped factory and
// defines a minimal Tracer interface so distributed tracing can be wired in
// later without the engine depending on any tracing vendor.
package logger

import (
	"context"
	"io"
	"log/slog"
)

// Component tags every log line emitted through a scoped logger, so operators
// can filter collaboration-engine logs from the rest of the process.
const Component = "collaboration"

// Named returns a slog.Logger scoped to the given sub-component (e.g. "sync",
// "snapshot", "recovery"). A nil base falls back to slog.Default.
func Named(base *slog.Logger, sub string) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	return base.With(slog.String("component", Component), slog.String("subsystem", sub))
}

// Discard returns a logger that drops everything; useful in tests.
func Discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Span represents an in-progress traced operation. Implementations must be safe
// to call End on exactly once.
type Span interface {
	// SetAttr attaches a key/value attribute to the span.
	SetAttr(key string, value any)
	// RecordError marks the span as failed with the given error.
	RecordError(err error)
	// End finishes the span.
	End()
}

// Tracer starts spans for collaboration operations. A concrete OpenTelemetry
// adapter is injected in production; NopTracer is used otherwise.
type Tracer interface {
	// StartSpan begins a span named `name`, returning a derived context and the
	// span. Callers must call span.End when the operation completes.
	StartSpan(ctx context.Context, name string) (context.Context, Span)
}

// NopTracer is a Tracer that produces no-op spans.
type NopTracer struct{}

// NewNopTracer constructs a NopTracer.
func NewNopTracer() NopTracer { return NopTracer{} }

// StartSpan implements Tracer.
func (NopTracer) StartSpan(ctx context.Context, _ string) (context.Context, Span) {
	return ctx, nopSpan{}
}

type nopSpan struct{}

func (nopSpan) SetAttr(string, any) {}
func (nopSpan) RecordError(error)   {}
func (nopSpan) End()                {}
