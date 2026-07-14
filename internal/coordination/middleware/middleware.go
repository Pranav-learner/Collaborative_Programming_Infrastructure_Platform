// Package middleware provides cross-cutting helpers for the coordination module:
// context propagation of node identity and correlation IDs, and a tracing-hook
// seam. These are small, dependency-light utilities the manager/API layers
// compose; they impose no framework and hold no global state.
package middleware

import "context"

type ctxKey int

const (
	ctxNodeID ctxKey = iota
	ctxRequestID
)

// WithNodeID attaches the acting node's id to ctx (which node issued the call).
func WithNodeID(ctx context.Context, nodeID string) context.Context {
	return context.WithValue(ctx, ctxNodeID, nodeID)
}

// NodeIDFrom returns the acting node id from ctx, or "" if none.
func NodeIDFrom(ctx context.Context) string {
	s, _ := ctx.Value(ctxNodeID).(string)
	return s
}

// WithRequestID attaches a correlation/request id for tracing across nodes.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxRequestID, id)
}

// RequestIDFrom returns the correlation id from ctx, or "" if none.
func RequestIDFrom(ctx context.Context) string {
	s, _ := ctx.Value(ctxRequestID).(string)
	return s
}

// Tracer is the tracing-hook seam. A real implementation bridges to
// OpenTelemetry; the default is a no-op. Start returns a finish function invoked
// (deferred) when the coordination operation completes.
type Tracer interface {
	Start(ctx context.Context, operation string) (context.Context, func(err error))
}

// NoopTracer is a Tracer that does nothing.
type NoopTracer struct{}

// Start implements Tracer.
func (NoopTracer) Start(ctx context.Context, _ string) (context.Context, func(error)) {
	return ctx, func(error) {}
}

var _ Tracer = NoopTracer{}
