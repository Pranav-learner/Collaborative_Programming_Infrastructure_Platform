// Package logger provides structured logging hooks for the coordination module.
// Every line carries machine-parseable fields (subsystem, node, resource,
// duration, error) so it is queryable in ELK / Datadog / CloudWatch. The logger
// wraps slog and never panics on a nil delegate.
package logger

import (
	"context"
	"log/slog"
	"time"
)

// Logger is the module's structured logger.
type Logger struct {
	inner *slog.Logger
}

// New creates a coordination Logger. If nil is passed it defaults to slog.Default().
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{inner: l.With("module", "coordination")}
}

// With returns a child logger with additional persistent fields.
func (l *Logger) With(args ...any) *Logger {
	if l == nil {
		return New(nil)
	}
	return &Logger{inner: l.inner.With(args...)}
}

// Slog exposes the underlying slog.Logger.
func (l *Logger) Slog() *slog.Logger { return l.inner }

// Membership logs a membership lifecycle event.
func (l *Logger) Membership(ctx context.Context, event, nodeID string, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "membership_event", "event", event, "node_id", nodeID, "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "membership_event", "event", event, "node_id", nodeID)
}

// Leader logs a leadership lifecycle event.
func (l *Logger) Leader(ctx context.Context, event, scope, leaderID string, err error) {
	if err != nil {
		l.inner.WarnContext(ctx, "leader_event", "event", event, "scope", scope, "leader_id", leaderID, "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "leader_event", "event", event, "scope", scope, "leader_id", leaderID)
}

// Lock logs a distributed lock lifecycle event.
func (l *Logger) Lock(ctx context.Context, event, resource, owner string, dur time.Duration, err error) {
	if err != nil {
		l.inner.WarnContext(ctx, "lock_event", "event", event, "resource", resource, "owner", owner, "duration_ms", dur.Milliseconds(), "error", err.Error())
		return
	}
	l.inner.DebugContext(ctx, "lock_event", "event", event, "resource", resource, "owner", owner, "duration_ms", dur.Milliseconds())
}

// Heartbeat logs a heartbeat lifecycle event.
func (l *Logger) Heartbeat(ctx context.Context, event, nodeID string, err error) {
	if err != nil {
		l.inner.WarnContext(ctx, "heartbeat_event", "event", event, "node_id", nodeID, "error", err.Error())
		return
	}
	l.inner.DebugContext(ctx, "heartbeat_event", "event", event, "node_id", nodeID)
}

// Discovery logs a service discovery query result.
func (l *Logger) Discovery(ctx context.Context, query string, matched int, dur time.Duration, err error) {
	if err != nil {
		l.inner.WarnContext(ctx, "discovery_query", "query", query, "matched", matched, "duration_ms", dur.Milliseconds(), "error", err.Error())
		return
	}
	l.inner.DebugContext(ctx, "discovery_query", "query", query, "matched", matched, "duration_ms", dur.Milliseconds())
}

// Replication logs a state replication event.
func (l *Logger) Replication(ctx context.Context, event, domain string, applied bool, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "replication_event", "event", event, "domain", domain, "error", err.Error())
		return
	}
	l.inner.DebugContext(ctx, "replication_event", "event", event, "domain", domain, "applied", applied)
}

// Backend logs a backend connectivity/health event.
func (l *Logger) Backend(ctx context.Context, event string, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "backend_event", "event", event, "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "backend_event", "event", event)
}
