// Package logger provides structured logging hooks for the distributed cache &
// state module. Every line carries machine-parseable fields (subsystem,
// operation, key, duration, error) so it is queryable in ELK / Datadog /
// CloudWatch. The logger wraps slog and never panics on a nil delegate.
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

// New creates a cache Logger. If nil is passed it defaults to slog.Default().
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{inner: l.With("module", "cache")}
}

// With returns a child logger with additional persistent fields.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{inner: l.inner.With(args...)}
}

// Slog exposes the underlying slog.Logger for subsystems that want to log
// directly with their own field sets.
func (l *Logger) Slog() *slog.Logger { return l.inner }

// CacheOp logs a completed cache operation.
func (l *Logger) CacheOp(ctx context.Context, cache, op, key string, hit bool, dur time.Duration, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "cache_op_error",
			"cache", cache, "op", op, "key", key,
			"duration_ms", dur.Milliseconds(), "error", err.Error())
		return
	}
	l.inner.DebugContext(ctx, "cache_op",
		"cache", cache, "op", op, "key", key,
		"hit", hit, "duration_ms", dur.Milliseconds())
}

// Invalidation logs a cache invalidation action.
func (l *Logger) Invalidation(ctx context.Context, mode string, count int, detail string) {
	l.inner.InfoContext(ctx, "cache_invalidation",
		"mode", mode, "count", count, "detail", detail)
}

// Session logs a session lifecycle event.
func (l *Logger) Session(ctx context.Context, event, sessionID, userID string, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "session_event",
			"event", event, "session_id", sessionID, "user_id", userID, "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "session_event",
		"event", event, "session_id", sessionID, "user_id", userID)
}

// Lock logs a distributed lock lifecycle event.
func (l *Logger) Lock(ctx context.Context, event, resource, owner string, dur time.Duration, err error) {
	if err != nil {
		l.inner.WarnContext(ctx, "lock_event",
			"event", event, "resource", resource, "owner", owner,
			"duration_ms", dur.Milliseconds(), "error", err.Error())
		return
	}
	l.inner.DebugContext(ctx, "lock_event",
		"event", event, "resource", resource, "owner", owner,
		"duration_ms", dur.Milliseconds())
}

// PubSub logs a pub/sub lifecycle event.
func (l *Logger) PubSub(ctx context.Context, event, channel string, detail any, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "pubsub_event",
			"event", event, "channel", channel, "detail", detail, "error", err.Error())
		return
	}
	l.inner.DebugContext(ctx, "pubsub_event",
		"event", event, "channel", channel, "detail", detail)
}

// Presence logs a presence replication event.
func (l *Logger) Presence(ctx context.Context, event, roomID, userID string, applied bool) {
	l.inner.DebugContext(ctx, "presence_event",
		"event", event, "room_id", roomID, "user_id", userID, "applied", applied)
}

// Redis logs a backend connectivity/health event.
func (l *Logger) Redis(ctx context.Context, event string, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "redis_event", "event", event, "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "redis_event", "event", event)
}
