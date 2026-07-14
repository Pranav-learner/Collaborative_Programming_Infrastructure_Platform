// Package logger provides structured logging hooks for the persistence layer.
// All log lines include structured fields (entity, operation, duration, error)
// so they are machine-parseable by ELK / Datadog / CloudWatch.
package logger

import (
	"context"
	"log/slog"
	"time"
)

// Logger is the persistence layer's structured logger.
type Logger struct {
	inner *slog.Logger
}

// New creates a persistence Logger. If nil is passed it defaults to slog.Default().
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{inner: l.With("component", "persistence")}
}

// QueryStart logs the beginning of a database query.
func (l *Logger) QueryStart(ctx context.Context, operation, entity string) {
	l.inner.InfoContext(ctx, "query_start",
		"operation", operation,
		"entity", entity,
	)
}

// QueryEnd logs the completion of a database query, including duration and error.
func (l *Logger) QueryEnd(ctx context.Context, operation, entity string, duration time.Duration, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "query_error",
			"operation", operation,
			"entity", entity,
			"duration_ms", duration.Milliseconds(),
			"error", err.Error(),
		)
		return
	}
	l.inner.InfoContext(ctx, "query_end",
		"operation", operation,
		"entity", entity,
		"duration_ms", duration.Milliseconds(),
	)
}

// TransactionEvent logs a transaction lifecycle event.
func (l *Logger) TransactionEvent(ctx context.Context, event string, duration time.Duration, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "transaction_event",
			"event", event,
			"duration_ms", duration.Milliseconds(),
			"error", err.Error(),
		)
		return
	}
	l.inner.InfoContext(ctx, "transaction_event",
		"event", event,
		"duration_ms", duration.Milliseconds(),
	)
}

// MigrationEvent logs a migration lifecycle event.
func (l *Logger) MigrationEvent(ctx context.Context, event string, version int64, name string, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "migration_event",
			"event", event,
			"version", version,
			"name", name,
			"error", err.Error(),
		)
		return
	}
	l.inner.InfoContext(ctx, "migration_event",
		"event", event,
		"version", version,
		"name", name,
	)
}
