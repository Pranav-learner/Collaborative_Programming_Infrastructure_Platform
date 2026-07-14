// Package logger is the observability platform's OWN internal diagnostic logger —
// distinct from the public logging framework the platform provides to business
// services. It exists to break a bootstrap/recursion problem: the exporter
// framework, health runner, and alert evaluator must be able to report their own
// failures (an exporter that cannot flush, a health check that panicked) without
// feeding those diagnostics back through the very logging pipeline that may be
// the thing failing.
//
// It is a thin, dependency-free wrapper over slog that never panics on a nil
// delegate. "The observability platform must also observe itself" — this is the
// self-observation channel.
package logger

import (
	"context"
	"log/slog"
)

// Logger is the platform's internal logger.
type Logger struct {
	inner *slog.Logger
}

// New creates an internal Logger. If nil is passed it defaults to slog.Default().
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{inner: l.With("module", "observability")}
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

// Debugf-style helpers kept minimal and structured.

// Info logs an internal informational event.
func (l *Logger) Info(ctx context.Context, msg string, args ...any) {
	l.inner.InfoContext(ctx, msg, args...)
}

// Warn logs an internal warning.
func (l *Logger) Warn(ctx context.Context, msg string, args ...any) {
	l.inner.WarnContext(ctx, msg, args...)
}

// Error logs an internal error (e.g. an exporter that failed to flush).
func (l *Logger) Error(ctx context.Context, msg string, err error, args ...any) {
	if err != nil {
		args = append(args, "error", err.Error())
	}
	l.inner.ErrorContext(ctx, msg, args...)
}

// ExporterError logs a failure inside an exporter — a first-class self-observation.
func (l *Logger) ExporterError(ctx context.Context, exporter, op string, err error) {
	l.inner.ErrorContext(ctx, "exporter_error", "exporter", exporter, "op", op, "error", errString(err))
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
