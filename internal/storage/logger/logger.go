// Package logger provides structured logging hooks for the object storage &
// artifact management module. Every line carries machine-parseable fields
// (subsystem, operation, artifact, bucket, key, duration, error) so it is
// queryable in ELK / Datadog / CloudWatch. The logger wraps slog and never
// panics on a nil delegate.
//
// It is a dependency-free observability leaf: every storage subsystem accepts a
// *Logger via dependency injection; there is no global logger state.
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

// New creates a storage Logger. If nil is passed it defaults to slog.Default().
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{inner: l.With("module", "storage")}
}

// With returns a child logger with additional persistent fields.
func (l *Logger) With(args ...any) *Logger {
	if l == nil {
		return New(nil)
	}
	return &Logger{inner: l.inner.With(args...)}
}

// Slog exposes the underlying slog.Logger for subsystems that want to log
// directly with their own field sets.
func (l *Logger) Slog() *slog.Logger { return l.inner }

// Upload logs a completed upload-pipeline stage or the terminal outcome.
func (l *Logger) Upload(ctx context.Context, artifactID, bucket, key string, size int64, dur time.Duration, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "artifact_upload_failed",
			"artifact_id", artifactID, "bucket", bucket, "key", key,
			"size", size, "duration_ms", dur.Milliseconds(), "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "artifact_uploaded",
		"artifact_id", artifactID, "bucket", bucket, "key", key,
		"size", size, "duration_ms", dur.Milliseconds())
}

// Download logs a completed download.
func (l *Logger) Download(ctx context.Context, artifactID, bucket, key string, size int64, dur time.Duration, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "artifact_download_failed",
			"artifact_id", artifactID, "bucket", bucket, "key", key,
			"duration_ms", dur.Milliseconds(), "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "artifact_downloaded",
		"artifact_id", artifactID, "bucket", bucket, "key", key,
		"size", size, "duration_ms", dur.Milliseconds())
}

// Lifecycle logs an artifact lifecycle transition.
func (l *Logger) Lifecycle(ctx context.Context, artifactID, from, to string, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "artifact_lifecycle_error",
			"artifact_id", artifactID, "from", from, "to", to, "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "artifact_lifecycle",
		"artifact_id", artifactID, "from", from, "to", to)
}

// Version logs a version-management event.
func (l *Logger) Version(ctx context.Context, lineageID, artifactID string, version int64, event string) {
	l.inner.InfoContext(ctx, "artifact_version",
		"lineage_id", lineageID, "artifact_id", artifactID, "version", version, "event", event)
}

// Integrity logs the outcome of an integrity (content-hash) validation.
func (l *Logger) Integrity(ctx context.Context, artifactID, expected, actual string, ok bool) {
	if ok {
		l.inner.DebugContext(ctx, "artifact_integrity_ok",
			"artifact_id", artifactID, "hash", expected)
		return
	}
	l.inner.ErrorContext(ctx, "artifact_integrity_mismatch",
		"artifact_id", artifactID, "expected", expected, "actual", actual)
}

// Compression logs the result of a compression decision.
func (l *Logger) Compression(ctx context.Context, artifactID, algorithm string, original, compressed int64, applied bool) {
	l.inner.DebugContext(ctx, "artifact_compression",
		"artifact_id", artifactID, "algorithm", algorithm,
		"original", original, "compressed", compressed, "applied", applied)
}

// Retention logs a retention decision or action.
func (l *Logger) Retention(ctx context.Context, artifactID, mode, action string, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "artifact_retention_error",
			"artifact_id", artifactID, "mode", mode, "action", action, "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "artifact_retention",
		"artifact_id", artifactID, "mode", mode, "action", action)
}

// Cleanup logs a cleanup/reaper cycle summary.
func (l *Logger) Cleanup(ctx context.Context, event string, scanned, deleted, failed int, dur time.Duration) {
	l.inner.InfoContext(ctx, "storage_cleanup",
		"event", event, "scanned", scanned, "deleted", deleted, "failed", failed,
		"duration_ms", dur.Milliseconds())
}

// Backend logs a backend connectivity / health event.
func (l *Logger) Backend(ctx context.Context, provider, event string, err error) {
	if err != nil {
		l.inner.ErrorContext(ctx, "storage_backend_event",
			"provider", provider, "event", event, "error", err.Error())
		return
	}
	l.inner.InfoContext(ctx, "storage_backend_event",
		"provider", provider, "event", event)
}
