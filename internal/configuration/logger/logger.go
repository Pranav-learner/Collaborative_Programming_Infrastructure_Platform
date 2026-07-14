// Package logger provides structured logging for the configuration platform.
package logger

import (
	"log/slog"
)

// Logger wraps slog with configuration-platform-specific context.
type Logger struct {
	inner *slog.Logger
}

// New creates a configuration Logger.
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{inner: l.With("component", "configuration")}
}

func (l *Logger) Info(msg string, args ...any)  { l.inner.Info(msg, args...) }
func (l *Logger) Error(msg string, args ...any) { l.inner.Error(msg, args...) }
func (l *Logger) Warn(msg string, args ...any)  { l.inner.Warn(msg, args...) }
func (l *Logger) Debug(msg string, args ...any) { l.inner.Debug(msg, args...) }

// SecretAccess logs a masked secret access event.
func (l *Logger) SecretAccess(key, provider string) {
	l.inner.Info("secret_accessed",
		"key", key,
		"provider", provider,
		"value", "••••••••",
	)
}
