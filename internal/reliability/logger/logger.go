package logger

import "log/slog"

// Logger wraps a structured slog.Logger.
type Logger struct {
	log *slog.Logger
}

// New constructs a logger from an existing slog instance.
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}
	return &Logger{log: l}
}

// Info logs info.
func (l *Logger) Info(msg string, args ...any) {
	l.log.Info(msg, args...)
}

// Warn logs a warning.
func (l *Logger) Warn(msg string, args ...any) {
	l.log.Warn(msg, args...)
}

// Error logs an error.
func (l *Logger) Error(msg string, args ...any) {
	l.log.Error(msg, args...)
}

// Debug logs debug traces.
func (l *Logger) Debug(msg string, args ...any) {
	l.log.Debug(msg, args...)
}

// With creates a sub-logger.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{log: l.log.With(args...)}
}
