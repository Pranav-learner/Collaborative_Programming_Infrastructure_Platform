package logger

import (
	"log/slog"
	"os"
)

var defaultLogger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

// Info logs a structured informational message.
func Info(msg string, args ...any) {
	defaultLogger.Info(msg, args...)
}

// Error logs a structured error message.
func Error(msg string, args ...any) {
	defaultLogger.Error(msg, args...)
}

// Warn logs a structured warning message.
func Warn(msg string, args ...any) {
	defaultLogger.Warn(msg, args...)
}
