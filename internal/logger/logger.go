// Package logger constructs the structured logger used across the platform.
//
// We standardize on the standard library's log/slog so every package can accept
// a *slog.Logger by dependency injection. There is no package-level global
// logger: the root logger is created once in main and threaded through
// constructors. Per-connection child loggers are derived with With(...) so that
// correlation fields (conn_id, user_id, request_id) ride on every line.
package logger

import (
	"io"
	"log/slog"
	"strings"
)

// New builds a root *slog.Logger writing to w with the given level and format.
// format is "json" or "text"; level is debug|info|warn|error. Unknown values
// fall back to info/json — Config.Validate rejects invalid values before we get
// here, so this is purely defensive.
func New(level, format string, w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}
	var h slog.Handler
	if strings.EqualFold(format, "text") {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h)
}

// Nop returns a logger that discards everything. Intended for tests.
func Nop() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
