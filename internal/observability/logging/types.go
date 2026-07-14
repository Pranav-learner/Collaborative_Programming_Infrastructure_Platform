// Package logging implements the Logging Framework: structured, level-based,
// context-aware logging that automatically enriches every record with the
// correlation identifiers in context and fans it out to pluggable sinks (the
// built-in stdout sink plus any exporter). It is the platform's log data-plane;
// business services log through the Logger interface and never touch a sink or an
// encoder directly.
//
// The framework is decoupled from any destination: a Sink is the seam an exporter
// implements, so the same log record can reach stdout, an OTLP collector, and a
// future Datadog agent without the caller knowing.
package logging

import (
	"strings"
	"time"

	"cpip/internal/observability/correlation"
)

// Level is a log severity. Ordered so Level comparisons gate emission.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

// String returns the lowercase level name.
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	case LevelFatal:
		return "fatal"
	default:
		return "info"
	}
}

// ParseLevel maps a name to a Level (defaulting to info on unknown input).
func ParseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "fatal":
		return LevelFatal
	default:
		return LevelInfo
	}
}

// Field is a single structured key/value attached to a log record.
type Field struct {
	Key   string
	Value any
}

// Field constructors — typed for ergonomics and to keep call sites readable.

func String(k, v string) Field                 { return Field{k, v} }
func Int(k string, v int) Field                { return Field{k, v} }
func Int64(k string, v int64) Field            { return Field{k, v} }
func Float64(k string, v float64) Field        { return Field{k, v} }
func Bool(k string, v bool) Field              { return Field{k, v} }
func Duration(k string, v time.Duration) Field { return Field{k, v.String()} }
func Any(k string, v any) Field                { return Field{k, v} }

// Err wraps an error as a field (nil-safe: yields an empty string).
func Err(err error) Field {
	if err == nil {
		return Field{"error", ""}
	}
	return Field{"error", err.Error()}
}

// Record is one immutable log entry handed to every sink. Sinks must treat it as
// read-only; the framework may reuse backing arrays otherwise.
type Record struct {
	Time      time.Time
	Level     Level
	Message   string
	Component string
	Fields    []Field
	IDs       correlation.IDs
}

// Sink is the destination seam a log record is written to. Implementations must
// be safe for concurrent Write calls and must never block indefinitely — a sink
// that cannot keep up should drop and account internally. Exporters implement
// this to receive the platform's logs.
type Sink interface {
	Name() string
	Write(Record)
	// Flush blocks until buffered records are written (best-effort).
	Flush() error
	// Close releases sink resources.
	Close() error
}

// Sampler decides whether a record is emitted, enabling high-volume logging
// without overwhelming sinks. It must be safe for concurrent use.
type Sampler interface {
	Sample(Record) bool
}

// AllSampler emits every record (sampling disabled).
type AllSampler struct{}

// Sample always returns true.
func (AllSampler) Sample(Record) bool { return true }
