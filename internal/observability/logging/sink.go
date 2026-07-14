package logging

import (
	"bufio"
	"encoding/json"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// WriterSink writes records to an io.Writer as JSON (or a compact text line),
// serializing writes with a mutex so concurrent loggers never interleave a line.
// It is the built-in stdout/stderr sink.
type WriterSink struct {
	name string
	json bool
	mu   sync.Mutex
	w    *bufio.Writer
	raw  io.Writer
}

// NewWriterSink constructs a WriterSink over w. When jsonEncode is false it emits
// a human-readable text line instead of JSON.
func NewWriterSink(name string, w io.Writer, jsonEncode bool) *WriterSink {
	return &WriterSink{name: name, json: jsonEncode, w: bufio.NewWriter(w), raw: w}
}

// Name identifies the sink.
func (s *WriterSink) Name() string { return s.name }

// Write encodes and emits a record.
func (s *WriterSink) Write(r Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.json {
		s.writeJSON(r)
	} else {
		s.writeText(r)
	}
}

func (s *WriterSink) writeJSON(r Record) {
	m := make(map[string]any, len(r.Fields)+8)
	m["ts"] = r.Time.UTC().Format(time.RFC3339Nano)
	m["level"] = r.Level.String()
	m["msg"] = r.Message
	if r.Component != "" {
		m["component"] = r.Component
	}
	for _, kv := range r.IDs.Fields() {
		m[kv[0]] = kv[1]
	}
	for _, f := range r.Fields {
		m[f.Key] = f.Value
	}
	enc := json.NewEncoder(s.w)
	_ = enc.Encode(m) // Encode appends a newline
}

func (s *WriterSink) writeText(r Record) {
	b := s.w
	_, _ = b.WriteString(r.Time.UTC().Format(time.RFC3339Nano))
	_, _ = b.WriteString(" [")
	_, _ = b.WriteString(r.Level.String())
	_, _ = b.WriteString("] ")
	if r.Component != "" {
		_, _ = b.WriteString(r.Component)
		_, _ = b.WriteString(": ")
	}
	_, _ = b.WriteString(r.Message)
	for _, kv := range r.IDs.Fields() {
		_, _ = b.WriteString(" ")
		_, _ = b.WriteString(kv[0])
		_, _ = b.WriteString("=")
		_, _ = b.WriteString(kv[1])
	}
	for _, f := range r.Fields {
		_, _ = b.WriteString(" ")
		_, _ = b.WriteString(f.Key)
		_, _ = b.WriteString("=")
		_, _ = b.WriteString(stringify(f.Value))
	}
	_ = b.WriteByte('\n')
}

func stringify(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// Flush flushes buffered output.
func (s *WriterSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Flush()
}

// Close flushes and releases the sink.
func (s *WriterSink) Close() error { return s.Flush() }

// --- Sampling ---

// CountSampler implements burst sampling per level: it emits the first Initial
// records of a level each second, then only every Thereafter-th record until the
// window rolls over. This bounds log volume under a storm while never hiding the
// first occurrences. Safe for concurrent use.
type CountSampler struct {
	initial    uint64
	thereafter uint64
	buckets    [5]levelBucket // indexed by Level
	now        func() time.Time
}

type levelBucket struct {
	windowUnix atomic.Int64
	count      atomic.Uint64
}

// NewCountSampler constructs a CountSampler. thereafter <= 0 is treated as 1
// (emit everything past the initial burst).
func NewCountSampler(initial, thereafter int) *CountSampler {
	if thereafter <= 0 {
		thereafter = 1
	}
	return &CountSampler{initial: uint64(initial), thereafter: uint64(thereafter), now: time.Now}
}

// Sample reports whether r should be emitted.
func (s *CountSampler) Sample(r Record) bool {
	idx := int(r.Level)
	if idx < 0 || idx >= len(s.buckets) {
		return true
	}
	b := &s.buckets[idx]
	sec := s.now().Unix()
	if b.windowUnix.Load() != sec {
		// Roll the window; a small race here at the boundary is harmless (it only
		// affects sampling precision, never correctness).
		b.windowUnix.Store(sec)
		b.count.Store(0)
	}
	n := b.count.Add(1)
	if n <= s.initial {
		return true
	}
	return (n-s.initial)%s.thereafter == 0
}
