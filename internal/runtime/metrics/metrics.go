package metrics

import (
	"sync"
	"time"
)

// Recorder defines the telemetry interface for the runtime subsystem.
type Recorder interface {
	RecordCompilation(language string, duration time.Duration, success bool)
	RecordExecution(language string, duration time.Duration, state string)
	RecordBytesStreamed(streamType string, count int64)
	RecordTimeout(language string, timeoutType string)
	RecordCancellation(language string, initiator string)
	RecordSessionError(language string, errType string)
}

// InMemRecorder implements Recorder in-memory for testing and default metrics.
type InMemRecorder struct {
	mu           sync.RWMutex
	compilations map[string]int
	executions   map[string]int
	bytesStream  map[string]int64
	timeouts     map[string]int
	cancels      map[string]int
	errors       map[string]int
}

// NewInMemRecorder constructs a thread-safe in-memory Recorder.
func NewInMemRecorder() *InMemRecorder {
	return &InMemRecorder{
		compilations: make(map[string]int),
		executions:   make(map[string]int),
		bytesStream:  make(map[string]int64),
		timeouts:     make(map[string]int),
		cancels:      make(map[string]int),
		errors:       make(map[string]int),
	}
}

func (r *InMemRecorder) RecordCompilation(language string, duration time.Duration, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := language
	if success {
		key += ":success"
	} else {
		key += ":failure"
	}
	r.compilations[key]++
}

func (r *InMemRecorder) RecordExecution(language string, duration time.Duration, state string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executions[language+":"+state]++
}

func (r *InMemRecorder) RecordBytesStreamed(streamType string, count int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bytesStream[streamType] += count
}

func (r *InMemRecorder) RecordTimeout(language string, timeoutType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.timeouts[language+":"+timeoutType]++
}

func (r *InMemRecorder) RecordCancellation(language string, initiator string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancels[language+":"+initiator]++
}

func (r *InMemRecorder) RecordSessionError(language string, errType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors[language+":"+errType]++
}

// GetCompilationCount returns compilation counts for test assertions.
func (r *InMemRecorder) GetCompilationCount(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.compilations[key]
}

// GetExecutionCount returns execution counts for test assertions.
func (r *InMemRecorder) GetExecutionCount(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.executions[key]
}

// GetBytesStreamed returns bytes streamed for test assertions.
func (r *InMemRecorder) GetBytesStreamed(streamType string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bytesStream[streamType]
}

// GetTimeoutCount returns timeout counts for test assertions.
func (r *InMemRecorder) GetTimeoutCount(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.timeouts[key]
}

// NoopRecorder is a no-op implementation of Recorder.
type NoopRecorder struct{}

func (NoopRecorder) RecordCompilation(language string, duration time.Duration, success bool) {}
func (NoopRecorder) RecordExecution(language string, duration time.Duration, state string)   {}
func (NoopRecorder) RecordBytesStreamed(streamType string, count int64)                      {}
func (NoopRecorder) RecordTimeout(language string, timeoutType string)                      {}
func (NoopRecorder) RecordCancellation(language string, initiator string)                    {}
func (NoopRecorder) RecordSessionError(language string, errType string)                      {}
