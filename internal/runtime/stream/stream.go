package stream

import (
	"sync"
	"time"

	"cpip/internal/runtime/events"
)

// Progress carries execution progress details streamed periodically.
type Progress struct {
	Timestamp time.Time     `json:"timestamp"`
	Duration  time.Duration `json:"duration"`
	CPUUsage  float64       `json:"cpu_usage"`
	MemUsage  int64         `json:"mem_usage"`
}

// StreamManager coordinates the live streaming of stdout, stderr, and progress updates.
type StreamManager struct {
	mu            sync.RWMutex
	sessionID     string
	jobID         string
	correlationID string
	language      string
	bus           *events.Bus

	stdoutCh   chan []byte
	stderrCh   chan []byte
	progressCh chan Progress

	closed bool
}

// NewStreamManager creates a new StreamManager.
func NewStreamManager(
	sessionID string,
	jobID string,
	correlationID string,
	language string,
	bus *events.Bus,
	bufSize int,
) *StreamManager {
	if bufSize <= 0 {
		bufSize = 100
	}
	return &StreamManager{
		sessionID:     sessionID,
		jobID:         jobID,
		correlationID: correlationID,
		language:      language,
		bus:           bus,
		stdoutCh:      make(chan []byte, bufSize),
		stderrCh:      make(chan []byte, bufSize),
		progressCh:    make(chan Progress, bufSize),
	}
}

// WriteStdout streams a chunk of stdout.
func (sm *StreamManager) WriteStdout(chunk []byte) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.closed {
		return
	}

	// Publish event to the bus
	if sm.bus != nil {
		sm.bus.Publish(events.Event{
			Type:          events.StdoutChunk,
			SessionID:     sm.sessionID,
			JobID:         sm.jobID,
			CorrelationID: sm.correlationID,
			Language:      sm.language,
			Payload:       chunk,
		})
	}

	// Send to channel with non-blocking check
	select {
	case sm.stdoutCh <- chunk:
	default:
		// Drop/backpressure handling (avoid stalling pipeline)
	}
}

// WriteStderr streams a chunk of stderr.
func (sm *StreamManager) WriteStderr(chunk []byte) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.closed {
		return
	}

	// Publish event to the bus
	if sm.bus != nil {
		sm.bus.Publish(events.Event{
			Type:          events.StderrChunk,
			SessionID:     sm.sessionID,
			JobID:         sm.jobID,
			CorrelationID: sm.correlationID,
			Language:      sm.language,
			Payload:       chunk,
		})
	}

	// Send to channel with non-blocking check
	select {
	case sm.stderrCh <- chunk:
	default:
		// Drop/backpressure handling
	}
}

// WriteProgress streams a progress update.
func (sm *StreamManager) WriteProgress(p Progress) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.closed {
		return
	}

	// Publish event to the bus
	if sm.bus != nil {
		sm.bus.Publish(events.Event{
			Type:          events.ExecutionProgress,
			SessionID:     sm.sessionID,
			JobID:         sm.jobID,
			CorrelationID: sm.correlationID,
			Language:      sm.language,
			Payload:       p,
		})
	}

	// Send to channel with non-blocking check
	select {
	case sm.progressCh <- p:
	default:
		// Drop/backpressure handling
	}
}

// Stdout returns the read-only channel for stdout chunks.
func (sm *StreamManager) Stdout() <-chan []byte {
	return sm.stdoutCh
}

// Stderr returns the read-only channel for stderr chunks.
func (sm *StreamManager) Stderr() <-chan []byte {
	return sm.stderrCh
}

// Progress returns the read-only channel for progress updates.
func (sm *StreamManager) Progress() <-chan Progress {
	return sm.progressCh
}

// Close closes all stream channels and prevents further writes.
func (sm *StreamManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return
	}

	sm.closed = true
	close(sm.stdoutCh)
	close(sm.stderrCh)
	close(sm.progressCh)
}
