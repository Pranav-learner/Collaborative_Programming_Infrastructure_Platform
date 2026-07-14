package metrics

import "sync"

// Recorder outlines methods for tracking sandbox subsystem metrics.
type Recorder interface {
	RecordCreate(sandboxID string, language string)
	RecordDestroy(sandboxID string)
	RecordFailure(language string, stage string)
	RecordExecution(sandboxID string)
	RecordNetworkCreate()
	RecordVolumeCreate()
}

// InMemRecorder is a thread-safe implementation of Recorder storing metrics in memory.
type InMemRecorder struct {
	mu            sync.RWMutex
	Creations     map[string]int64
	Destructions  map[string]int64
	Failures      map[string]int64
	Executions    map[string]int64
	NetworkCount  int64
	VolumeCount   int64
}

// NewInMemRecorder initializes a new InMemRecorder instance.
func NewInMemRecorder() *InMemRecorder {
	return &InMemRecorder{
		Creations:    make(map[string]int64),
		Destructions: make(map[string]int64),
		Failures:     make(map[string]int64),
		Executions:    make(map[string]int64),
	}
}

func (r *InMemRecorder) RecordCreate(sandboxID string, language string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Creations[language]++
}

func (r *InMemRecorder) RecordDestroy(sandboxID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Destructions[sandboxID]++
}

func (r *InMemRecorder) RecordFailure(language string, stage string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := language + ":" + stage
	r.Failures[key]++
}

func (r *InMemRecorder) RecordExecution(sandboxID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Executions[sandboxID]++
}

func (r *InMemRecorder) RecordNetworkCreate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.NetworkCount++
}

func (r *InMemRecorder) RecordVolumeCreate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.VolumeCount++
}
