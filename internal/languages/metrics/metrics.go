package metrics

import "sync"

// Recorder defines methods to capture language plugin framework telemetry.
type Recorder interface {
	RecordRegister(pluginID string)
	RecordLoad(pluginID string)
	RecordUnload(pluginID string)
	RecordExecution(pluginID string, success bool, compileTimeMs, execTimeMs int64)
	RecordTimeout(pluginID string)
	RecordCancellation(pluginID string)
	RecordDiagnostics(pluginID string, errCount int)
}

// InMemRecorder is a thread-safe implementation of Recorder storing metrics in memory.
type InMemRecorder struct {
	mu           sync.RWMutex
	Registers    map[string]int64
	Loads        map[string]int64
	Unloads      map[string]int64
	Executions   map[string]int64
	Successes    map[string]int64
	Failures     map[string]int64
	Timeouts     map[string]int64
	Cancels      map[string]int64
	CompileTime  map[string]int64
	ExecTime     map[string]int64
	Diagnostics  map[string]int64
}

// NewInMemRecorder creates a new InMemRecorder.
func NewInMemRecorder() *InMemRecorder {
	return &InMemRecorder{
		Registers:   make(map[string]int64),
		Loads:       make(map[string]int64),
		Unloads:     make(map[string]int64),
		Executions:  make(map[string]int64),
		Successes:   make(map[string]int64),
		Failures:    make(map[string]int64),
		Timeouts:    make(map[string]int64),
		Cancels:     make(map[string]int64),
		CompileTime: make(map[string]int64),
		ExecTime:    make(map[string]int64),
		Diagnostics: make(map[string]int64),
	}
}

func (r *InMemRecorder) RecordRegister(pluginID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Registers[pluginID]++
}

func (r *InMemRecorder) RecordLoad(pluginID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Loads[pluginID]++
}

func (r *InMemRecorder) RecordUnload(pluginID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Unloads[pluginID]++
}

func (r *InMemRecorder) RecordExecution(pluginID string, success bool, compileTimeMs, execTimeMs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Executions[pluginID]++
	if success {
		r.Successes[pluginID]++
	} else {
		r.Failures[pluginID]++
	}
	r.CompileTime[pluginID] += compileTimeMs
	r.ExecTime[pluginID] += execTimeMs
}

func (r *InMemRecorder) RecordTimeout(pluginID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Timeouts[pluginID]++
}

func (r *InMemRecorder) RecordCancellation(pluginID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Cancels[pluginID]++
}

func (r *InMemRecorder) RecordDiagnostics(pluginID string, errCount int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Diagnostics[pluginID] += int64(errCount)
}
