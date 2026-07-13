package types

import "time"

// WorkerState is a position in the worker lifecycle state machine.
//
//	Starting → Idle → Reserved → Executing → Completed → Idle
//	                                   │
//	                                Failed → Recovering → Idle
//	<any>    → Offline (terminal for the instance)
type WorkerState uint8

const (
	// WorkerStarting is a worker booting and not yet ready for work.
	WorkerStarting WorkerState = iota
	// WorkerIdle is a ready worker awaiting an assignment.
	WorkerIdle
	// WorkerReserved is a worker assigned a message but not yet executing.
	WorkerReserved
	// WorkerExecuting is a worker processing its assignment.
	WorkerExecuting
	// WorkerCompleted is a worker that just finished an assignment successfully.
	WorkerCompleted
	// WorkerFailed is a worker whose assignment failed.
	WorkerFailed
	// WorkerRecovering is a failed worker being brought back to health.
	WorkerRecovering
	// WorkerOffline is a decommissioned/dead worker (terminal for the instance).
	WorkerOffline
)

// String returns the lowercase name of the worker state.
func (s WorkerState) String() string {
	switch s {
	case WorkerStarting:
		return "starting"
	case WorkerIdle:
		return "idle"
	case WorkerReserved:
		return "reserved"
	case WorkerExecuting:
		return "executing"
	case WorkerCompleted:
		return "completed"
	case WorkerFailed:
		return "failed"
	case WorkerRecovering:
		return "recovering"
	case WorkerOffline:
		return "offline"
	default:
		return "unknown"
	}
}

var workerTransitions = map[WorkerState]map[WorkerState]struct{}{
	WorkerStarting:   setOfWorker(WorkerIdle, WorkerOffline),
	WorkerIdle:       setOfWorker(WorkerReserved, WorkerOffline, WorkerRecovering),
	WorkerReserved:   setOfWorker(WorkerExecuting, WorkerIdle, WorkerFailed, WorkerOffline),
	WorkerExecuting:  setOfWorker(WorkerCompleted, WorkerFailed, WorkerOffline),
	WorkerCompleted:  setOfWorker(WorkerIdle, WorkerOffline),
	WorkerFailed:     setOfWorker(WorkerRecovering, WorkerOffline),
	WorkerRecovering: setOfWorker(WorkerIdle, WorkerOffline),
	WorkerOffline:    setOfWorker(),
}

func setOfWorker(states ...WorkerState) map[WorkerState]struct{} {
	m := make(map[WorkerState]struct{}, len(states))
	for _, s := range states {
		m[s] = struct{}{}
	}
	return m
}

// CanTransitionWorker reports whether from → to is a legal worker transition.
func CanTransitionWorker(from, to WorkerState) bool {
	next, ok := workerTransitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

// IsTerminal reports whether the worker state admits no further transitions.
func (s WorkerState) IsTerminal() bool { return s == WorkerOffline }

// IsAvailable reports whether a worker in this state can accept a new assignment.
func (s WorkerState) IsAvailable() bool { return s == WorkerIdle }

// Health classifies a worker's health independently of its lifecycle state.
type Health uint8

const (
	// HealthUnknown is the zero value before the first heartbeat.
	HealthUnknown Health = iota
	// HealthHealthy indicates recent heartbeats and normal operation.
	HealthHealthy
	// HealthDegraded indicates a missed heartbeat or elevated failures.
	HealthDegraded
	// HealthUnhealthy indicates heartbeat timeout or repeated failures.
	HealthUnhealthy
)

// String returns the lowercase name of the health.
func (h Health) String() string {
	switch h {
	case HealthHealthy:
		return "healthy"
	case HealthDegraded:
		return "degraded"
	case HealthUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// ResourceProfile describes a worker's resource envelope (advisory metadata).
type ResourceProfile struct {
	CPUMillicores int   `json:"cpu_millicores"`
	MemoryBytes   int64 `json:"memory_bytes"`
	MaxConcurrent int   `json:"max_concurrent"`
}

// WorkerStats is an immutable snapshot of a worker's counters.
type WorkerStats struct {
	Processed  uint64        `json:"processed"`
	Succeeded  uint64        `json:"succeeded"`
	Failed     uint64        `json:"failed"`
	Retried    uint64        `json:"retried"`
	BusyTime   time.Duration `json:"busy_time"`
	LastJobAt  time.Time     `json:"last_job_at"`
}

// Worker is the runtime descriptor for a worker in the pool. Worker values are
// plain data; the registry holds the authoritative copy under its lock and hands
// out copies via Clone.
type Worker struct {
	ID           string          `json:"id"`
	State        WorkerState     `json:"state"`
	Health       Health          `json:"health"`
	Capabilities []string        `json:"capabilities"`
	CurrentJob   string          `json:"current_job,omitempty"`
	CurrentMsg   string          `json:"current_msg,omitempty"`
	LastHeartbeat time.Time      `json:"last_heartbeat"`
	Version      string          `json:"version"`
	Runtime      map[string]string `json:"runtime,omitempty"`
	Resources    ResourceProfile `json:"resources"`
	Stats        WorkerStats     `json:"stats"`
	StartedAt    time.Time       `json:"started_at"`

	// SandboxID is a forward-looking hook for the Docker-sandbox module.
	SandboxID string `json:"sandbox_id,omitempty"`

	// Transitions counts lifecycle changes, for observability.
	Transitions int `json:"transitions"`
}

// Clone returns a deep copy of the worker.
func (w Worker) Clone() Worker {
	cp := w
	if w.Capabilities != nil {
		cp.Capabilities = append([]string(nil), w.Capabilities...)
	}
	if w.Runtime != nil {
		cp.Runtime = make(map[string]string, len(w.Runtime))
		for k, v := range w.Runtime {
			cp.Runtime[k] = v
		}
	}
	return cp
}

// HasCapability reports whether the worker can handle the given capability
// (e.g. a language ID). A worker with no declared capabilities is a universal
// worker and matches everything.
func (w Worker) HasCapability(cap string) bool {
	if len(w.Capabilities) == 0 {
		return true
	}
	for _, c := range w.Capabilities {
		if c == cap {
			return true
		}
	}
	return false
}
