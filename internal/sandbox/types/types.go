package types

import (
	"fmt"
	"time"
	"sync"
)

// State defines the sandbox lifecycle state.
type State string

const (
	StateCreated          State = "created"
	StatePreparing        State = "preparing"
	StateContainerCreated State = "container_created"
	StateReady            State = "ready"
	StateExecuting        State = "executing"
	StateRunning          State = "running"
	StateCompleted        State = "completed"
	StateFailed           State = "failed"
	StateRecovering       State = "recovering"
	StateTimedOut         State = "timed_out"
	StateTerminating      State = "terminating"
	StateCleaning         State = "cleaning"
	StateDestroyed        State = "destroyed"
)

// SandboxSession wraps runtime state and metadata for an isolated container execution session.
type SandboxSession struct {
	mu             sync.RWMutex
	ID             string            `json:"id"`
	ContainerID    string            `json:"container_id"`
	JobID          string            `json:"job_id"`
	WorkerID       string            `json:"worker_id"`
	RuntimeID      string            `json:"runtime_id"`
	Language       string            `json:"language"`
	Status         string            `json:"status"` // "running", "exited", "created", "unknown"
	State          State             `json:"state"`
	Image          string            `json:"image"`
	WorkspacePath  string            `json:"workspace_path"`
	Mounts         []string          `json:"mounts"`
	Volumes        []string          `json:"volumes"`
	Network        string            `json:"network"`
	CreatedAt        time.Time         `json:"created_at"`
	ExpiresAt        time.Time         `json:"expires_at"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	Stats            Stats             `json:"stats"`
	MemoryLimitBytes int64             `json:"memory_limit_bytes"`
	ProcessLimit     int64             `json:"process_limit"`
}

func (s *SandboxSession) GetState() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State
}

func (s *SandboxSession) SetState(st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.State = st
}

func (s *SandboxSession) GetContainerID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ContainerID
}

func (s *SandboxSession) SetContainerID(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ContainerID = id
}

func (s *SandboxSession) GetWorkspacePath() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.WorkspacePath
}

func (s *SandboxSession) SetWorkspacePath(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.WorkspacePath = path
}

func (s *SandboxSession) GetNetwork() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Network
}

func (s *SandboxSession) SetNetwork(net string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Network = net
}

func (s *SandboxSession) GetMounts() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.Mounts == nil {
		return nil
	}
	res := make([]string, len(s.Mounts))
	copy(res, s.Mounts)
	return res
}

func (s *SandboxSession) SetMounts(mounts []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Mounts = mounts
}

func (s *SandboxSession) GetExpiresAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ExpiresAt
}

func (s *SandboxSession) SetExpiresAt(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ExpiresAt = t
}

func (s *SandboxSession) GetStatus() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Status
}

func (s *SandboxSession) SetStatus(st string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = st
}

func (s *SandboxSession) GetStats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Stats
}

func (s *SandboxSession) SetStats(st Stats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Stats = st
}

func (s *SandboxSession) GetMemoryLimitBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.MemoryLimitBytes
}

func (s *SandboxSession) SetMemoryLimitBytes(l int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MemoryLimitBytes = l
}

func (s *SandboxSession) GetProcessLimit() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ProcessLimit
}

func (s *SandboxSession) SetProcessLimit(l int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ProcessLimit = l
}

func (s *SandboxSession) Lock() {
	s.mu.Lock()
}

func (s *SandboxSession) Unlock() {
	s.mu.Unlock()
}

func (s *SandboxSession) RLock() {
	s.mu.RLock()
}

func (s *SandboxSession) RUnlock() {
	s.mu.RUnlock()
}

// Stats holds CPU and memory resource consumption metrics.
type Stats struct {
	CPUPercentage    float64 `json:"cpu_percentage"`
	MemoryUsageBytes int64   `json:"memory_usage_bytes"`
	DiskUsageBytes   int64   `json:"disk_usage_bytes"`
	OutputSizeBytes  int64   `json:"output_size_bytes"`
	ProcessCount     int64   `json:"process_count"`
}

// ValidateTransition returns nil if a state transition is legal, or an error if illegal.
func ValidateTransition(current, next State) error {
	if current == next {
		return nil
	}

	allowed := false
	switch current {
	case StateCreated:
		allowed = (next == StatePreparing || next == StateCleaning || next == StateDestroyed)
	case StatePreparing:
		allowed = (next == StateContainerCreated || next == StateReady || next == StateCleaning || next == StateDestroyed)
	case StateContainerCreated:
		allowed = (next == StateReady || next == StateCleaning || next == StateDestroyed)
	case StateReady:
		allowed = (next == StateExecuting || next == StateRunning || next == StateCleaning || next == StateDestroyed)
	case StateExecuting:
		allowed = (next == StateReady || next == StateRunning || next == StateCompleted || next == StateFailed || next == StateTimedOut || next == StateCleaning || next == StateDestroyed)
	case StateRunning:
		allowed = (next == StateCompleted || next == StateFailed || next == StateTimedOut || next == StateCleaning || next == StateDestroyed || next == StateReady)
	case StateCompleted:
		allowed = (next == StateCleaning || next == StateDestroyed)
	case StateFailed:
		allowed = (next == StateRecovering || next == StateCleaning || next == StateDestroyed)
	case StateRecovering:
		allowed = (next == StateRunning || next == StateExecuting || next == StateFailed || next == StateCleaning || next == StateDestroyed)
	case StateTimedOut:
		allowed = (next == StateTerminating || next == StateCleaning || next == StateDestroyed)
	case StateTerminating:
		allowed = (next == StateCleaning || next == StateDestroyed)
	case StateCleaning:
		allowed = (next == StateDestroyed)
	case StateDestroyed:
		allowed = false // Terminal state
	}

	if !allowed {
		return fmt.Errorf("invalid sandbox state transition: %s -> %s", current, next)
	}
	return nil
}
