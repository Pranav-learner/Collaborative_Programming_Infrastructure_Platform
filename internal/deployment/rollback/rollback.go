package rollback

import (
	"fmt"
	"sync"
	"time"

	"cpip/internal/deployment/services"
)

// DeploymentStatus represents the outcome of a deployment event.
type DeploymentStatus string

const (
	StatusSuccess DeploymentStatus = "success"
	StatusFailed  DeploymentStatus = "failed"
	StatusPending DeploymentStatus = "pending"
)

// Snapshot represents a historic deployment revision.
type Snapshot struct {
	Version     int                `json:"version"`
	Timestamp   time.Time          `json:"timestamp"`
	Profile     string             `json:"profile"`
	Services    []services.Service `json:"services"`
	Description string             `json:"description,omitempty"`
	Status      DeploymentStatus   `json:"status"`
}

// RollbackReport captures results of a rollback operation.
type RollbackReport struct {
	Success       bool      `json:"success"`
	TargetVersion int       `json:"target_version"`
	NewVersion    int       `json:"new_version"`
	Timestamp     time.Time `json:"timestamp"`
	Detail        string    `json:"detail"`
}

// Registry tracks historic deployment snapshots.
type Registry struct {
	mu       sync.RWMutex
	history  map[string][]Snapshot // profile -> snapshots list
	limits   int
	versions map[string]int        // profile -> current version number
}

// NewRegistry creates a new Registry.
func NewRegistry(limits int) *Registry {
	if limits <= 0 {
		limits = 10
	}
	return &Registry{
		history:  make(map[string][]Snapshot),
		limits:   limits,
		versions: make(map[string]int),
	}
}

// RecordSnapshot appends a new deployment snapshot to history.
func (r *Registry) RecordSnapshot(profile string, svcs []services.Service, desc string, status DeploymentStatus) Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	nextVer := r.versions[profile] + 1
	r.versions[profile] = nextVer

	snap := Snapshot{
		Version:     nextVer,
		Timestamp:   time.Now(),
		Profile:     profile,
		Services:    svcs,
		Description: desc,
		Status:      status,
	}

	list := r.history[profile]
	list = append(list, snap)

	// Enforce limits
	if len(list) > r.limits {
		list = list[len(list)-r.limits:]
	}
	r.history[profile] = list

	return snap
}

// GetByVersion retrieves a specific historic revision.
func (r *Registry) GetByVersion(profile string, version int) (Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := r.history[profile]
	for _, snap := range list {
		if snap.Version == version {
			return snap, nil
		}
	}
	return Snapshot{}, fmt.Errorf("revision %d not found in history for profile %q", version, profile)
}

// Current returns the latest deployment snapshot.
func (r *Registry) Current(profile string) (Snapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := r.history[profile]
	if len(list) == 0 {
		return Snapshot{}, false
	}
	return list[len(list)-1], true
}

// History returns a copy of the entire history list for a profile.
func (r *Registry) History(profile string) []Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := r.history[profile]
	copied := make([]Snapshot, len(list))
	copy(copied, list)
	return copied
}
