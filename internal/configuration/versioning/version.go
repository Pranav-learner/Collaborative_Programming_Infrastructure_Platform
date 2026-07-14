package versioning

import (
	"fmt"
	"sync"
	"time"
)

// Snapshot represents a point-in-time configuration state.
type Snapshot struct {
	Version   int               `json:"version"`
	Timestamp time.Time         `json:"timestamp"`
	Data      map[string]string `json:"data"`
	Metadata  ChangeMetadata    `json:"metadata"`
}

// ChangeMetadata stores the author, action, and rationale for a config update.
type ChangeMetadata struct {
	Actor       string `json:"actor"`
	Action      string `json:"action"` // e.g. "load", "set", "rollback", "reload"
	Description string `json:"description"`
}

// Diff represents the difference between two configuration states.
type Diff struct {
	Added   map[string]string `json:"added"`
	Changed map[string]ValueChange `json:"changed"`
	Removed []string          `json:"removed"`
}

// ValueChange describes a change for a single key.
type ValueChange struct {
	Old string `json:"old"`
	New string `json:"new"`
}

// VersionManager maintains a thread-safe history of configuration snapshots.
type VersionManager struct {
	mu          sync.RWMutex
	maxVersions int
	history     []*Snapshot
}

// NewVersionManager creates a new VersionManager.
func NewVersionManager(maxVersions int) *VersionManager {
	if maxVersions <= 0 {
		maxVersions = 50 // default safety margin
	}
	return &VersionManager{
		maxVersions: maxVersions,
		history:     make([]*Snapshot, 0),
	}
}

// RecordSnapshot adds a new configuration state to history.
func (vm *VersionManager) RecordSnapshot(data map[string]string, meta ChangeMetadata) *Snapshot {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	nextVersion := 1
	if len(vm.history) > 0 {
		nextVersion = vm.history[len(vm.history)-1].Version + 1
	}

	// Deep copy data to prevent external mutation
	copiedData := make(map[string]string, len(data))
	for k, v := range data {
		copiedData[k] = v
	}

	snap := &Snapshot{
		Version:   nextVersion,
		Timestamp: time.Now(),
		Data:      copiedData,
		Metadata:  meta,
	}

	vm.history = append(vm.history, snap)

	// Trim old history if limit exceeded
	if len(vm.history) > vm.maxVersions {
		vm.history = vm.history[len(vm.history)-vm.maxVersions:]
	}

	return snap
}

// Current returns the latest configuration snapshot, or nil if none exists.
func (vm *VersionManager) Current() *Snapshot {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	if len(vm.history) == 0 {
		return nil
	}
	return vm.history[len(vm.history)-1]
}

// GetByVersion retrieves a specific snapshot version.
func (vm *VersionManager) GetByVersion(version int) (*Snapshot, error) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	for _, snap := range vm.history {
		if snap.Version == version {
			return snap, nil
		}
	}
	return nil, fmt.Errorf("version %d not found in history", version)
}

// GetHistory returns the entire history log.
func (vm *VersionManager) GetHistory() []*Snapshot {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	out := make([]*Snapshot, len(vm.history))
	copy(out, vm.history)
	return out
}

// DiffSnapshots generates a delta between two configuration states.
func DiffSnapshots(oldSnap, newSnap map[string]string) Diff {
	diff := Diff{
		Added:   make(map[string]string),
		Changed: make(map[string]ValueChange),
		Removed: make([]string, 0),
	}

	for k, newV := range newSnap {
		oldV, exists := oldSnap[k]
		if !exists {
			diff.Added[k] = newV
		} else if oldV != newV {
			diff.Changed[k] = ValueChange{Old: oldV, New: newV}
		}
	}

	for k := range oldSnap {
		if _, exists := newSnap[k]; !exists {
			diff.Removed = append(diff.Removed, k)
		}
	}

	return diff
}
