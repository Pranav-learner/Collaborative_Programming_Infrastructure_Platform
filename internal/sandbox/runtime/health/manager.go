package health

import (
	"sync"
	"time"

	"cpip/internal/sandbox/events"
)

// HealthSnapshot defines the detailed health statistics of a runtime.
type HealthSnapshot struct {
	RuntimeID    string
	Available    bool
	LastHeartbeat time.Time
	Latency      time.Duration
	FailureCount int64
	Status       string // "Healthy", "Degraded", "Unhealthy"
}

// RuntimeHealthManager tracks and manages the health state of each runtime.
type RuntimeHealthManager struct {
	mu        sync.RWMutex
	snapshots map[string]*HealthSnapshot
	history   map[string][]HealthSnapshot
	bus       *events.Bus
}

// NewRuntimeHealthManager initializes the health manager.
func NewRuntimeHealthManager(bus *events.Bus) *RuntimeHealthManager {
	return &RuntimeHealthManager{
		snapshots: make(map[string]*HealthSnapshot),
		history:   make(map[string][]HealthSnapshot),
		bus:       bus,
	}
}

// RecordHeartbeat logs a successful runtime check-in.
func (m *RuntimeHealthManager) RecordHeartbeat(runtimeID string, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap, ok := m.snapshots[runtimeID]
	if !ok {
		snap = &HealthSnapshot{RuntimeID: runtimeID}
		m.snapshots[runtimeID] = snap
	}

	prevStatus := snap.Status
	snap.Available = true
	snap.LastHeartbeat = time.Now()
	snap.Latency = latency
	snap.FailureCount = 0
	snap.Status = "Healthy"

	m.appendHistory(runtimeID, *snap)

	if prevStatus != "Healthy" && m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.AuditRecorded, // using existing types if needed, or generic events
			SandboxID: "",
			Timestamp: time.Now(),
			Payload: map[string]any{
				"event_type":  "RuntimeHealthChanged",
				"runtime_id":  runtimeID,
				"prev_status": prevStatus,
				"new_status":  "Healthy",
			},
		})
	}
}

// RecordFailure increments failure metrics and updates status.
func (m *RuntimeHealthManager) RecordFailure(runtimeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap, ok := m.snapshots[runtimeID]
	if !ok {
		snap = &HealthSnapshot{RuntimeID: runtimeID}
		m.snapshots[runtimeID] = snap
	}

	snap.FailureCount++
	prevStatus := snap.Status

	if snap.FailureCount > 5 {
		snap.Available = false
		snap.Status = "Unhealthy"
	} else {
		snap.Status = "Degraded"
	}

	m.appendHistory(runtimeID, *snap)

	if prevStatus != snap.Status && m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.AuditRecorded,
			SandboxID: "",
			Timestamp: time.Now(),
			Payload: map[string]any{
				"event_type":  "RuntimeHealthChanged",
				"runtime_id":  runtimeID,
				"prev_status": prevStatus,
				"new_status":  snap.Status,
			},
		})
	}
}

// GetSnapshot returns a health snapshot copy.
func (m *RuntimeHealthManager) GetSnapshot(runtimeID string) (HealthSnapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snap, ok := m.snapshots[runtimeID]
	if !ok {
		return HealthSnapshot{}, false
	}
	return *snap, true
}

// GetHistory returns the health history copy of a runtime.
func (m *RuntimeHealthManager) GetHistory(runtimeID string) []HealthSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	h, ok := m.history[runtimeID]
	if !ok {
		return nil
	}
	copied := make([]HealthSnapshot, len(h))
	copy(copied, h)
	return copied
}

func (m *RuntimeHealthManager) appendHistory(runtimeID string, snap HealthSnapshot) {
	h := m.history[runtimeID]
	if len(h) >= 50 {
		h = h[1:] // Limit history size
	}
	m.history[runtimeID] = append(h, snap)
}
