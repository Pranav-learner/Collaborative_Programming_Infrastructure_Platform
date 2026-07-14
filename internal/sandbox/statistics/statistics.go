package statistics

import (
	"sync"

	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/health"
)

// RuntimeStatistics tracks overall execution scope.
type RuntimeStatistics struct {
	ActiveSandboxes         int64 `json:"active_sandboxes"`
	TotalSandboxesCreated   int64 `json:"total_sandboxes_created"`
	TotalSandboxesDestroyed int64 `json:"total_sandboxes_destroyed"`
}

// ResourceStatistics tracks CPU and Memory aggregates.
type ResourceStatistics struct {
	AverageCPUPercent  float64 `json:"average_cpu_percent"`
	AverageMemoryBytes int64   `json:"average_memory_bytes"`
	PeakMemoryBytes    int64   `json:"peak_memory_bytes"`
	PeakCPUPercent     float64 `json:"peak_cpu_percent"`
	samplesCount       int64
}

// LifecycleStatistics tracks transition metrics.
type LifecycleStatistics struct {
	StateTransitionCounts map[string]int64 `json:"state_transition_counts"`
}

// RecoveryStatistics tracks automated restarts.
type RecoveryStatistics struct {
	RecoveryAttempts  int64 `json:"recovery_attempts"`
	RecoverySuccesses int64 `json:"recovery_successes"`
	RecoveryFailures  int64 `json:"recovery_failures"`
}

// CleanupStatistics tracks GC metrics.
type CleanupStatistics struct {
	OrphansCleaned         int64 `json:"orphans_cleaned"`
	TotalWorkspacesCleaned int64 `json:"total_workspaces_cleaned"`
}

// SandboxStatistics aggregates all sub-statistic tiers.
type SandboxStatistics struct {
	Runtime   RuntimeStatistics   `json:"runtime"`
	Resource  ResourceStatistics  `json:"resource"`
	Lifecycle LifecycleStatistics `json:"lifecycle"`
	Recovery  RecoveryStatistics  `json:"recovery"`
	Cleanup   CleanupStatistics   `json:"cleanup"`
}

// StatisticsCollector monitors sandbox events to aggregate operational metrics.
type StatisticsCollector struct {
	mu       sync.RWMutex
	stats    SandboxStatistics
	bus      *events.Bus
	stopChan chan struct{}
}

// NewStatisticsCollector initializes a new StatisticsCollector.
func NewStatisticsCollector(bus *events.Bus) *StatisticsCollector {
	return &StatisticsCollector{
		bus: bus,
		stats: SandboxStatistics{
			Lifecycle: LifecycleStatistics{
				StateTransitionCounts: make(map[string]int64),
			},
		},
		stopChan: make(chan struct{}),
	}
}

// Start listens on the event bus to update aggregate stats.
func (sc *StatisticsCollector) Start() {
	if sc.bus == nil {
		return
	}
	ch := sc.bus.Subscribe(100)
	go func() {
		defer sc.bus.Unsubscribe(ch)
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					return
				}
				sc.processEvent(ev)
			case <-sc.stopChan:
				return
			}
		}
	}()
}

// Stop halts the event loop.
func (sc *StatisticsCollector) Stop() {
	close(sc.stopChan)
}

// GetStatistics returns a copy of current aggregated statistics.
func (sc *StatisticsCollector) GetStatistics() SandboxStatistics {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// Deep copy
	res := sc.stats
	res.Lifecycle.StateTransitionCounts = make(map[string]int64)
	for k, v := range sc.stats.Lifecycle.StateTransitionCounts {
		res.Lifecycle.StateTransitionCounts[k] = v
	}
	return res
}

func (sc *StatisticsCollector) processEvent(ev events.Event) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Update transition counts
	if ev.LifecycleState != "" {
		sc.stats.Lifecycle.StateTransitionCounts[ev.LifecycleState]++
	}

	switch ev.Type {
	case events.SandboxCreated:
		sc.stats.Runtime.TotalSandboxesCreated++
		sc.stats.Runtime.ActiveSandboxes++

	case events.SandboxDestroyed:
		sc.stats.Runtime.TotalSandboxesDestroyed++
		if sc.stats.Runtime.ActiveSandboxes > 0 {
			sc.stats.Runtime.ActiveSandboxes--
		}
		sc.stats.Cleanup.TotalWorkspacesCleaned++

	case events.SandboxHealthy, events.SandboxUnhealthy:
		if snap, ok := ev.Payload.(health.SandboxHealthSnapshot); ok {
			sc.updateResourceStats(snap.CPU, snap.Memory)
		}

	case events.SandboxRecovered:
		sc.stats.Recovery.RecoveryAttempts++
		if payloadMap, ok := ev.Payload.(map[string]any); ok {
			status := payloadMap["status"]
			if status == "succeeded" {
				sc.stats.Recovery.RecoverySuccesses++
			} else if status == "failed" {
				sc.stats.Recovery.RecoveryFailures++
			}
		}

	case events.CleanupStarted:
		// orphan tracking could be added in payload
		if payloadMap, ok := ev.Payload.(map[string]any); ok {
			if isOrphan, _ := payloadMap["orphan"].(bool); isOrphan {
				sc.stats.Cleanup.OrphansCleaned++
			}
		}
	}
}

func (sc *StatisticsCollector) updateResourceStats(cpu float64, mem int64) {
	r := &sc.stats.Resource
	r.samplesCount++

	// Recalculate rolling average
	n := float64(r.samplesCount)
	r.AverageCPUPercent = ((r.AverageCPUPercent * (n - 1)) + cpu) / n
	r.AverageMemoryBytes = int64(((float64(r.AverageMemoryBytes) * (n - 1)) + float64(mem)) / n)

	if mem > r.PeakMemoryBytes {
		r.PeakMemoryBytes = mem
	}
	if cpu > r.PeakCPUPercent {
		r.PeakCPUPercent = cpu
	}
}
