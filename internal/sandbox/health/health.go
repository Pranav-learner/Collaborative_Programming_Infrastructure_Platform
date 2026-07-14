package health

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

// HealthMonitor tracks health status of active sandboxes.
type HealthMonitor struct {
	mu        sync.RWMutex
	snapshots map[string]SandboxHealthSnapshot
	bus       *events.Bus
	adapter   runtime.RuntimeAdapter
}

// NewHealthMonitor creates a new passive HealthMonitor instance.
func NewHealthMonitor(bus *events.Bus, adapter runtime.RuntimeAdapter) *HealthMonitor {
	return &HealthMonitor{
		snapshots: make(map[string]SandboxHealthSnapshot),
		bus:       bus,
		adapter:   adapter,
	}
}

// CheckHealth queries the runtime adapter and filesystem status to compute a health snapshot.
func (hm *HealthMonitor) CheckHealth(ctx context.Context, sess *types.SandboxSession) (SandboxHealthSnapshot, error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	sandboxID := sess.ID
	cID := sess.GetContainerID()

	// Default/Start snapshot
	snap := SandboxHealthSnapshot{
		SandboxID:       sandboxID,
		ContainerHealth: "unknown",
		RuntimeHealth:   "healthy",
		Filesystem:      "ok",
		Heartbeat:       time.Now(),
		Status:          string(sess.GetState()),
		LastUpdated:     time.Now(),
		HealthScore:     100,
	}

	if cID != "" {
		info, err := hm.adapter.InspectContainer(ctx, cID)
		if err != nil {
			snap.ContainerHealth = "unhealthy"
			snap.RuntimeHealth = "unhealthy"
			snap.HealthScore -= 50
			sess.SetStatus("unknown")
		} else {
			snap.Status = info.State
			sess.SetStatus(info.State)
			if info.Running {
				snap.ContainerHealth = "healthy"
			} else {
				snap.ContainerHealth = "unhealthy"
				snap.HealthScore -= 40
			}
		}

		stats, err := hm.adapter.GetContainerStats(ctx, cID)
		if err == nil {
			snap.CPU = stats.CPUPercentage
			snap.Memory = stats.MemoryUsageBytes
			snap.ProcessCount = stats.ProcessCount
			snap.Disk = stats.DiskUsageBytes
			snap.OutputSize = stats.OutputSizeBytes

			// Score adjustments
			if sess.GetMemoryLimitBytes() > 0 {
				ratio := float64(stats.MemoryUsageBytes) / float64(sess.GetMemoryLimitBytes())
				if ratio > 0.95 {
					snap.HealthScore -= 30
				} else if ratio > 0.75 {
					snap.HealthScore -= 10
				}
			}
			if stats.CPUPercentage > 95 {
				snap.HealthScore -= 10
			}
		}
	} else {
		snap.ContainerHealth = "unknown"
		snap.HealthScore -= 20
	}

	// Workspace path checks
	wkPath := sess.GetWorkspacePath()
	if wkPath != "" {
		if fi, err := os.Stat(wkPath); err != nil {
			snap.Filesystem = "corrupted"
			snap.HealthScore -= 30
		} else if !fi.IsDir() {
			snap.Filesystem = "corrupted"
			snap.HealthScore -= 30
		} else {
			testFile := filepath.Join(wkPath, ".health_check")
			if f, err := os.OpenFile(testFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
				snap.Filesystem = "readonly"
				snap.HealthScore -= 30
			} else {
				f.Close()
				os.Remove(testFile)
			}
		}
	}

	if snap.HealthScore < 0 {
		snap.HealthScore = 0
	}

	prev, exists := hm.snapshots[sandboxID]
	hm.snapshots[sandboxID] = snap

	if hm.bus != nil {
		isHealthy := snap.HealthScore >= 70
		wasHealthy := !exists || prev.HealthScore >= 70

		if isHealthy && !wasHealthy {
			hm.bus.Publish(events.Event{
				Type:           events.SandboxHealthy,
				SandboxID:      sandboxID,
				JobID:          sess.JobID,
				Timestamp:      time.Now(),
				LifecycleState: string(sess.GetState()),
				Severity:       "Info",
				Origin:         "health",
				Payload:        snap,
			})
		} else if !isHealthy && wasHealthy {
			hm.bus.Publish(events.Event{
				Type:           events.SandboxUnhealthy,
				SandboxID:      sandboxID,
				JobID:          sess.JobID,
				Timestamp:      time.Now(),
				LifecycleState: string(sess.GetState()),
				Severity:       "Critical",
				Origin:         "health",
				Payload:        snap,
			})
		}
	}

	return snap, nil
}

// GetSnapshot retrieves the health snapshot for a sandbox.
func (hm *HealthMonitor) GetSnapshot(sandboxID string) (SandboxHealthSnapshot, bool) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	snap, exists := hm.snapshots[sandboxID]
	return snap, exists
}

// GetSnapshots retrieves all cached health snapshots.
func (hm *HealthMonitor) GetSnapshots() map[string]SandboxHealthSnapshot {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	res := make(map[string]SandboxHealthSnapshot, len(hm.snapshots))
	for k, v := range hm.snapshots {
		res[k] = v
	}
	return res
}

// Remove cleans up cached health snapshots for a destroyed sandbox.
func (hm *HealthMonitor) Remove(sandboxID string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	delete(hm.snapshots, sandboxID)
}
