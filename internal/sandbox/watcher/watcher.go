package watcher

import (
	"context"
	"sync"
	"time"

	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

// ThresholdConfig holds the warning, critical and termination percentages or limits.
type ThresholdConfig struct {
	CPUWarningPercent    float64 `json:"cpu_warning_percent"`
	CPUCriticalPercent   float64 `json:"cpu_critical_percent"`
	CPUTerminatePercent  float64 `json:"cpu_terminate_percent"`
	MemoryWarningPercent float64 `json:"memory_warning_percent"`
	MemoryCriticalPercent float64 `json:"memory_critical_percent"`
	MemoryTerminatePercent float64 `json:"memory_terminate_percent"`
	DiskWarningBytes     int64   `json:"disk_warning_bytes"`
	DiskCriticalBytes    int64   `json:"disk_critical_bytes"`
	DiskTerminateBytes   int64   `json:"disk_terminate_bytes"`
	OutputWarningBytes   int64   `json:"output_warning_bytes"`
	OutputCriticalBytes  int64   `json:"output_critical_bytes"`
	OutputTerminateBytes int64   `json:"output_terminate_bytes"`
	DurationWarningPercent float64 `json:"duration_warning_percent"`
	DurationCriticalPercent float64 `json:"duration_critical_percent"`
	DurationTerminatePercent float64 `json:"duration_terminate_percent"`
}

// DefaultThresholds defines standard resource policy warning bands.
var DefaultThresholds = ThresholdConfig{
	CPUWarningPercent:      75.0,
	CPUCriticalPercent:     90.0,
	CPUTerminatePercent:    95.0,
	MemoryWarningPercent:   75.0,
	MemoryCriticalPercent:  90.0,
	MemoryTerminatePercent: 95.0,
	DiskWarningBytes:       50 * 1024 * 1024,  // 50 MB
	DiskCriticalBytes:      90 * 1024 * 1024,  // 90 MB
	DiskTerminateBytes:     100 * 1024 * 1024, // 100 MB
	OutputWarningBytes:     10 * 1024 * 1024,  // 10 MB
	OutputCriticalBytes:    18 * 1024 * 1024,  // 18 MB
	OutputTerminateBytes:   20 * 1024 * 1024,  // 20 MB
	DurationWarningPercent: 75.0,
	DurationCriticalPercent: 90.0,
	DurationTerminatePercent: 95.0,
}

// ResourceWatcher passive checker to analyze stats and verify bounds.
type ResourceWatcher struct {
	mu                 sync.RWMutex
	thresholds         ThresholdConfig
	bus                *events.Bus
	adapter            runtime.RuntimeAdapter
	terminationHandler func(ctx context.Context, id string, reason string)
}

// NewResourceWatcher initializes a ResourceWatcher.
func NewResourceWatcher(bus *events.Bus, adapter runtime.RuntimeAdapter, thresholds ThresholdConfig) *ResourceWatcher {
	return &ResourceWatcher{
		bus:        bus,
		adapter:    adapter,
		thresholds: thresholds,
	}
}

// RegisterTerminationHandler registers callback for terminating violated sessions.
func (rw *ResourceWatcher) RegisterTerminationHandler(fn func(ctx context.Context, id string, reason string)) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.terminationHandler = fn
}

// Watch checks container stats and evaluates against thresholds.
func (rw *ResourceWatcher) Watch(ctx context.Context, sess *types.SandboxSession) error {
	cID := sess.GetContainerID()
	if cID == "" {
		return nil
	}

	stats, err := rw.adapter.GetContainerStats(ctx, cID)
	if err != nil {
		return err
	}

	// Update stats inside session
	sess.SetStats(stats)

	rw.mu.RLock()
	thresh := rw.thresholds
	termHandler := rw.terminationHandler
	rw.mu.RUnlock()

	// 1. Memory Check
	if sess.GetMemoryLimitBytes() > 0 {
		memUsed := float64(stats.MemoryUsageBytes)
		memLimit := float64(sess.GetMemoryLimitBytes())
		memPercent := (memUsed / memLimit) * 100.0

		if memPercent >= thresh.MemoryTerminatePercent {
			rw.emitViolation(sess, "Memory", memPercent, "Critical", "exceeded termination limit")
			if termHandler != nil {
				go termHandler(ctx, sess.ID, "Memory limit exceeded termination threshold")
			}
		} else if memPercent >= thresh.MemoryCriticalPercent {
			rw.emitViolation(sess, "Memory", memPercent, "Critical", "exceeded critical threshold")
		} else if memPercent >= thresh.MemoryWarningPercent {
			rw.emitViolation(sess, "Memory", memPercent, "Warning", "exceeded warning threshold")
		}
	}

	// 2. CPU Check
	if stats.CPUPercentage >= thresh.CPUTerminatePercent {
		rw.emitViolation(sess, "CPU", stats.CPUPercentage, "Critical", "exceeded termination limit")
		if termHandler != nil {
			go termHandler(ctx, sess.ID, "CPU usage exceeded termination threshold")
		}
	} else if stats.CPUPercentage >= thresh.CPUCriticalPercent {
		rw.emitViolation(sess, "CPU", stats.CPUPercentage, "Critical", "exceeded critical threshold")
	} else if stats.CPUPercentage >= thresh.CPUWarningPercent {
		rw.emitViolation(sess, "CPU", stats.CPUPercentage, "Warning", "exceeded warning threshold")
	}

	// 3. Duration Check
	totalAllocated := sess.ExpiresAt.Sub(sess.CreatedAt)
	if totalAllocated > 0 {
		elapsed := time.Since(sess.CreatedAt)
		durPercent := (float64(elapsed) / float64(totalAllocated)) * 100.0

		if durPercent >= thresh.DurationTerminatePercent {
			rw.emitViolation(sess, "Duration", durPercent, "Critical", "exceeded termination limit")
			if termHandler != nil {
				go termHandler(ctx, sess.ID, "Execution duration exceeded termination threshold")
			}
		} else if durPercent >= thresh.DurationCriticalPercent {
			rw.emitViolation(sess, "Duration", durPercent, "Critical", "exceeded critical threshold")
		} else if durPercent >= thresh.DurationWarningPercent {
			rw.emitViolation(sess, "Duration", durPercent, "Warning", "exceeded warning threshold")
		}
	}

	// 4. Output Size Check
	if stats.OutputSizeBytes >= thresh.OutputTerminateBytes {
		rw.emitViolation(sess, "OutputSize", float64(stats.OutputSizeBytes), "Critical", "exceeded termination limit")
		if termHandler != nil {
			go termHandler(ctx, sess.ID, "Output size exceeded termination threshold")
		}
	} else if stats.OutputSizeBytes >= thresh.OutputCriticalBytes {
		rw.emitViolation(sess, "OutputSize", float64(stats.OutputSizeBytes), "Critical", "exceeded critical threshold")
	} else if stats.OutputSizeBytes >= thresh.OutputWarningBytes {
		rw.emitViolation(sess, "OutputSize", float64(stats.OutputSizeBytes), "Warning", "exceeded warning threshold")
	}

	return nil
}

func (rw *ResourceWatcher) emitViolation(sess *types.SandboxSession, metric string, val float64, severity string, desc string) {
	if rw.bus == nil {
		return
	}
	rw.bus.Publish(events.Event{
		Type:           events.ResourceThresholdExceeded,
		SandboxID:      sess.ID,
		JobID:          sess.JobID,
		Timestamp:      time.Now(),
		LifecycleState: string(sess.GetState()),
		Severity:       severity,
		Origin:         "watcher",
		Payload: map[string]any{
			"metric":      metric,
			"value":       val,
			"description": desc,
		},
	})

	if severity == "Critical" && desc == "exceeded termination limit" {
		rw.bus.Publish(events.Event{
			Type:      events.LimitExceeded,
			SandboxID: sess.ID,
			JobID:     sess.JobID,
			Timestamp: time.Now(),
			Payload:   desc,
		})
		rw.bus.Publish(events.Event{
			Type:      events.ResourceViolation,
			SandboxID: sess.ID,
			JobID:     sess.JobID,
			Timestamp: time.Now(),
			Payload:   desc,
		})
	}
}
