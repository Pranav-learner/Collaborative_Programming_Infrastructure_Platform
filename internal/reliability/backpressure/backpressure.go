package backpressure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/reliability/events"
	"cpip/internal/reliability/metrics"
)

// ErrBackpressureShed is returned when requests are rejected to maintain system stability.
var ErrBackpressureShed = errors.New("backpressure active; request shed due to high system load")

// Priority thresholds.
const (
	PriorityLow    = 0
	PriorityNormal = 1
	PriorityHigh   = 2
)

// BackpressureManager tracks queue limits, execution speeds, and sheds low-priority traffic.
type BackpressureManager struct {
	mu            sync.Mutex
	maxQueueSize  int64
	activeTasks   int64
	slowThreshold time.Duration
	latencies     []time.Duration
	latencyLimit  time.Duration

	bus           *events.Bus
	metrics       metrics.Recorder
}

func NewBackpressureManager(maxQueue int64, slowThreshold, latencyLimit time.Duration, bus *events.Bus, rec metrics.Recorder) *BackpressureManager {
	return &BackpressureManager{
		maxQueueSize:  maxQueue,
		slowThreshold: slowThreshold,
		latencyLimit:  latencyLimit,
		latencies:     make([]time.Duration, 0),
		bus:           bus,
		metrics:       rec,
	}
}

// Acquire requests execution permission. Low priority jobs are shed first if latency/queue limits are reached.
func (bm *BackpressureManager) Acquire(ctx context.Context, priority int) error {
	active := atomic.LoadInt64(&bm.activeTasks)
	avgLatency := bm.AverageLatency()

	// High load threshold detections
	isLoadHigh := active >= bm.maxQueueSize
	isSystemSlow := avgLatency > bm.latencyLimit && len(bm.latencies) >= 5

	if isLoadHigh || isSystemSlow {
		// High priority jobs get a pass unless absolute limit is reached
		if priority == PriorityHigh && active < bm.maxQueueSize+5 {
			atomic.AddInt64(&bm.activeTasks, 1)
			return nil
		}

		if bm.metrics != nil {
			bm.metrics.Inc(metrics.MetricBackpressureSheds)
		}

		if bm.bus != nil {
			bm.bus.Publish(events.Event{
				Type:      events.BackpressureActivated,
				Timestamp: time.Now(),
				Detail:    fmt.Sprintf("Backpressure activated. Active: %d, Avg Latency: %v, Priority: %d", active, avgLatency, priority),
			})
		}
		return ErrBackpressureShed
	}

	atomic.AddInt64(&bm.activeTasks, 1)
	return nil
}

// Release registers task duration and decrements active workers.
func (bm *BackpressureManager) Release(duration time.Duration) {
	atomic.AddInt64(&bm.activeTasks, -1)

	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Track sliding window of last 50 latencies
	bm.latencies = append(bm.latencies, duration)
	if len(bm.latencies) > 50 {
		bm.latencies = bm.latencies[1:]
	}
}

// AverageLatency calculates execution latencies in current window.
func (bm *BackpressureManager) AverageLatency() time.Duration {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if len(bm.latencies) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range bm.latencies {
		total += d
	}
	return total / time.Duration(len(bm.latencies))
}

// ActiveTasks returns current task load.
func (bm *BackpressureManager) ActiveTasks() int64 {
	return atomic.LoadInt64(&bm.activeTasks)
}
