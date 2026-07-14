package monitor

import (
	"context"
	"time"

	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/metrics"
	"cpip/internal/sandbox/registry"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

type ResourceMonitor struct {
	reg         *registry.SandboxRegistry
	adapter     runtime.RuntimeAdapter
	bus         *events.Bus
	rec         metrics.Recorder
	interval    time.Duration
	stopChan    chan struct{}
	violationFn func(ctx context.Context, id string, reason string)
}

func NewResourceMonitor(reg *registry.SandboxRegistry, adapter runtime.RuntimeAdapter, bus *events.Bus, rec metrics.Recorder, interval time.Duration) *ResourceMonitor {
	return &ResourceMonitor{
		reg:      reg,
		adapter:  adapter,
		bus:      bus,
		rec:      rec,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

func (rm *ResourceMonitor) RegisterViolationHandler(fn func(ctx context.Context, id string, reason string)) {
	rm.violationFn = fn
}

func (rm *ResourceMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(rm.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rm.CheckResources(ctx)
			case <-rm.stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (rm *ResourceMonitor) Stop() {
	close(rm.stopChan)
}

func (rm *ResourceMonitor) CheckResources(ctx context.Context) {
	for _, sess := range rm.reg.List() {
		cID := sess.GetContainerID()
		if cID == "" {
			continue
		}

		state := sess.GetState()
		if state != types.StateExecuting && state != types.StateReady {
			continue
		}

		stats, err := rm.adapter.GetContainerStats(ctx, cID)
		if err != nil {
			continue
		}

		sess.SetStats(stats)

		memLimit := sess.GetMemoryLimitBytes()
		if memLimit > 0 && stats.MemoryUsageBytes > memLimit {
			rm.handleViolation(ctx, sess.ID, "Memory limit exceeded")
		}
	}
}

func (rm *ResourceMonitor) handleViolation(ctx context.Context, id string, reason string) {
	sess, err := rm.reg.Get(id)
	if err != nil {
		return
	}

	rm.bus.Publish(events.Event{
		Type:      events.LimitExceeded,
		SandboxID: id,
		JobID:     sess.JobID,
		Timestamp: time.Now(),
		Payload:   reason,
	})

	rm.bus.Publish(events.Event{
		Type:      events.ResourceViolation,
		SandboxID: id,
		JobID:     sess.JobID,
		Timestamp: time.Now(),
		Payload:   reason,
	})

	if rm.violationFn != nil {
		go rm.violationFn(ctx, id, reason)
	}
}
