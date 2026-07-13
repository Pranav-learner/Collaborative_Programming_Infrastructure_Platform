package heartbeat

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"cpip/internal/queue/config"
	"cpip/internal/queue/events"
	"cpip/internal/queue/metrics"
	"cpip/internal/queue/registry"
	"cpip/internal/queue/types"
)

// Monitor runs background checks to detect heartbeat timeouts of workers.
type Monitor struct {
	cfg     config.Config
	reg     *registry.Registry
	bus     *events.Bus
	metrics metrics.Recorder
	log     *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewMonitor constructs a Heartbeat Monitor.
func NewMonitor(
	cfg config.Config,
	reg *registry.Registry,
	bus *events.Bus,
	rec metrics.Recorder,
	log *slog.Logger,
) *Monitor {
	if log == nil {
		log = slog.Default()
	}
	return &Monitor{
		cfg:     cfg,
		reg:     reg,
		bus:     bus,
		metrics: rec,
		log:     log.With("subsystem", "heartbeat"),
	}
}

// Heartbeat registers a heartbeat for a worker, updating its health to healthy.
func (m *Monitor) Heartbeat(ctx context.Context, workerID string) error {
	w, err := m.reg.Get(workerID)
	if err != nil {
		return err
	}

	// Update in registry.
	if err := m.reg.UpdateHeartbeat(workerID, types.HealthHealthy); err != nil {
		return err
	}

	// If the worker was not previously healthy, publish a recovery event.
	if w.Health != types.HealthHealthy && w.Health != types.HealthUnknown {
		m.log.Info("worker recovered health", "worker_id", workerID)
		m.metrics.WorkerRecovered()
		m.bus.Publish(events.Event{
			Type:      events.WorkerRecovered,
			WorkerID:  workerID,
			Timestamp: time.Now(),
		})
	}

	m.metrics.HeartbeatReceived()
	m.bus.Publish(events.Event{
		Type:      events.WorkerHeartbeat,
		WorkerID:  workerID,
		Timestamp: time.Now(),
	})

	return nil
}

// Start launches the background monitoring loop.
func (m *Monitor) Start(ctx context.Context) {
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.wg.Add(1)
	go m.run()
}

// Stop cancels the monitor check loop and blocks until it exits.
func (m *Monitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func (m *Monitor) run() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.HeartbeatCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkWorkers()
		}
	}
}

func (m *Monitor) checkWorkers() {
	workers := m.reg.List()
	now := time.Now()

	for _, w := range workers {
		if w.State == types.WorkerOffline {
			continue
		}

		idleTime := now.Sub(w.LastHeartbeat)
		if idleTime > m.cfg.HeartbeatTimeout {
			m.log.Warn("worker heartbeat timeout detected",
				"worker_id", w.ID,
				"idle_seconds", idleTime.Seconds(),
				"last_seen", w.LastHeartbeat,
			)

			// Transition worker to Offline.
			if err := m.reg.UpdateState(w.ID, types.WorkerOffline); err != nil {
				m.log.Error("failed to transition timed-out worker to offline", "worker_id", w.ID, "err", err)
				continue
			}

			// Update health to Unhealthy.
			_ = m.reg.UpdateHeartbeat(w.ID, types.HealthUnhealthy)

			// Update metrics.
			m.metrics.HeartbeatTimeout()
			m.metrics.WorkerOffline()

			// Publish offline event.
			m.bus.Publish(events.Event{
				Type:      events.WorkerOffline,
				WorkerID:  w.ID,
				Reason:    types.ErrHeartbeatTimeout.Error(),
				Timestamp: now,
			})
		} else if idleTime > m.cfg.HeartbeatInterval*2 && w.Health == types.HealthHealthy {
			// Degraded check (missed a couple of heartbeats).
			_ = m.reg.UpdateHeartbeat(w.ID, types.HealthDegraded)
			m.log.Warn("worker health degraded (missed heartbeat)", "worker_id", w.ID, "idle_seconds", idleTime.Seconds())
		}
	}
}
