// Package heartbeat implements the Heartbeat Manager: nodes publish periodic
// liveness beats, and a monitor detects overdue nodes, transitions them Suspect,
// and evicts them when their lease expires.
//
// Two signals back a node's liveness: its record's Heartbeat timestamp in the
// Node Registry (updated on every beat and propagated cluster-wide by membership
// sync), and a short-TTL backend key that lets a future backend-native watch
// expire a dead node without a scan. The monitor's decision uses the timestamp so
// it behaves identically on the in-memory and Redis backends.
//
// Liveness thresholds come from config: a node overdue past Timeout is Suspect;
// past Expiry it is evicted (Dead). Any node's monitor may reap any dead peer —
// eviction is idempotent, so concurrent monitors converge without coordination.
package heartbeat

import (
	"context"
	"sync"
	"time"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/keys"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/metrics"
	"cpip/internal/coordination/registry"
	"cpip/internal/coordination/types"
)

// Manager is the Heartbeat Manager.
type Manager struct {
	backend backend.Backend
	reg     *registry.Registry
	kb      keys.Builder
	cfg     config.Heartbeat
	bus     *events.Bus
	rec     metrics.Recorder
	log     *logger.Logger
	now     func() time.Time

	mu        sync.Mutex
	monCancel context.CancelFunc
	monDone   chan struct{}
	pubCancel context.CancelFunc
	pubDone   chan struct{}
}

// Params configures a Manager.
type Params struct {
	Backend  backend.Backend
	Registry *registry.Registry
	Keys     keys.Builder
	Config   config.Heartbeat
	Events   *events.Bus
	Metrics  metrics.Recorder
	Logger   *logger.Logger
}

// New constructs a Heartbeat Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		backend: p.Backend,
		reg:     p.Registry,
		kb:      p.Keys,
		cfg:     p.Config,
		bus:     p.Events,
		rec:     rec,
		log:     p.Logger.With("subsystem", "heartbeat"),
		now:     time.Now,
	}
}

// Beat publishes a single heartbeat for nodeID: it refreshes the backend liveness
// key (TTL = Expiry) and stamps the node's record Healthy/Active, recovering a
// node that was transiently Suspect.
func (m *Manager) Beat(ctx context.Context, nodeID string) error {
	now := m.now().UTC()
	if err := m.backend.Set(ctx, m.kb.HeartbeatKey(nodeID), now.Format(time.RFC3339Nano), m.cfg.Expiry); err != nil {
		m.rec.IncCounter(metrics.MetricBackendError, map[string]string{"op": "beat"})
		return err
	}
	_, err := m.reg.Mutate(ctx, nodeID, func(n *types.Node) {
		n.Heartbeat = now
		n.LastSeen = now
		n.Stats.HeartbeatsSent++
		if n.Status == types.StatusSuspect {
			n.Status = types.StatusActive
		}
		if n.Status == types.StatusActive {
			n.Health = types.HealthHealthy
		}
	})
	if err != nil {
		return err
	}
	m.rec.IncCounter(metrics.MetricHeartbeatSent, nil)
	m.bus.Emit(events.HeartbeatReceived, "heartbeat", func(e *events.Event) { e.NodeID = nodeID; e.Origin = nodeID })
	return nil
}

// StartPublishing launches a background loop that beats for nodeID every
// configured Interval until StopPublishing or ctx cancellation.
func (m *Manager) StartPublishing(ctx context.Context, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pubDone != nil {
		return
	}
	pubCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.pubCancel = cancel
	m.pubDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(m.cfg.Interval)
		defer ticker.Stop()
		// Beat immediately so liveness is asserted without waiting a full interval.
		_ = m.Beat(pubCtx, nodeID)
		for {
			select {
			case <-pubCtx.Done():
				return
			case <-ticker.C:
				rctx, c := context.WithTimeout(context.Background(), m.cfg.Interval)
				_ = m.Beat(rctx, nodeID)
				c()
			}
		}
	}()
}

// StartMonitor launches the background monitor that scans for overdue nodes.
func (m *Manager) StartMonitor(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.monDone != nil {
		return
	}
	monCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.monCancel = cancel
	m.monDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(m.cfg.MonitorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-monCtx.Done():
				return
			case <-ticker.C:
				m.scan(monCtx)
			}
		}
	}()
}

// CheckOnce runs a single monitor pass (exposed for tests and manual sweeps).
func (m *Manager) CheckOnce(ctx context.Context) { m.scan(ctx) }

// scan evaluates every known node against the liveness thresholds.
func (m *Manager) scan(ctx context.Context) {
	now := m.now().UTC()
	for _, n := range m.reg.List() {
		if !n.IsAlive() {
			continue
		}
		age := now.Sub(n.Heartbeat)
		switch {
		case age >= m.cfg.Expiry:
			m.expire(ctx, n)
		case age >= m.cfg.Timeout:
			m.suspect(ctx, n)
		}
	}
}

func (m *Manager) suspect(ctx context.Context, n *types.Node) {
	if n.Status == types.StatusSuspect {
		return
	}
	_, err := m.reg.Mutate(ctx, n.ID, func(nn *types.Node) {
		if nn.Status == types.StatusActive {
			nn.Status = types.StatusSuspect
			nn.Health = types.HealthDegraded
			nn.Stats.HeartbeatsMissed++
		}
	})
	if err != nil {
		return
	}
	m.log.Heartbeat(ctx, "suspect", n.ID, nil)
	m.bus.Emit(events.NodeSuspected, "heartbeat", func(e *events.Event) { e.NodeID = n.ID })
}

func (m *Manager) expire(ctx context.Context, n *types.Node) {
	// Evict the dead node cluster-wide. Remove is idempotent, so concurrent
	// monitors on other nodes reaching the same conclusion is harmless.
	_, _ = m.backend.Delete(ctx, m.kb.HeartbeatKey(n.ID))
	if err := m.reg.Remove(ctx, n.ID); err != nil {
		return
	}
	m.rec.IncCounter(metrics.MetricHeartbeatExpired, nil)
	m.log.Heartbeat(ctx, "expired", n.ID, types.ErrHeartbeatTimeout)
	m.bus.Emit(events.HeartbeatExpired, "heartbeat", func(e *events.Event) { e.NodeID = n.ID })
	m.bus.Emit(events.NodeLeft, "heartbeat", func(e *events.Event) {
		e.NodeID = n.ID
		e.Payload = map[string]any{"graceful": false, "reason": "heartbeat_timeout"}
	})
}

// StopPublishing halts only the publishing loop and waits for it to exit.
func (m *Manager) StopPublishing() {
	m.mu.Lock()
	cancel, done := m.pubCancel, m.pubDone
	m.pubCancel = nil
	m.pubDone = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// Stop halts all background loops (publisher + monitor) and waits for them.
func (m *Manager) Stop() {
	m.StopPublishing()
	m.mu.Lock()
	cancel, done := m.monCancel, m.monDone
	m.monCancel = nil
	m.monDone = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}
