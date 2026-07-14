// Package health implements the Node Health Manager: it assesses each node's
// liveness quality (availability, latency, heartbeat freshness, resource/load
// summary, connection state) and maintains a Health verdict distinct from
// membership Status. Where the Heartbeat Manager decides IF a node is alive, the
// Health Manager decides HOW WELL it is doing — the signal load-aware discovery
// and future autoscaling consume.
//
// Health is derived, not stored authoritatively: an Assess pass recomputes each
// node's verdict from heartbeat age, reported load, and observed latency, and
// emits HealthChanged only on transitions. Detailed per-subsystem telemetry is a
// future stage; the LatencyProbe seam is provided so richer probes can plug in.
package health

import (
	"context"
	"sync"
	"time"

	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/metrics"
	"cpip/internal/coordination/registry"
	"cpip/internal/coordination/types"
)

// Report is a detailed health assessment of one node.
type Report struct {
	NodeID       string
	Status       types.Status
	Health       types.Health
	HeartbeatAge time.Duration
	Available    bool
	LatencyMs    float64
	Load         types.Load
	LoadScore    float64
	ConnState    string // "connected" | "degraded" | "unreachable"
	AssessedAt   time.Time
}

// Manager is the Node Health Manager.
type Manager struct {
	reg *registry.Registry
	cfg config.Heartbeat
	bus *events.Bus
	rec metrics.Recorder
	log *logger.Logger
	now func() time.Time

	mu        sync.Mutex
	latencyMs map[string]float64 // EWMA latency per node
	cancel    context.CancelFunc
	done      chan struct{}
}

// Params configures a Manager.
type Params struct {
	Registry *registry.Registry
	Config   config.Heartbeat
	Events   *events.Bus
	Metrics  metrics.Recorder
	Logger   *logger.Logger
}

// New constructs a Node Health Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		reg:       p.Registry,
		cfg:       p.Config,
		bus:       p.Events,
		rec:       rec,
		log:       p.Logger.With("subsystem", "health"),
		now:       time.Now,
		latencyMs: make(map[string]float64),
	}
}

// ReportLatency folds a new latency sample into a node's EWMA. Probes (heartbeat
// RTT, RPC timings) feed this; discovery reads it to avoid slow nodes.
func (m *Manager) ReportLatency(nodeID string, sample time.Duration) {
	ms := float64(sample.Microseconds()) / 1000.0
	m.mu.Lock()
	if cur, ok := m.latencyMs[nodeID]; ok {
		m.latencyMs[nodeID] = 0.8*cur + 0.2*ms // EWMA
	} else {
		m.latencyMs[nodeID] = ms
	}
	m.mu.Unlock()
}

func (m *Manager) latency(nodeID string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latencyMs[nodeID]
}

// Evaluate computes a node's Health verdict from heartbeat freshness, membership
// status, and load. It is a pure function of its inputs (deterministic, testable).
func (m *Manager) Evaluate(n *types.Node, now time.Time) types.Health {
	switch n.Status {
	case types.StatusDead, types.StatusLeft:
		return types.HealthUnhealthy
	case types.StatusSuspect, types.StatusDraining:
		return types.HealthDegraded
	}
	age := now.Sub(n.Heartbeat)
	if age >= m.cfg.Expiry {
		return types.HealthUnhealthy
	}
	if age >= m.cfg.Timeout {
		return types.HealthDegraded
	}
	// Overloaded but alive nodes are Degraded, not Unhealthy.
	if n.Load.Score() >= 0.9 || !n.Load.HasFreeCapacity() {
		return types.HealthDegraded
	}
	return types.HealthHealthy
}

// NodeHealth returns a detailed report for one node.
func (m *Manager) NodeHealth(ctx context.Context, nodeID string) (Report, error) {
	n, err := m.reg.Get(ctx, nodeID)
	if err != nil {
		return Report{}, err
	}
	return m.report(n, m.now().UTC()), nil
}

func (m *Manager) report(n *types.Node, now time.Time) Report {
	age := now.Sub(n.Heartbeat)
	h := m.Evaluate(n, now)
	conn := "connected"
	switch h {
	case types.HealthDegraded:
		conn = "degraded"
	case types.HealthUnhealthy:
		conn = "unreachable"
	}
	return Report{
		NodeID:       n.ID,
		Status:       n.Status,
		Health:       h,
		HeartbeatAge: age,
		Available:    h == types.HealthHealthy || h == types.HealthDegraded,
		LatencyMs:    m.latency(n.ID),
		Load:         n.Load,
		LoadScore:    n.Load.Score(),
		ConnState:    conn,
		AssessedAt:   now,
	}
}

// Assess recomputes every node's Health and writes back any transitions, emitting
// HealthChanged for each. It is the periodic pass the background loop runs.
func (m *Manager) Assess(ctx context.Context) {
	now := m.now().UTC()
	for _, n := range m.reg.List() {
		newHealth := m.Evaluate(n, now)
		if newHealth == n.Health {
			continue
		}
		old := n.Health
		if _, err := m.reg.Mutate(ctx, n.ID, func(nn *types.Node) { nn.Health = newHealth }); err != nil {
			continue
		}
		m.log.Slog().DebugContext(ctx, "health_changed", "node_id", n.ID, "from", string(old), "to", string(newHealth))
		m.bus.Emit(events.HealthChanged, "health", func(e *events.Event) {
			e.NodeID = n.ID
			e.Payload = map[string]any{"from": string(old), "to": string(newHealth)}
		})
	}
}

// Summary aggregates the current health distribution across the cluster.
func (m *Manager) Summary(_ context.Context) map[types.Health]int {
	out := map[types.Health]int{}
	for _, n := range m.reg.List() {
		out[n.Health]++
	}
	return out
}

// Start launches the periodic assessment loop.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.cancel = cancel
	m.done = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(m.cfg.MonitorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				m.Assess(loopCtx)
			}
		}
	}()
}

// Stop halts the assessment loop.
func (m *Manager) Stop() {
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.cancel = nil
	m.done = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}
