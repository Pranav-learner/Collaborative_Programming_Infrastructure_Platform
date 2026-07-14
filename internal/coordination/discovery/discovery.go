// Package discovery implements Service Discovery: finding the right node(s) for a
// piece of work by role, capability, placement, health, and load. It is the read
// side of the cluster — business services (execution scheduler, gateway, runtime
// manager) ask "who can run a Python job with a free slot, least loaded?" and get
// a ranked answer without knowing how membership is stored.
//
// Discovery is stateless over the Node Registry (itself a fast local cache kept
// fresh by membership sync), so queries are cheap and never hit the backend on
// the hot path. Results are health-filtered and load-ranked. A Resolver seam is
// provided for a future DNS/SRV integration without changing callers.
package discovery

import (
	"context"
	"sort"
	"time"

	"cpip/internal/coordination/events"
	"cpip/internal/coordination/health"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/metrics"
	"cpip/internal/coordination/registry"
	"cpip/internal/coordination/types"
)

// Query selects and ranks nodes. Zero-valued fields are ignored.
type Query struct {
	Role         types.Role
	Capabilities []string // ALL must be present
	Region       string
	Zone         string
	// MinHealth is the worst acceptable health (default: HealthDegraded — exclude
	// only Unhealthy/Unknown). Set HealthHealthy to require pristine nodes.
	MinHealth types.Health
	// RequireSchedulable filters to nodes that can accept new work.
	RequireSchedulable bool
	// ExcludeNodeIDs removes specific nodes (e.g. the caller itself, or a node
	// that just failed).
	ExcludeNodeIDs []string
	// Limit caps the number of results (0 = all).
	Limit int
}

// Manager is the Service Discovery manager.
type Manager struct {
	reg    *registry.Registry
	health *health.Manager
	bus    *events.Bus
	rec    metrics.Recorder
	log    *logger.Logger
	now    func() time.Time
}

// Params configures a Manager.
type Params struct {
	Registry *registry.Registry
	Health   *health.Manager
	Events   *events.Bus
	Metrics  metrics.Recorder
	Logger   *logger.Logger
}

// New constructs a Service Discovery manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		reg:    p.Registry,
		health: p.Health,
		bus:    p.Events,
		rec:    rec,
		log:    p.Logger.With("subsystem", "discovery"),
		now:    time.Now,
	}
}

// Discover returns nodes matching the query, ranked least-loaded first. The
// ranking makes repeated Discover calls naturally spread work across the cluster.
func (m *Manager) Discover(ctx context.Context, q Query) ([]*types.Node, error) {
	start := m.now()
	m.rec.IncCounter(metrics.MetricDiscoveryQuery, map[string]string{"role": string(q.Role)})

	minHealth := q.MinHealth
	if minHealth == "" {
		minHealth = types.HealthDegraded
	}
	excluded := make(map[string]struct{}, len(q.ExcludeNodeIDs))
	for _, id := range q.ExcludeNodeIDs {
		excluded[id] = struct{}{}
	}

	candidates := m.reg.Filter(func(n *types.Node) bool {
		if _, skip := excluded[n.ID]; skip {
			return false
		}
		if q.Role != "" && n.Role != q.Role {
			return false
		}
		if q.Region != "" && n.Region != q.Region {
			return false
		}
		if q.Zone != "" && n.Zone != q.Zone {
			return false
		}
		for _, cap := range q.Capabilities {
			if !n.HasCapability(cap) {
				return false
			}
		}
		if !healthAtLeast(n.Health, minHealth) {
			return false
		}
		if q.RequireSchedulable && !n.IsSchedulable() {
			return false
		}
		return true
	})

	// Rank: least-loaded first, then lowest latency, then stable by ID.
	sort.SliceStable(candidates, func(i, j int) bool {
		li, lj := candidates[i].Load.Score(), candidates[j].Load.Score()
		if li != lj {
			return li < lj
		}
		return candidates[i].ID < candidates[j].ID
	})
	if q.Limit > 0 && len(candidates) > q.Limit {
		candidates = candidates[:q.Limit]
	}

	dur := metrics.ObserveDuration(m.rec, metrics.MetricDiscoveryLatency, start, nil)
	if len(candidates) == 0 {
		m.rec.IncCounter(metrics.MetricDiscoveryMiss, nil)
		m.log.Discovery(ctx, string(q.Role), 0, dur, types.ErrNoCandidates)
		return nil, types.ErrNoCandidates
	}
	m.rec.IncCounter(metrics.MetricDiscoveryHit, nil)
	m.log.Discovery(ctx, string(q.Role), len(candidates), dur, nil)
	m.bus.Emit(events.ServiceDiscovered, "discovery", func(e *events.Event) {
		e.Payload = map[string]any{"role": string(q.Role), "matched": len(candidates)}
	})
	return candidates, nil
}

// Select returns the single best node for a query (least-loaded healthy match),
// the primary primitive a scheduler uses to place one unit of work.
func (m *Manager) Select(ctx context.Context, q Query) (*types.Node, error) {
	q.Limit = 1
	nodes, err := m.Discover(ctx, q)
	if err != nil {
		return nil, err
	}
	return nodes[0], nil
}

// ByCapability returns schedulable nodes advertising a capability.
func (m *Manager) ByCapability(ctx context.Context, capability string) ([]*types.Node, error) {
	return m.Discover(ctx, Query{Capabilities: []string{capability}, RequireSchedulable: true})
}

// ByRole returns nodes of a role (any health at least Degraded).
func (m *Manager) ByRole(ctx context.Context, role types.Role) ([]*types.Node, error) {
	return m.Discover(ctx, Query{Role: role})
}

// LeastLoaded returns the least-loaded schedulable node matching a role.
func (m *Manager) LeastLoaded(ctx context.Context, role types.Role) (*types.Node, error) {
	return m.Select(ctx, Query{Role: role, RequireSchedulable: true})
}

// Resolver is the seam for a future DNS/SRV-based discovery integration. It maps
// a logical service name to concrete node addresses. No implementation ships in
// this stage; the interface lets a resolver be injected without touching callers.
type Resolver interface {
	Resolve(ctx context.Context, service string) ([]string, error)
}

// healthRank orders health states for comparison.
func healthRank(h types.Health) int {
	switch h {
	case types.HealthHealthy:
		return 3
	case types.HealthDegraded:
		return 2
	case types.HealthUnhealthy:
		return 1
	default:
		return 0
	}
}

func healthAtLeast(have, min types.Health) bool { return healthRank(have) >= healthRank(min) }
