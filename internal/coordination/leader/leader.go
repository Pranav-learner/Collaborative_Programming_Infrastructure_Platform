package leader

import (
	"context"
	"sync"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/keys"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/metrics"
)

// DefaultScope is the leadership domain used when a caller doesn't name one — the
// cluster-wide "who runs singleton control-plane duties" election.
const DefaultScope = "cluster"

// Manager is the Leader Election Framework entry point. It mints and tracks one
// Election per scope, so different singleton responsibilities (cleanup owner,
// scheduler owner, rebalancer owner) can each elect an independent leader over
// the same cluster.
type Manager struct {
	elector     Elector
	cfg         config.Leader
	candidateID string
	bus         *events.Bus
	rec         metrics.Recorder
	log         *logger.Logger

	mu        sync.Mutex
	elections map[string]*Election
}

// Params configures a Manager. If Elector is nil a LeaseElector over Backend+Keys
// is constructed (the default lease-based election).
type Params struct {
	Backend     backend.Backend
	Keys        keys.Builder
	Elector     Elector
	Config      config.Leader
	CandidateID string
	Events      *events.Bus
	Metrics     metrics.Recorder
	Logger      *logger.Logger
}

// New constructs a Leader Election Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	el := p.Elector
	if el == nil {
		el = NewLeaseElector(p.Backend, p.Keys)
	}
	return &Manager{
		elector:     el,
		cfg:         p.Config,
		candidateID: p.CandidateID,
		bus:         p.Events,
		rec:         rec,
		log:         p.Logger.With("subsystem", "leader"),
		elections:   make(map[string]*Election),
	}
}

// Election returns the Election for a scope, creating it (stopped) on first use.
func (m *Manager) Election(scope string) *Election {
	if scope == "" {
		scope = DefaultScope
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.elections[scope]; ok {
		return e
	}
	e := newElection(scope, m.candidateID, m.elector, m.cfg, m.bus, m.rec, m.log)
	m.elections[scope] = e
	return e
}

// Campaign returns the Election for a scope and starts its loop (the candidate
// begins competing for leadership immediately). Idempotent per scope.
func (m *Manager) Campaign(ctx context.Context, scope string) *Election {
	e := m.Election(scope)
	e.Start(ctx)
	return e
}

// IsLeader reports whether this node leads a scope.
func (m *Manager) IsLeader(scope string) bool {
	if scope == "" {
		scope = DefaultScope
	}
	m.mu.Lock()
	e, ok := m.elections[scope]
	m.mu.Unlock()
	return ok && e.IsLeader()
}

// Leader returns the authoritative current leader of a scope.
func (m *Manager) Leader(ctx context.Context, scope string) (string, error) {
	return m.Election(scope).Leader(ctx)
}

// StopAll halts every election (resigning where this node leads).
func (m *Manager) StopAll() {
	m.mu.Lock()
	elections := make([]*Election, 0, len(m.elections))
	for _, e := range m.elections {
		elections = append(elections, e)
	}
	m.elections = make(map[string]*Election)
	m.mu.Unlock()
	for _, e := range elections {
		e.Stop()
	}
}
