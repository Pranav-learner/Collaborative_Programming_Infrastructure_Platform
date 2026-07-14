// Package membership implements the Membership Manager: the join/leave/reconnect
// protocol that governs how a node enters and exits the cluster, plus membership
// validation, snapshots, and anti-entropy synchronization.
//
// Membership is layered on the Node Registry (durable store) and adds the
// lifecycle semantics: a join assigns an incarnation and marks the node Active;
// a reconnect bumps the incarnation so a returning node's fresh record supersedes
// any stale copy; a leave tombstones and evicts. Every change advances a logical
// version so peers can detect divergence, and is announced on the cluster event
// bus so future modules (scheduler, autoscaler) react without polling.
package membership

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/metrics"
	"cpip/internal/coordination/registry"
	"cpip/internal/coordination/types"
)

// Manager is the Membership Manager.
type Manager struct {
	reg     *registry.Registry
	cfg     config.Config
	bus     *events.Bus
	rec     metrics.Recorder
	log     *logger.Logger
	now     func() time.Time
	version atomic.Uint64
}

// Params configures a Manager.
type Params struct {
	Registry *registry.Registry
	Config   config.Config
	Events   *events.Bus
	Metrics  metrics.Recorder
	Logger   *logger.Logger
}

// New constructs a Membership Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		reg: p.Registry,
		cfg: p.Config,
		bus: p.Events,
		rec: rec,
		log: p.Logger.With("subsystem", "membership"),
		now: time.Now,
	}
}

// Validate performs structural validation of a node record before admission.
func (m *Manager) Validate(n *types.Node) error {
	if n == nil {
		return fmt.Errorf("%w: nil node", types.ErrInvalidNode)
	}
	if n.ID == "" {
		return fmt.Errorf("%w: empty node id", types.ErrInvalidNode)
	}
	if n.Address == "" {
		return fmt.Errorf("%w: node %s has no address", types.ErrInvalidNode, n.ID)
	}
	if n.Role == "" {
		return fmt.Errorf("%w: node %s has no role", types.ErrInvalidNode, n.ID)
	}
	return nil
}

// Join admits a node to the cluster. If the node is already known, Join treats it
// as a reconnect (incarnation bump), so a node that flapped can rejoin without a
// membership conflict. Returns the admitted record (with assigned incarnation).
func (m *Manager) Join(ctx context.Context, n *types.Node) (*types.Node, error) {
	if err := m.Validate(n); err != nil {
		return nil, err
	}
	node := n.Clone()
	now := m.now().UTC()

	existing, err := m.reg.Get(ctx, node.ID)
	reconnect := err == nil && existing != nil
	if reconnect {
		// A returning node must strictly supersede its stale record.
		node.Incarnation = existing.Incarnation + 1
		node.JoinedAt = existing.JoinedAt
		node.Stats = existing.Stats
		node.Stats.Reconnects++
	} else {
		if node.Incarnation == 0 {
			node.Incarnation = 1
		}
		node.JoinedAt = now
	}
	node.Status = types.StatusActive
	// A node admitted with a fresh heartbeat is healthy until proven otherwise by
	// the Health Manager; leaving it Unknown would make it invisible to discovery.
	node.Health = types.HealthHealthy
	node.Heartbeat = now
	node.LastSeen = now
	node.UpdatedAt = now

	if err := m.reg.Put(ctx, node); err != nil {
		return nil, err
	}
	m.bumpVersion()

	if reconnect {
		m.rec.IncCounter(metrics.MetricNodeReconnected, nil)
		m.log.Membership(ctx, "reconnected", node.ID, nil)
		m.emit(events.NodeJoined, node.ID, map[string]any{"reconnect": true, "incarnation": node.Incarnation})
	} else {
		m.rec.IncCounter(metrics.MetricNodeJoined, nil)
		m.log.Membership(ctx, "joined", node.ID, nil)
		m.emit(events.NodeJoined, node.ID, map[string]any{"incarnation": node.Incarnation})
	}
	m.emit(events.MembershipChanged, node.ID, map[string]any{"version": m.Version()})
	return node.Clone(), nil
}

// Reconnect re-admits a previously-suspected/expired node, bumping its
// incarnation so its record supersedes any stale copy still circulating.
func (m *Manager) Reconnect(ctx context.Context, n *types.Node) (*types.Node, error) {
	// Join already implements reconnect semantics when the node is known; calling
	// it here keeps a single admission path. For an unknown node this is a join.
	return m.Join(ctx, n)
}

// Leave performs a graceful departure: the node is tombstoned Left and evicted
// from the registry so it stops being discovered/scheduled.
func (m *Manager) Leave(ctx context.Context, nodeID string) error {
	if _, err := m.reg.Get(ctx, nodeID); err != nil {
		if errors.Is(err, types.ErrNodeNotFound) {
			return nil // idempotent: already gone
		}
		return err
	}
	if err := m.reg.Remove(ctx, nodeID); err != nil {
		return err
	}
	m.bumpVersion()
	m.rec.IncCounter(metrics.MetricNodeLeft, nil)
	m.log.Membership(ctx, "left", nodeID, nil)
	m.emit(events.NodeLeft, nodeID, map[string]any{"graceful": true})
	m.emit(events.MembershipChanged, nodeID, map[string]any{"version": m.Version()})
	return nil
}

// Snapshot returns a point-in-time view of the whole cluster.
func (m *Manager) Snapshot(_ context.Context, leaderID string) types.ClusterState {
	nodes := m.reg.List()
	return types.ClusterState{
		ClusterID: m.cfg.ClusterID,
		LeaderID:  leaderID,
		Nodes:     nodes,
		Metadata:  m.cfg.Metadata,
		Snapshot:  m.now().UTC(),
		Version:   m.Version(),
	}
}

// Sync runs anti-entropy: it refreshes the registry from the backend's
// authoritative membership set, pulling in nodes registered by other processes
// and evicting vanished ones. It advances the version and announces a membership
// change when the observed roster differs.
func (m *Manager) Sync(ctx context.Context) error {
	before := m.reg.Count()
	if err := m.reg.Refresh(ctx); err != nil {
		m.rec.IncCounter(metrics.MetricBackendError, map[string]string{"op": "membership_sync"})
		return err
	}
	if after := m.reg.Count(); after != before {
		m.bumpVersion()
		m.emit(events.MembershipChanged, "", map[string]any{"version": m.Version(), "size": after})
	}
	return nil
}

// Version returns the current membership logical clock.
func (m *Manager) Version() uint64 { return m.version.Load() }

func (m *Manager) bumpVersion() uint64 {
	v := m.version.Add(1)
	m.rec.IncCounter(metrics.MetricMembershipChange, nil)
	return v
}

func (m *Manager) emit(t events.Type, nodeID string, payload any) {
	m.bus.Emit(t, "membership", func(e *events.Event) {
		e.NodeID = nodeID
		e.Origin = m.cfg.Node.ID
		e.Payload = payload
	})
}
