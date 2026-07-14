// Package cluster implements the Cluster Manager: the cohesive control surface
// for cluster membership. It composes the Node Registry (storage) and the
// Membership Manager (lifecycle) into one facade that business services use to
// register nodes, remove them, look them up, and read cluster state, metadata,
// and statistics.
//
// The Cluster Manager is intentionally leader-agnostic: it accepts the current
// leader id as a parameter when building a ClusterState snapshot, so it never
// depends on the leader-election package (keeping the dependency graph acyclic).
// The Coordination Manager supplies the leader id when it assembles a full view.
// "Future scaling support" is expressed through the same snapshot/stats surface a
// scheduler or autoscaler will consume.
package cluster

import (
	"context"

	"cpip/internal/coordination/config"
	"cpip/internal/coordination/membership"
	"cpip/internal/coordination/registry"
	"cpip/internal/coordination/types"
)

// Manager is the Cluster Manager.
type Manager struct {
	reg    *registry.Registry
	member *membership.Manager
	cfg    config.Config
}

// Params configures a Manager.
type Params struct {
	Registry   *registry.Registry
	Membership *membership.Manager
	Config     config.Config
}

// New constructs a Cluster Manager.
func New(p Params) *Manager {
	return &Manager{reg: p.Registry, member: p.Membership, cfg: p.Config}
}

// Register admits a node to the cluster (join or reconnect).
func (m *Manager) Register(ctx context.Context, n *types.Node) (*types.Node, error) {
	return m.member.Join(ctx, n)
}

// Deregister gracefully removes a node from the cluster.
func (m *Manager) Deregister(ctx context.Context, nodeID string) error {
	return m.member.Leave(ctx, nodeID)
}

// Node returns a single node by ID.
func (m *Manager) Node(ctx context.Context, id string) (*types.Node, error) {
	return m.reg.Get(ctx, id)
}

// Nodes returns a snapshot of all known nodes.
func (m *Manager) Nodes() []*types.Node { return m.reg.List() }

// NodesByRole returns all nodes of a role.
func (m *Manager) NodesByRole(role types.Role) []*types.Node {
	return m.reg.Filter(func(n *types.Node) bool { return n.Role == role })
}

// ActiveNodes returns all nodes currently Active.
func (m *Manager) ActiveNodes() []*types.Node {
	return m.reg.Filter(func(n *types.Node) bool { return n.Status == types.StatusActive })
}

// Size returns the number of known nodes.
func (m *Manager) Size() int { return m.reg.Count() }

// State returns a point-in-time snapshot of the cluster, stamped with the given
// leader id (supplied by the Coordination Manager).
func (m *Manager) State(ctx context.Context, leaderID string) types.ClusterState {
	return m.member.Snapshot(ctx, leaderID)
}

// Stats returns aggregate cluster statistics.
func (m *Manager) Stats(ctx context.Context, leaderID string) types.ClusterStats {
	return m.State(ctx, leaderID).Stats()
}

// Metadata returns cluster-level metadata.
func (m *Manager) Metadata() map[string]string { return m.cfg.Metadata }

// ClusterID returns the cluster identity.
func (m *Manager) ClusterID() string { return m.cfg.ClusterID }

// Version returns the membership logical clock (increments on every change).
func (m *Manager) Version() uint64 { return m.member.Version() }

// Sync runs a membership anti-entropy pass (pull peers' registrations).
func (m *Manager) Sync(ctx context.Context) error { return m.member.Sync(ctx) }
