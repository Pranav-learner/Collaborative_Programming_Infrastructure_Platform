// Package manager is the composition root of the Distributed Coordination module
// and the Coordination SDK the rest of the platform depends on. It constructs and
// wires every cluster service (registry, membership, cluster, heartbeat, health,
// discovery, leader election, locks, replication) over a single pluggable
// backend, and exposes them behind one cohesive Coordinator facade.
//
// Business services depend on THIS package (or the interfaces it hands out), never
// on Redis or any backend primitive. The design realizes the module's layering:
//
//	Business Services → Coordination SDK (Coordinator) → Cluster Services → backend.Backend → Redis / etcd (future)
//
// A Coordinator owns this process's node identity: Start joins the cluster,
// begins heartbeating, and launches the health/membership loops; Stop leaves
// gracefully and releases everything.
package manager

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/cluster"
	"cpip/internal/coordination/config"
	"cpip/internal/coordination/discovery"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/health"
	"cpip/internal/coordination/heartbeat"
	"cpip/internal/coordination/keys"
	"cpip/internal/coordination/leader"
	"cpip/internal/coordination/locks"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/membership"
	"cpip/internal/coordination/metrics"
	"cpip/internal/coordination/registry"
	"cpip/internal/coordination/replication"
	"cpip/internal/coordination/types"
	"cpip/internal/id"
)

// Coordinator is the wired coordination module facade.
type Coordinator struct {
	cfg     config.Config
	kb      keys.Builder
	backend backend.Backend
	nodeID  string

	mu   sync.Mutex
	self *types.Node

	registry    *registry.Registry
	membership  *membership.Manager
	cluster     *cluster.Manager
	heartbeat   *heartbeat.Manager
	health      *health.Manager
	discovery   *discovery.Manager
	leader      *leader.Manager
	locks       *locks.Manager
	replication *replication.Replicator

	bus *events.Bus
	rec metrics.Recorder
	log *logger.Logger

	ownsBackend bool
	syncCancel  context.CancelFunc
	syncDone    chan struct{}
	started     bool
}

// Params configures a Coordinator. Only Config is required. When Backend is nil a
// self-contained in-memory backend is constructed (valid for a single node and
// tests); pass backend.NewRedis(...) for a multi-node deployment.
type Params struct {
	Config  config.Config
	Backend backend.Backend
	Events  *events.Bus
	Metrics metrics.Recorder
	Logger  *slog.Logger
}

// New constructs and wires the entire module. Call Start after New to join the
// cluster and launch background loops.
func New(p Params) (*Coordinator, error) {
	cfg, err := p.Config.Validate()
	if err != nil {
		return nil, err
	}

	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	bus := p.Events
	if bus == nil {
		bus = events.NewBus()
	}

	be := p.Backend
	ownsBackend := false
	if be == nil {
		be = backend.NewMemory()
		ownsBackend = true
	}

	nodeID := cfg.Node.ID
	if nodeID == "" {
		nodeID = id.NewWithPrefix("node")
		cfg.Node.ID = nodeID
	}
	log := logger.New(p.Logger).With("node_id", nodeID, "cluster_id", cfg.ClusterID)
	kb := keys.New(cfg.KeyPrefix, cfg.ClusterID)

	reg := registry.New(registry.Params{Backend: be, Keys: kb, Events: bus, Metrics: rec, Logger: log})
	mem := membership.New(membership.Params{Registry: reg, Config: cfg, Events: bus, Metrics: rec, Logger: log})
	clu := cluster.New(cluster.Params{Registry: reg, Membership: mem, Config: cfg})
	hb := heartbeat.New(heartbeat.Params{Backend: be, Registry: reg, Keys: kb, Config: cfg.Heartbeat, Events: bus, Metrics: rec, Logger: log})
	hlth := health.New(health.Params{Registry: reg, Config: cfg.Heartbeat, Events: bus, Metrics: rec, Logger: log})
	disc := discovery.New(discovery.Params{Registry: reg, Health: hlth, Events: bus, Metrics: rec, Logger: log})
	ldr := leader.New(leader.Params{Backend: be, Keys: kb, Config: cfg.Leader, CandidateID: nodeID, Events: bus, Metrics: rec, Logger: log})
	lk := locks.New(locks.Params{Backend: be, Config: cfg.Lock, Keys: kb, NodeID: nodeID, Events: bus, Metrics: rec, Logger: log})
	repl := replication.New(replication.Params{Backend: be, Keys: kb, Config: cfg.Replication, NodeID: nodeID, Events: bus, Metrics: rec, Logger: log})

	return &Coordinator{
		cfg: cfg, kb: kb, backend: be, nodeID: nodeID,
		self:        buildSelf(cfg),
		registry:    reg,
		membership:  mem,
		cluster:     clu,
		heartbeat:   hb,
		health:      hlth,
		discovery:   disc,
		leader:      ldr,
		locks:       lk,
		replication: repl,
		bus:         bus,
		rec:         rec,
		log:         log,
		ownsBackend: ownsBackend,
	}, nil
}

func buildSelf(cfg config.Config) *types.Node {
	n := &types.Node{
		ID:             cfg.Node.ID,
		Name:           cfg.Node.Name,
		Address:        cfg.Node.Address,
		Role:           cfg.Node.Role,
		Region:         cfg.Node.Region,
		Zone:           cfg.Node.Zone,
		Capabilities:   cfg.Node.Capabilities,
		RuntimeVersion: cfg.Node.RuntimeVersion,
		Status:         types.StatusJoining,
		Health:         types.HealthUnknown,
		Metadata:       cfg.Node.Metadata,
	}
	if n.Name == "" {
		n.Name = n.ID
	}
	if n.Address == "" {
		n.Address = "local://" + n.ID
	}
	return n
}

// --- Lifecycle ---

// Start joins this node to the cluster, begins heartbeating, and launches the
// health, membership-sync, and replication loops. Idempotent.
func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = true
	c.mu.Unlock()

	if err := c.backend.Ping(ctx); err != nil {
		return err
	}

	joined, err := c.membership.Join(ctx, c.self)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.self = joined
	c.mu.Unlock()

	c.replication.Start(ctx)
	c.heartbeat.StartPublishing(ctx, c.nodeID)
	c.heartbeat.StartMonitor(ctx)
	c.health.Start(ctx)
	c.startSyncLoop(ctx)

	c.log.Backend(ctx, "coordinator_started", nil)
	return nil
}

func (c *Coordinator) startSyncLoop(ctx context.Context) {
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.syncCancel = cancel
	c.syncDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(c.cfg.Discovery.RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				if err := c.cluster.Sync(loopCtx); err != nil {
					c.log.Membership(loopCtx, "sync_error", "", err)
				}
			}
		}
	}()
}

// Stop leaves the cluster gracefully and shuts down every subsystem.
func (c *Coordinator) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = false
	cancel, done := c.syncCancel, c.syncDone
	c.syncCancel = nil
	c.syncDone = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}

	c.heartbeat.Stop()
	c.health.Stop()
	c.leader.StopAll()
	_ = c.replication.Close()

	// Graceful leave on a context detached from any cancelled parent.
	leaveCtx, lc := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	_ = c.membership.Leave(leaveCtx, c.nodeID)
	lc()

	c.bus.Close()
	if c.ownsBackend {
		return c.backend.Close()
	}
	return nil
}

// --- Subsystem accessors (the seams business services depend on) ---

// Cluster returns the Cluster Manager.
func (c *Coordinator) Cluster() *cluster.Manager { return c.cluster }

// Registry returns the Node Registry.
func (c *Coordinator) Registry() *registry.Registry { return c.registry }

// Membership returns the Membership Manager.
func (c *Coordinator) Membership() *membership.Manager { return c.membership }

// Heartbeat returns the Heartbeat Manager.
func (c *Coordinator) Heartbeat() *heartbeat.Manager { return c.heartbeat }

// Health returns the Node Health Manager.
func (c *Coordinator) Health() *health.Manager { return c.health }

// Discovery returns the Service Discovery manager.
func (c *Coordinator) Discovery() *discovery.Manager { return c.discovery }

// Leader returns the Leader Election Manager.
func (c *Coordinator) Leader() *leader.Manager { return c.leader }

// Locks returns the Distributed Lock Service.
func (c *Coordinator) Locks() *locks.Manager { return c.locks }

// Replication returns the State Replication Framework.
func (c *Coordinator) Replication() *replication.Replicator { return c.replication }

// Events returns the cluster event bus (future modules subscribe here).
func (c *Coordinator) Events() *events.Bus { return c.bus }

// Backend exposes the underlying coordination backend (for advanced/diagnostic use).
func (c *Coordinator) Backend() backend.Backend { return c.backend }

// NodeID returns this node's identity.
func (c *Coordinator) NodeID() string { return c.nodeID }

// Self returns a snapshot of this node's record.
func (c *Coordinator) Self() *types.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.self.Clone()
}

// --- Convenience API (the minimal Coordination SDK surface) ---

// Register admits an external node to the cluster (e.g. an agent registering a
// worker it manages).
func (c *Coordinator) Register(ctx context.Context, n *types.Node) (*types.Node, error) {
	return c.cluster.Register(ctx, n)
}

// Deregister removes a node from the cluster.
func (c *Coordinator) Deregister(ctx context.Context, nodeID string) error {
	return c.cluster.Deregister(ctx, nodeID)
}

// UpdateLoad updates this node's reported load so discovery can rank it.
func (c *Coordinator) UpdateLoad(ctx context.Context, load types.Load) error {
	updated, err := c.registry.Mutate(ctx, c.nodeID, func(n *types.Node) { n.Load = load })
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.self = updated
	c.mu.Unlock()
	return nil
}

// Discover finds nodes matching a query, ranked least-loaded first.
func (c *Coordinator) Discover(ctx context.Context, q discovery.Query) ([]*types.Node, error) {
	return c.discovery.Discover(ctx, q)
}

// AcquireLock acquires a distributed lock on a resource.
func (c *Coordinator) AcquireLock(ctx context.Context, resource string, opts *locks.Options) (*locks.Lock, error) {
	return c.locks.Acquire(ctx, resource, opts)
}

// Campaign starts (or returns) this node's campaign for leadership of a scope.
func (c *Coordinator) Campaign(ctx context.Context, scope string) *leader.Election {
	return c.leader.Campaign(ctx, scope)
}

// IsLeader reports whether this node currently leads a scope.
func (c *Coordinator) IsLeader(scope string) bool { return c.leader.IsLeader(scope) }

// LeaderID returns the authoritative current leader of a scope.
func (c *Coordinator) LeaderID(ctx context.Context, scope string) (string, error) {
	return c.leader.Leader(ctx, scope)
}

// ClusterState returns a full snapshot including the cluster-scope leader.
func (c *Coordinator) ClusterState(ctx context.Context) types.ClusterState {
	leaderID, _ := c.leader.Leader(ctx, leader.DefaultScope)
	return c.cluster.State(ctx, leaderID)
}

// ClusterStats returns aggregate cluster statistics.
func (c *Coordinator) ClusterStats(ctx context.Context) types.ClusterStats {
	return c.ClusterState(ctx).Stats()
}

// Health reports whether the backend is reachable and summarizes node health.
func (c *Coordinator) HealthReport(ctx context.Context) HealthReport {
	hr := HealthReport{
		BackendUp: c.backend.Ping(ctx) == nil,
		Nodes:     c.registry.Count(),
		ByHealth:  c.health.Summary(ctx),
	}
	hr.Healthy = hr.BackendUp
	return hr
}

// HealthReport is the module's aggregate health snapshot.
type HealthReport struct {
	Healthy   bool
	BackendUp bool
	Nodes     int
	ByHealth  map[types.Health]int
}
