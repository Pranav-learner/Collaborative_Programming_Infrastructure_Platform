// Package types defines the leaf domain model, enums, and shared value objects
// for the Distributed Coordination & Cluster State module (Stage 4 Module 4). It
// has NO dependencies on other coordination packages so every subsystem can
// import it without creating cycles.
//
// The central model is Node: the description of one member of the cluster. A Node
// record is replicated across the cluster (via the coordination backend) and is
// the unit the Cluster Manager, Node Registry, Membership Manager, Health
// Manager, and Service Discovery all operate on.
package types

import (
	"sort"
	"time"
)

// Role classifies what a node does in the cluster. Roles are free-form strings so
// new node kinds can appear without a code change; the constants name the roles
// the platform ships today.
type Role string

const (
	RoleCoordinator Role = "coordinator" // runs coordination/control-plane duties
	RoleWorker      Role = "worker"      // executes jobs (execution/sandbox)
	RoleGateway     Role = "gateway"     // terminates client connections
	RoleRuntime     Role = "runtime"     // hosts language runtimes
	RoleStorage     Role = "storage"     // owns object storage / persistence
	RoleGeneric     Role = "generic"     // unspecialized member
)

// Status is a node's membership status — where it sits in the join/leave
// lifecycle. It is distinct from Health (which is about liveness quality).
type Status string

const (
	// StatusJoining: the node has registered but not yet been confirmed live.
	StatusJoining Status = "joining"
	// StatusActive: a full, healthy participant.
	StatusActive Status = "active"
	// StatusSuspect: heartbeats are overdue; liveness is in doubt (SWIM-style).
	StatusSuspect Status = "suspect"
	// StatusDraining: administratively removed from scheduling; finishing work.
	StatusDraining Status = "draining"
	// StatusLeft: performed a graceful leave.
	StatusLeft Status = "left"
	// StatusDead: expired (heartbeat timeout) and evicted.
	StatusDead Status = "dead"
)

// Health is the liveness/quality assessment computed by the Health Manager from
// heartbeat freshness, latency, and load.
type Health string

const (
	HealthUnknown   Health = "unknown"
	HealthHealthy   Health = "healthy"
	HealthDegraded  Health = "degraded"
	HealthUnhealthy Health = "unhealthy"
)

// Load is a node's current resource/work utilization. It drives load-aware
// service discovery and future autoscaling. Values are best-effort snapshots
// reported by the node itself.
type Load struct {
	CPUPercent        float64 `json:"cpu_percent"`        // 0..100
	MemoryPercent     float64 `json:"memory_percent"`     // 0..100
	ActiveJobs        int64   `json:"active_jobs"`        // running executions
	QueuedJobs        int64   `json:"queued_jobs"`        // pending executions
	ActiveConnections int64   `json:"active_connections"` // open client conns
	Capacity          int64   `json:"capacity"`           // max concurrent units of work
}

// Score returns a normalized 0..1 busyness estimate used to pick the least-loaded
// node. It blends CPU, memory, and job saturation; lower is less busy.
func (l Load) Score() float64 {
	cpu := clamp01(l.CPUPercent / 100)
	mem := clamp01(l.MemoryPercent / 100)
	var jobs float64
	if l.Capacity > 0 {
		jobs = clamp01(float64(l.ActiveJobs) / float64(l.Capacity))
	}
	// Weighted toward job saturation since that is the schedulable resource.
	return clamp01(0.3*cpu + 0.2*mem + 0.5*jobs)
}

// HasFreeCapacity reports whether the node can accept at least one more unit of
// work (unknown capacity is treated as available).
func (l Load) HasFreeCapacity() bool {
	return l.Capacity <= 0 || l.ActiveJobs < l.Capacity
}

// Stats accumulates lifetime counters for a node (observability, not scheduling).
type Stats struct {
	HeartbeatsSent     int64 `json:"heartbeats_sent"`
	HeartbeatsMissed   int64 `json:"heartbeats_missed"`
	JobsProcessed      int64 `json:"jobs_processed"`
	Reconnects         int64 `json:"reconnects"`
	LeadershipsWon     int64 `json:"leaderships_won"`
	LocksHeld          int64 `json:"locks_held"`
	ReplicationApplied int64 `json:"replication_applied"`
}

// Node is the canonical description of one cluster member.
type Node struct {
	// Identity.
	ID      string `json:"id"`      // stable, unique, immutable
	Name    string `json:"name"`    // human-friendly name
	Address string `json:"address"` // host:port reachable by peers

	// Placement & classification.
	Role         Role     `json:"role"`
	Region       string   `json:"region"`
	Zone         string   `json:"zone"`
	Capabilities []string `json:"capabilities"` // e.g. ["python","gpu","docker"]

	// Lifecycle & liveness.
	Status    Status    `json:"status"`
	Health    Health    `json:"health"`
	Heartbeat time.Time `json:"heartbeat"` // last heartbeat emitted by the node
	LastSeen  time.Time `json:"last_seen"` // last time the cluster observed it

	// Incarnation disambiguates re-registrations of the same ID (split
	// registration / reconnect): a higher incarnation supersedes a lower one, so
	// a stale record can never overwrite a fresh one. SWIM-style monotonic epoch.
	Incarnation uint64 `json:"incarnation"`

	// Runtime & utilization.
	RuntimeVersion string `json:"runtime_version"`
	Load           Load   `json:"load"`
	Stats          Stats  `json:"stats"`

	// Extensible attributes.
	Metadata map[string]string `json:"metadata,omitempty"`
	// Labels are reserved for future label-selector-based scheduling (k8s-style).
	Labels map[string]string `json:"labels,omitempty"`

	// Bookkeeping.
	JoinedAt  time.Time `json:"joined_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// HasCapability reports whether the node advertises capability c.
func (n *Node) HasCapability(c string) bool {
	for _, cap := range n.Capabilities {
		if cap == c {
			return true
		}
	}
	return false
}

// IsSchedulable reports whether the node may receive new work: active, healthy
// (or degraded), and with free capacity.
func (n *Node) IsSchedulable() bool {
	if n.Status != StatusActive {
		return false
	}
	if n.Health == HealthUnhealthy || n.Health == HealthUnknown {
		return false
	}
	return n.Load.HasFreeCapacity()
}

// IsAlive reports whether the node is a live member (not left/dead).
func (n *Node) IsAlive() bool {
	return n.Status != StatusLeft && n.Status != StatusDead
}

// Clone returns a deep copy so callers can mutate without racing the stored node.
func (n *Node) Clone() *Node {
	if n == nil {
		return nil
	}
	cp := *n
	if n.Capabilities != nil {
		cp.Capabilities = append([]string(nil), n.Capabilities...)
	}
	if n.Metadata != nil {
		cp.Metadata = make(map[string]string, len(n.Metadata))
		for k, v := range n.Metadata {
			cp.Metadata[k] = v
		}
	}
	if n.Labels != nil {
		cp.Labels = make(map[string]string, len(n.Labels))
		for k, v := range n.Labels {
			cp.Labels[k] = v
		}
	}
	return &cp
}

// Supersedes reports whether n is a newer view of the same node than other,
// using incarnation first and UpdatedAt as a tiebreaker. This is the conflict
// resolution rule for membership synchronization.
func (n *Node) Supersedes(other *Node) bool {
	if other == nil {
		return true
	}
	if n.Incarnation != other.Incarnation {
		return n.Incarnation > other.Incarnation
	}
	return n.UpdatedAt.After(other.UpdatedAt)
}

// ClusterState is a point-in-time snapshot of the whole cluster.
type ClusterState struct {
	ClusterID string            `json:"cluster_id"`
	LeaderID  string            `json:"leader_id"`
	Nodes     []*Node           `json:"nodes"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Snapshot  time.Time         `json:"snapshot"`
	// Version increments on every membership change (a logical clock for the
	// membership set, used to detect staleness between snapshots).
	Version uint64 `json:"version"`
}

// Stats derives aggregate counts from the snapshot.
func (c ClusterState) Stats() ClusterStats {
	s := ClusterStats{ClusterID: c.ClusterID, Version: c.Version}
	byRole := map[Role]int{}
	byStatus := map[Status]int{}
	byHealth := map[Health]int{}
	for _, n := range c.Nodes {
		s.Total++
		byRole[n.Role]++
		byStatus[n.Status]++
		byHealth[n.Health]++
		if n.Status == StatusActive {
			s.Active++
		}
		if n.Health == HealthHealthy {
			s.Healthy++
		}
		if n.IsSchedulable() {
			s.Schedulable++
		}
	}
	s.ByRole = byRole
	s.ByStatus = byStatus
	s.ByHealth = byHealth
	return s
}

// ClusterStats is an aggregate summary of cluster membership.
type ClusterStats struct {
	ClusterID   string
	Version     uint64
	Total       int
	Active      int
	Healthy     int
	Schedulable int
	ByRole      map[Role]int
	ByStatus    map[Status]int
	ByHealth    map[Health]int
}

// SortNodesByID sorts a node slice by ID for deterministic output.
func SortNodesByID(nodes []*Node) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
}

func clamp01(f float64) float64 {
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}
