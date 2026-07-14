# Distributed Coordination & Cluster State (Stage 4 · Module 4)

The coordination module is the platform's **cluster control plane**. It tracks
which nodes exist, whether they are alive and healthy, who leads each singleton
responsibility, who holds which distributed lock, and how ephemeral cluster state
replicates between nodes — all behind one `Coordinator` facade. Business services
depend on that facade (the **Coordination SDK**), never on Redis or any backend
primitive.

```
Business Services  (execution scheduler · gateway · runtime manager)
        │
        ▼
Coordination SDK  (manager.Coordinator)         ← single cluster abstraction
        │
        ├── Cluster Manager ── Membership Manager ── Node Registry
        ├── Heartbeat Manager · Health Manager · Service Discovery
        ├── Leader Election Framework  (pluggable Elector)
        ├── Distributed Lock Service   (fenced leases)
        └── State Replication Framework (CRDT-ready)
        │
        ▼
backend.Backend  (the ONLY seam a store sits behind)
        │
        ▼
Redis (today, via internal/cache/redis) · in-memory · etcd / Consul (future)
```

**This is not a consensus system.** There is one authoritative store (the
backend); leadership and locks are mutual exclusion over TTL'd keys with owner
fencing — the pattern behind Kubernetes Leases, Consul sessions, and etcd
election, *not* Raft/Paxos. The framework is designed so a consensus-backed
`Elector` or `Backend` can be dropped in later without touching business logic.

## Packages

| Package | Responsibility |
|---|---|
| `types` | Leaf domain model: `Node`, roles, status, health, load, cluster state/stats, canonical errors. |
| `config` | Configuration surface (heartbeat, leader, lock, discovery, replication) with validation. |
| `backend` | **The decoupling seam.** `Backend` interface + self-contained `Memory` impl + `Redis` adapter over `internal/cache/redis`. |
| `keys` | Namespaced backend key/channel construction under one prefix. |
| `registry` | **Node Registry** — concurrent-safe, write-through node store with a monotonic (Supersedes-guarded) local cache. |
| `membership` | **Membership Manager** — join/leave/reconnect, incarnation assignment, snapshots, anti-entropy sync, versioned change events. |
| `cluster` | **Cluster Manager** — cohesive facade over registry + membership; cluster state, metadata, statistics. |
| `heartbeat` | **Heartbeat Manager** — periodic beat publishing, overdue detection, Suspect transition, expiry eviction. |
| `health` | **Node Health Manager** — derives a Health verdict (availability, latency EWMA, load) distinct from membership status. |
| `discovery` | **Service Discovery** — role/capability/placement/health filtering, load-aware ranking; DNS `Resolver` seam. |
| `leader` | **Leader Election Framework** — pluggable `Elector` (lease-based default), per-scope `Election` runtime with campaign/renew/resign/transfer/loss. |
| `locks` | **Distributed Lock Service** — SET-NX acquisition, token-fenced release/renew, watchdog auto-renew, Redlock-compatible validity. |
| `replication` | **State Replication Framework** — domain-partitioned pub/sub replication with a CRDT-ready `Merger` seam (LWW default). |
| `events` | **Cluster Event Bus** — typed lifecycle events; future modules subscribe. |
| `metrics` | `Recorder` interface + in-memory / no-op recorders. |
| `logger` | Structured slog hooks for every subsystem. |
| `middleware` | Context propagation (node id, request id) and a tracing-hook seam. |
| `manager` | **Coordination Manager** — composition root & Coordination SDK facade. |

## Node model

Each node carries: ID, name, address, role, region, zone, capabilities, status,
health, heartbeat, last-seen, **incarnation** (monotonic epoch for
reconnect/split-registration resolution), runtime version, current load,
lifetime stats, metadata, and reserved labels. `Node.Supersedes` (incarnation,
then updated-at) is the conflict-resolution rule for every merge.

## Cluster architecture

Every process runs one `Coordinator`. On `Start` it joins itself to the cluster,
begins heartbeating, and launches the monitor/health/sync loops. All nodes share
one `Backend`; each node keeps a **local cache** of the registry that membership
sync keeps eventually-consistent with the backend's authoritative member set.
Reads (discovery, lookup) hit the local cache; writes go through to the backend
and are Supersedes-guarded so a stale write never wins.

## Membership lifecycle

```
             Join (incarnation=1)          overloaded / heartbeat late
   (none) ─────────────────────▶ Active ───────────────────────▶ Suspect
      ▲                            │  ▲                              │
      │ Reconnect (incarnation++)  │  │  heartbeat resumes           │ heartbeat
      └────────────────────────────┘  └──────────────────────────────┘  expiry
                                   │                                      ▼
                          Leave (graceful)                             Dead
                                   ▼                                 (evicted)
                                 Left  ──────────────────────────────▶ (evicted)
```

Every transition advances a membership **version** (logical clock) and emits
`NodeJoined` / `NodeLeft` / `MembershipChanged` on the event bus.

## Leader election workflow

```
follower ── Campaign (SET NX leader key = self, TTL=lease) ──▶ won? ──yes──▶ LEADER
   ▲                                                             │              │
   │ retry every RetryInterval                                   no             │ renew every
   │                                                             ▼              │ RenewInterval
   └──────────────────────── observe current leader ◀───────────┘   CompareAndExpire(self)
                                                                              │
                              renew fails (lease lost / superseded) ──────────┘
                                          │
                                          ▼  transitionLost → OnLost callbacks → resume campaigning
```

Also supports **voluntary resign** and **leadership transfer** (atomic
CompareAndSwap of the leader key from → to). Pluggable via the `Elector`
interface — a future etcd/Raft elector implements the same five methods.

## Service discovery workflow

`Discover(Query)` → filter registry by role, capabilities (all required), region,
zone, minimum health, and schedulability → **rank least-loaded first** (by
`Load.Score`, then latency, then id) → return top-N. `Select` returns the single
best node — the primitive a scheduler uses to place one unit of work. Repeated
calls naturally spread load.

## Heartbeat lifecycle

```
node ── Beat every Interval ──▶ backend liveness key (TTL=Expiry) + registry timestamp
monitor (every MonitorInterval) ── scan nodes:
        age ≥ Timeout  → Suspect (Degraded)
        age ≥ Expiry   → evict (Dead): delete key, Remove from registry, emit HeartbeatExpired + NodeLeft
```

Eviction is idempotent, so any node's monitor may reap any dead peer and
concurrent monitors converge without coordination.

## State replication workflow

```
node A ── Broadcast(domain, key, payload, version) ──▶ backend pub/sub channel(domain)
node B ── receiveLoop ──▶ decode ──▶ dedup (drop own origin) ──▶ Merger(current, incoming)
                                                                 └─▶ handlers + StateReplicated event
anti-entropy: every SyncInterval, registered StateProviders re-broadcast local state
```

Domains: presence, room, execution, worker, cluster, node. Conflict rule is
last-write-wins (version, then timestamp); the `Merger` interface is the seam a
future CRDT engine plugs into without changing publishers or subscribers.

## Distributed lock workflow

```
Acquire ── SET NX lock key = <node:random token>, TTL=lease ──▶ held (fencing token returned)
  │  (blocks up to AcquireTimeout, spinning every RetryInterval; deadlock-free via TTL)
Renew   ── CompareAndExpire(key, token) ── extends lease iff still owner
Release ── CompareAndDelete(key, token) ── deletes iff still owner  (fencing)
Watchdog (AutoRenew) ── renews at AutoRenewFraction of the lease until released
```

Token fencing prevents the "expired-then-deleted-by-someone-else" bug; the design
is Redlock-compatible so a future multi-node quorum drops in behind the same API.

## Concurrency strategy

- **Backend atomics** (SET NX, CompareAnd*) are the serialization points for locks
  and leadership — a single winner is guaranteed without application locking.
- **Registry** writes are write-through with **Supersedes-guarded** cache commits,
  so concurrent registrations/heartbeats/refreshes converge monotonically and a
  stale record never overwrites a fresh one. Backend I/O happens outside the cache
  lock, so thousands of readers never serialize behind a network call.
- **Refresh** re-checks each eviction candidate against the backend, so anti-entropy
  running concurrently with live registrations never drops a fresh node.
- **Best-effort buses** (event bus, replication, pub/sub) drop for slow consumers
  rather than block a heartbeat or election.
- Verified with `go test -race`: 200 concurrent registrations, 100 concurrent
  discoveries, 200-way lock contention (single-holder invariant), 3-candidate
  elections (single-leader invariant), and cross-node replication.

## Usage

```go
c, _ := manager.New(manager.Params{
    Config:  cfg,                       // set cfg.Node.{ID,Address,Role,Capabilities}
    Backend: backend.NewRedis(redisClient), // omit → in-memory (single node / tests)
})
_ = c.Start(ctx)                        // join, heartbeat, monitor, health, sync
defer c.Stop(ctx)                       // graceful leave + shutdown

// Elect a leader for a singleton responsibility.
e := c.Campaign(ctx, "scheduler")
e.OnElected(func(ctx context.Context) { /* start scheduling */ })

// Discover the least-loaded python worker and guard a critical section.
node, _ := c.Discovery().LeastLoaded(ctx, types.RoleWorker)
_ = c.Locks().WithLock(ctx, "rebalance", nil, func(ctx context.Context) error { return nil })
```

## Future integration points

- **Kubernetes:** map `Node` ↔ `v1.Node`/Lease; run a leader-elected controller via
  `Campaign`; feed pod capacity into `Load` and `Labels` for label-selector discovery.
- **etcd:** implement `backend.Backend` over etcd (lease + txn) or a native
  `leader.Elector` over etcd election — no business-logic change.
- **Consul:** implement `Backend` over the Consul KV + session API; map health
  checks onto the Health Manager; use the DNS `discovery.Resolver` seam for Consul DNS.
- **Distributed Scheduler:** consume `discovery.Discover` for placement and
  `leader.Election` to run a single active scheduler; subscribe to `MembershipChanged`.
- **Autoscaler:** drive off `ClusterStats`, `Load` aggregates, and health/heartbeat
  events; scale via the same registry the coordinator already exposes.
- **Service Mesh:** publish node addresses/capabilities through discovery; bridge
  the event bus to mesh control-plane xDS.

Explicitly **out of scope** (future stages): Raft/Paxos consensus, Kubernetes
controllers, distributed scheduling, autoscaling, multi-region replication, and
service mesh — the framework is shaped to accept them without a rewrite.
