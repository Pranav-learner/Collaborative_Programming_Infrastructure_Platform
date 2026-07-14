# Redis Distributed State & Caching — Stage 4 Module 2

> Production-grade distributed state and caching infrastructure for CPIP.
> **Business logic never touches Redis.** All ephemeral shared state flows
> through the Cache interface and the Distributed State Manager; Redis becomes
> the distributed state layer while PostgreSQL (Stage 4 Module 1) remains the
> durable system of record.

This module is the state fabric behind a multi-node CPIP deployment: the layer
that lets any node answer for any user's session, presence, room membership, and
hot data — the same job Redis does behind GitHub, Discord, Figma, and Slack.

---

## 1. Design Principle

```
Business Services            (collaboration, execution, sandbox, future APIs)
      │  depends only on interfaces
      ▼
Cache Interface  +  Distributed State Manager        (state.Manager facade)
      │
      ▼
Redis Adapter                (redis.Client: go-redis OR in-memory Emulator)
      │
      ▼
Redis
```

The `redis` package is the **only** package that imports `go-redis`. Every other
package depends on the narrow `redis.Client` interface, so the entire module runs
against the in-memory `Emulator` in tests and can be re-pointed at Redis Cluster
or Sentinel by changing one constructor.

---

## 2. Folder Tree

```
internal/cache/
├── config/          Configuration surface (Redis, pool, TTL, policy, lock, pubsub, replication)
├── types/           Canonical errors, Codec (JSON + CRC32), Item/Stats/Health models
├── keys/            Namespaced Redis key & channel construction (single source of key layout)
├── redis/           redis.Client interface + go-redis impl (real.go) + in-memory Emulator
├── logger/          Structured slog logging hooks
├── metrics/         Recorder interface + InMemory + Noop, all metric names
├── events/          In-process event bus (CacheHit/Miss/Set/… → future subscribers)
├── ttl/             TTL manager: default/per-cache, absolute/sliding, jitter, callbacks, reaper
├── policies/        Cache policy engine: aside / read-through / write-through / write-behind / refresh-ahead
├── registry/        Cache catalog: descriptors, atomic stats, hit-ratio, health
├── invalidation/    Manual / pattern / tag / bulk / event-driven cross-node invalidation
├── manager/         Cache Manager: the Cache interface + policies.Store implementation
├── pubsub/          Pub/Sub hub: topics, fan-out, backpressure, reconnect
├── locks/           Distributed lock manager: fencing tokens, watchdog, Redlock-ready
├── replication/     Eventual-consistency transport: LWW-ordered delta broadcast
├── presence/        Presence replication: cursor/typing/state/heartbeat/membership
├── sessions/        Distributed session store: multi-device, CAS concurrent updates
├── state/           Distributed State Manager: composition root + generic namespaced state
└── README.md
```

## 3. Files Created & Package Responsibilities

| Package | Responsibility |
|---|---|
| **config** | Typed configuration with `Default()` + `Validate()` (zero-value normalization, fail-fast on nonsense). |
| **types** | `ErrNil`, `ErrRedisUnavailable`, `ErrLockNotHeld`, … sentinel set; `Codec` (JSON, checksummed); `Item`, `Stats`, `Health`. |
| **keys** | `Builder` that namespaces every key/channel under one prefix — auditable, collision-free, re-shardable. |
| **redis** | `Client` interface (strings, hashes, sets, atomic CAS, pub/sub, scan, health). `Redis` (go-redis + Lua) and `Emulator` (faithful in-memory). |
| **logger** | slog wrapper emitting machine-parseable fields per subsystem. |
| **metrics** | `Recorder` (counter/gauge/histogram) + in-memory & no-op; centralized metric names. |
| **events** | Best-effort, nil-safe event `Bus` (handlers + buffered subscribers). |
| **ttl** | Resolves TTLs (default/per-cache/override), jitter, sliding vs absolute, and a reaper that fires expiration callbacks + `TTLExpired` events. |
| **policies** | Strategy engine over a `Store`; write-behind buffer with coalescing + workers; refresh-ahead with singleflight dedupe. |
| **registry** | Concurrent catalog of caches with atomic hot-path counters, derived hit-ratio, and health. |
| **invalidation** | Every eviction mode + cross-node broadcast with local-eviction hooks (future L1 caches). |
| **manager** | Wires everything behind the small `Cache` interface; implements `policies.Store`. |
| **pubsub** | One router per topic; bounded per-subscriber buffers; drop/backpressure policy; transparent reconnect. |
| **locks** | `SETNX`+token acquire, token-checked release/renew (Lua), auto-renew watchdog, clock-drift-adjusted validity. |
| **replication** | Versioned delta transport with LWW ordering and per-namespace listeners. |
| **presence** | Authoritative Redis records (heartbeat-TTL) + real-time replicated deltas + local materialized view + anti-entropy. |
| **sessions** | Create/lookup/renew/expire/invalidate; per-user device sets; optimistic-concurrency updates. |
| **state** | Composition root; subsystem accessors; generic `PutState/GetState/CompareAndSwapState/WatchState`; room membership. |

---

## 4. Cache Architecture

```
                     ┌─────────────────────────────────────────────┐
   Get/Set/Delete    │                Cache Manager                 │
  ───────────────►   │  ┌────────────┐   ┌──────────┐   ┌────────┐  │
                     │  │  Registry  │   │  Policy  │   │  TTL   │  │
                     │  │ stats/hlth │   │  Engine  │   │  mgr   │  │
                     │  └────────────┘   └────┬─────┘   └────────┘  │
                     │        ▲               │ RawGet/RawSet        │
                     │        │               ▼ (policies.Store)     │
                     │  ┌──────────────────────────────────────┐    │
                     │  │            manager (Store)            │    │
                     │  └───────────────────┬──────────────────┘    │
                     └──────────────────────┼───────────────────────┘
                                            ▼
                                    redis.Client  ──►  Redis
                                            ▲
                    invalidation ───────────┘  (SCAN/DEL, tag sets, pub/sub broadcast)
```

- **Registry** tracks per-cache strategy, TTL, live stats, health.
- **Policy Engine** decides *how* a Get/Set behaves (see §6) and calls back into
  the manager (`policies.Store`) for the raw Redis operation.
- **TTL Manager** resolves the effective lifetime (with jitter) and schedules
  local expiration callbacks.
- **Invalidation** handles every eviction path and cross-node coherence.

---

## 5. Distributed State Flow

```
 Node A                                   Redis (shared)                Node B
 ──────                                   ─────────────                 ──────
 state.PutState(ns,id,v) ───► SET state:ns:id ──────────────────► (durable-ish, TTL'd)
        │
        └─► replication.Broadcast ─► PUBLISH repl:state:ns ─────► replication.listen
                                                                    │  LWW check (version)
                                                                    ▼
                                                            apply → local view / handler
                                                                    │
                                                            (WS gateway fan-out to clients)
```

- **Authoritative truth** lives in Redis (any node can read it).
- **Real-time convergence** rides pub/sub deltas so nodes update in-memory views
  without polling.
- **Last-Writer-Wins** (monotonic version per `namespace|id`) discards stale or
  out-of-order deltas; a node ignores the echo of its own broadcast.
- **Anti-entropy** periodically re-reads Redis to heal deltas lost to a transient
  disconnect — the eventual-consistency safety net.

`CompareAndSwapState` provides cross-node **optimistic synchronization** (atomic
compare-and-set via Lua) for cases that need coordination without a lock.

---

## 6. Cache Policy Comparison

| Policy | Read (miss) | Write | Consistency | Best for | State |
|---|---|---|---|---|---|
| **Cache-Aside** | app loads & populates | cache only | lazy | general, read-heavy | ✅ |
| **Read-Through** | engine loads via `Loader`, populates, returns | cache only | lazy, transparent | hides load logic from callers | ✅ |
| **Write-Through** | cache | cache **and** `Writer` synchronously | strong (cache=store) | data that must never be stale | ✅ |
| **Write-Behind** | cache | cache now; `Writer` async (coalesced buffer + workers) | eventual write | write-heavy, bursty | ✅ (buffer flushes on close) |
| **Refresh-Ahead** | serve cached; async reload once `ratio·TTL` elapsed | cache | hot keys never expire under load | predictable hot keys | ✅ (singleflight-deduped) |

Selection is per-cache via `CacheSpec.Strategy`, defaulting to `config.Policy.Default`.
Write-behind and refresh-ahead ship as **working** implementations (not stubs):
write-behind coalesces last-writer-wins per key and degrades to synchronous
writes on buffer overflow so durability is never silently dropped; refresh-ahead
dedupes concurrent reloads.

---

## 7. Pub/Sub Workflow

```
RegisterTopic("chat")
     │
Subscribe("chat") ──► first subscriber starts the topic ROUTER goroutine
     │                        │
     │                        ├─ redis.Subscribe(channel)
     │                        ├─ pump: for msg := range sub.Channel()
     │                        │        fanout → each subscriber.send()
     │                        │             drop-on-backpressure OR bounded block
     │                        └─ on connection drop: reconnect w/ exp backoff
     ▼
Publish("chat", payload) ──► redis PUBLISH ──► all nodes' routers ──► subscribers
```

- **One router per active topic**, owning a single Redis subscription.
- **Backpressure isolation**: a per-subscription mutex serializes send/close so a
  slow subscriber can never send-on-closed-channel, block the router, or starve
  its peers; it just drops (or bounded-blocks) per policy.
- **Transparent reconnect** with exponential backoff up to `MaxReconnectBackoff`.
- **Redis Streams** (durable, replayable) is reserved via `TopicSpec.Durable`.

---

## 8. Distributed Lock Workflow

```
Acquire(resource):
   token = nodeID:random(16B)                       # unguessable fence token
   loop until AcquireTimeout:
       SET lock:resource token NX PX lease           # atomic mutual exclusion
       ok?  → return Lock{token, lease}              #   optionally start watchdog
       else → sleep RetryInterval                    # deadlock-free: bounded wait

Watchdog (AutoRenew):  every lease·AutoRenewFraction → CompareAndExtend(token)
Renew():   PEXPIRE only if GET == token  (Lua)       # can only extend what you own
Release(): DEL       only if GET == token  (Lua)     # can only release what you own
```

**Safety properties**

| Property | Mechanism |
|---|---|
| Mutual exclusion | `SET NX` → single owner |
| Deadlock freedom | every lease has a TTL; acquisition is time-bounded |
| Liveness (long work) | watchdog auto-renews honest holders |
| Fencing | caller-visible token; `ValidUntil()` is clock-drift-adjusted |
| Correctness on expiry | token-checked release/renew — never delete another owner's lock |

Single-instance today, **Redlock-compatible** by construction (token fencing +
drift math) so a multi-node quorum drops in behind the same `Lock` API.
`WithLock(resource, fn)` guards a critical section and releases even on panic.

---

## 9. Presence Replication Workflow

```
Announce/Cursor/Typing/Heartbeat(room,user)
     │
     ├─ HSET presence:room:user {state,cursor,typing,meta,hb,ver,node}  (authoritative)
     ├─ EXPIRE presence:room:user  (heartbeat-refreshed TTL → self-healing liveness)
     ├─ SADD  presence:room:members user
     ├─ updateView()  (local materialized view, LWW)
     └─ replication.Broadcast(ns=presence, ver) ─► other nodes apply → handlers → WS clients

Leave(room,user):  DEL record + SREM member + broadcast tombstone(Left=true)
GetRoom(room):     authoritative read (prunes expired members)
GetRoomLocal(room):hot-path read from in-memory view (no round trip)
Anti-entropy loop: periodic GetRoom to heal missed deltas
```

Presence is **eventually consistent** and **self-healing**: a user who stops
heart-beating simply expires — no explicit crash detection needed.

---

## 10. TTL Lifecycle

```
Set(key,value)
   └─ ttl.Resolve(cache, override) = base ± jitter        # anti-thundering-herd
        └─ Redis SET ... PX ttl
        └─ (sliding caches) ttl.Watch(key, ttl, cb)

Get(key)  ──(sliding cache)──► Redis EXPIRE ttl  +  ttl.Touch(key)   # renew on access

Reaper (every ReaperInterval):
   sweep() → for each watched key past deadline:
        fire callback(key)  +  emit TTLExpired event  +  metric
```

- **Absolute**: dies a fixed duration after write.
- **Sliding**: each access extends the lease (sessions, presence).
- **Jitter**: deterministic ±fraction spread so many keys don't co-expire.
- **Callbacks**: Redis can't push expiry without keyspace notifications, so the
  reaper provides application-level expiration hooks. Redis remains the
  authoritative expiry; the reaper is a best-effort local scheduler.

---

## 11. Concurrency Strategy

| Concern | Strategy |
|---|---|
| Thousands of concurrent cache requests | Redis connection pool (`PoolSize`); stateless manager; atomic registry counters |
| Concurrent Pub/Sub | one router goroutine per topic; per-subscription mutex isolates slow consumers |
| Concurrent session updates | optimistic concurrency — `CompareAndSet` on exact prior bytes, bounded retry → no lost updates |
| Concurrent presence replication | LWW versioning at both the replication gate and the local view |
| Concurrent lock operations | atomic `SET NX` acquire; token-checked (Lua) release/renew; mutex-guarded lease state |
| Distributed state sync | `CompareAndSwapState` (Lua CAS) + LWW-ordered broadcast |
| Cache stampede | refresh-ahead singleflight dedupe; TTL jitter |
| Backpressure | bounded buffers everywhere; drop-or-block policy; write-behind overflow → sync write |
| Shutdown | write-behind flush, reaper stop, subscriber teardown, context cancellation |

All subsystems propagate `context.Context` and honor cancellation. The full suite
passes under `go test -race`, including dedicated mutual-exclusion,
lost-update, backpressure-isolation, and 100k-op stress tests.

---

## 12. Error Handling

Canonical sentinels in `types` (matched with `errors.Is`): `ErrNil`,
`ErrRedisUnavailable`, `ErrSerialization`/`ErrDeserialization`/`ErrCorruption`,
`ErrLockNotAcquired`/`ErrLockNotHeld`/`ErrLockExpired`,
`ErrSessionNotFound`/`ErrSessionExpired`/`ErrSessionConflict`,
`ErrTopicNotRegistered`/`ErrBackpressure`/`ErrPubSubClosed`,
`ErrCacheNotRegistered`, `ErrNoLoader`/`ErrNoWriter`/`ErrUnknownPolicy`,
`ErrStateConflict`, `ErrConfig`.

| Failure | Handling |
|---|---|
| Redis unavailable | wrapped as `ErrRedisUnavailable`; health flips to `down`; metrics counted |
| Cache corruption | `ChecksummedCodec` detects CRC mismatch → `ErrCorruption` on read |
| Session conflict | CAS retry; `ErrSessionConflict` only after exhausting retries |
| Lock timeout | bounded `Acquire` → `ErrLockNotAcquired` |
| Pub/Sub disconnect | router reconnects with backoff; subscribers unaffected |
| Replication delay | anti-entropy resync; lag histogram |
| Serialization failure | `ErrSerialization` before any Redis write |
| Configuration error | `Validate()` fails fast at boot with `ErrConfig` |

---

## Quick Start

```go
sm, _ := state.New(state.Params{Config: config.Default()}) // nil Client → real Redis
_ = sm.Start(ctx)
defer sm.Close(ctx)

// Register a read-through cache backed by the PostgreSQL repository.
_ = sm.CacheManager().RegisterCache(manager.CacheSpec{
    Name:     "rooms",
    Strategy: policies.ReadThrough,
    TTL:      10 * time.Minute,
    Loader:   func(ctx context.Context, key string) (string, time.Duration, bool, error) {
        room, err := roomRepo.GetByID(ctx, key) // durable system of record
        if err != nil { return "", 0, false, err }
        b, _ := json.Marshal(room)
        return string(b), 0, true, nil
    },
})

var room Room
found, _ := sm.Cache().Get(ctx, "rooms", roomID, &room)

// Presence, sessions, locks — all from the one facade.
_ = sm.Presence().Announce(ctx, presence.Presence{RoomID: r, UserID: u, State: "online"})
sess, _ := sm.Sessions().Create(ctx, sessions.CreateParams{UserID: u, DeviceID: "laptop"})
_ = sm.Locks().WithLock(ctx, "exec:"+jobID, nil, func(ctx context.Context) error { return runJob(ctx) })
```

For tests / local dev with **no Redis**, inject the emulator:

```go
sm, _ := state.New(state.Params{Config: config.Default(), Client: redis.NewEmulator()})
```

---

## Future Integration Points

| Capability | How it integrates (interfaces already in place) |
|---|---|
| **Object Storage** | Cache metadata via write-through; store blobs in S3/GCS behind a `policies.Writer`. The cache holds hot metadata; the object store holds bytes. |
| **Distributed Coordination** | The `locks` manager + `NamespaceClusterState` are the seam. Swap single-instance locks for a Redlock quorum (token fencing already Redlock-compatible) or an etcd/ZK coordinator behind the same `Lock` API. |
| **Kubernetes** | Each pod is a node with its own `NodeID`; `Health()`/`Ping()` feed liveness/readiness probes; pub/sub + replication make the fleet stateless-per-request and horizontally scalable. |
| **Redis Cluster** | Replace `redis.NewRedis` with a `ClusterClient`; the `redis.Client` interface is unchanged. `keys.Builder` centralizes key layout for hash-tag sharding. |
| **Redis Sentinel** | Same one-constructor swap to a failover client; reconnect logic in pub/sub already tolerates topology changes. |
| **Redis Streams** | `TopicSpec.Durable` is reserved; a Streams-backed topic (consumer groups, replay) slots in alongside the pub/sub router. The Stage-2 `queue` module already demonstrates Streams. |
| **CQRS** | The event `Bus` already emits every mutation; split reads (cache/read-through) from writes (write-through/behind) and project into read models. |
| **Event Sourcing** | Subscribe to the event `Bus` (`CacheSet`, `SessionCreated`, `PresenceReplicated`, `LockAcquired`, …) and append to an event store; the events are already structured and typed. |
| **Multi-region** | LWW replication is region-agnostic; add region-scoped replication channels and a conflict policy. (Out of scope for this module.) |

---

## Boundaries (explicitly NOT in this module)

Redis Cluster, Redis Sentinel, CQRS, event sourcing, multi-region replication,
and consensus algorithms are **future stages**. This module focuses solely on
Redis-backed distributed state and caching, with the interfaces shaped so those
capabilities drop in without churn.
