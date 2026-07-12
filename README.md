# CPIP — WebSocket Gateway (Stage 1, Module 1)

Production-grade WebSocket infrastructure for the **Collaborative Programming
Infrastructure Platform (CPIP)**. This module implements the transport layer
that every later module (rooms, presence, CRDT relay, execution result
streaming) builds on: connection admission, lifecycle, heartbeat, backpressure,
a concurrency-safe registry, and graceful shutdown.

> Architecture context lives in the Stage-0 documents at the repo root:
> [`PRD.md`](./PRD.md), [`ARCHITECTURE.md`](./ARCHITECTURE.md),
> [`PROTOCOL.md`](./PROTOCOL.md), [`INFRASTRUCTURE.md`](./INFRASTRUCTURE.md).
> This module implements the **WebSocket Gateway** subsystem described there.

---

## Status

| | |
|---|---|
| Go | 1.24+ (developed on 1.26) |
| Transport | `github.com/gorilla/websocket` (isolated behind an interface) |
| HTTP router | `github.com/go-chi/chi/v5` |
| Tests | `go test -race ./...` — unit + concurrency + end-to-end |
| External state (Redis/Postgres/Docker) | **not** in this module — later stages |

What is implemented: WebSocket gateway, connection manager/registry, client
connection with read/write pumps, heartbeat (ping/pong + dead-connection
detection), pluggable authentication, graceful shutdown, connection cleanup,
origin validation, payload/connection limits, backpressure, panic recovery, and
metrics/logging **hooks** (extension points, no Prometheus yet).

---

## Quick start

```bash
# Run the gateway (defaults shown; all are overridable via CPIP_* env vars)
go run ./cmd/cpip

# Health probes
curl localhost:8080/healthz     # liveness  -> 200 {"status":"ok"}
curl localhost:8080/readyz      # readiness -> 200 {"status":"ready", ...}
curl localhost:8080/metrics     # reserved for the Prometheus exporter

# WebSocket endpoint (dummy auth: pass a user id)
#   ws://localhost:8080/ws?user_id=alice
```

```bash
# Tests (race detector on)
go test -race ./...

# Build the binary
go build -o bin/cpip ./cmd/cpip
```

---

## Architecture

The gateway is a **modular monolith**: one binary, many small packages with a
strict, acyclic dependency graph. Everything above the transport depends only on
**interfaces** (`websocket.Conn`, `auth.Authenticator`, `metrics.Recorder`,
`connection.Handler`, `ratelimit.Limiter`), so implementations are swapped by
dependency injection at the composition root (`cmd/cpip`) without touching
business logic.

```
                         ┌──────────────────────────────┐
      HTTP / WSS  ─────▶  │  api (chi router + middleware) │
                         └───────────────┬───────────────┘
                                         │  GET /ws
                                         ▼
                         ┌──────────────────────────────┐
                         │           gateway             │  admission pipeline
                         │  ratelimit→auth→capacity→     │
                         │  upgrade→session→register→run │
                         └───────┬───────────────┬───────┘
                                 │               │
                    ┌────────────▼───┐     ┌─────▼──────────────┐
                    │    manager      │     │    connection      │
                    │  (registry:     │◀────│  read pump         │
                    │  by conn/user/  │     │  write pump + ping │
                    │  session;       │     │  heartbeat         │
                    │  broadcast;     │     │  backpressure      │
                    │  shutdown)      │     │  Handler seam ─────┼──▶ (future:
                    └─────────────────┘     └────────────────────┘     rooms,
                                                                       presence,
   leaf packages (no internal deps):                                   CRDT,
   config · logger · metrics · auth · session · websocket ·            exec)
   security · ratelimit · id
```

**Dependency direction:** leaf packages depend on nothing internal; `connection`
depends on the leaves; `manager` depends on `connection`; `gateway` depends on
`manager` + `connection` + leaves; `api` depends on `gateway` + `health` +
`middleware`; `cmd/cpip` wires everything. No cycles.

---

## Folder tree

```
cpip/
├── cmd/
│   └── cpip/            main.go — composition root; wiring + graceful shutdown
├── internal/
│   ├── api/             chi router; mounts /ws, /healthz, /readyz, /metrics
│   ├── auth/            Authenticator interface + DummyAuthenticator (pluggable)
│   ├── config/          typed, validated, env-driven configuration (fail-fast)
│   ├── connection/      the connection: read/write pumps, heartbeat, backpressure,
│   │                    lifecycle, Handler seam, state machine
│   ├── gateway/         HTTP→WS upgrade + admission pipeline
│   ├── health/          liveness/readiness probes + drain flag
│   ├── id/              CSPRNG identifier generator
│   ├── logger/          slog-based structured logger constructor
│   ├── manager/         connection registry: lookup, broadcast, shutdown draining
│   ├── metrics/         Recorder interface + Noop (Prometheus extension point)
│   ├── middleware/      HTTP middleware: request-id, recover, access log (hijack-aware)
│   ├── ratelimit/       Limiter interface + NoopLimiter (extension point)
│   ├── security/        Origin validation for the WS handshake
│   ├── session/         per-participation session metadata
│   ├── websocket/       Conn/Upgrader interfaces + the gorilla adapter (only file
│   │                    importing the 3rd-party lib)
│   └── wstest/          in-memory fake Conn for tests (non-test package)
├── PRD.md ARCHITECTURE.md PROTOCOL.md INFRASTRUCTURE.md   (Stage-0 blueprints)
└── README.md
```

---

## Package responsibilities

| Package | Responsibility | Key exported types |
|---|---|---|
| `config` | Load + validate immutable config from `CPIP_*`; fail-fast on bad values | `Config` |
| `logger` | Build the root `*slog.Logger`; no global logger | `New`, `Nop` |
| `metrics` | Observability hook surface + no-op default | `Recorder`, `Noop` |
| `id` | Cryptographically-random ids for conns/sessions/correlation | `New`, `NewWithPrefix` |
| `auth` | Edge authentication boundary; pluggable strategy | `Authenticator`, `Identity`, `DummyAuthenticator` |
| `session` | Participation-window metadata (decoupled from the socket) | `Session` |
| `websocket` | Library-agnostic `Conn`/`Upgrader` + gorilla adapter + close codes | `Conn`, `Upgrader`, `GorillaUpgrader` |
| `security` | Strict Origin allow-list for the handshake | `OriginChecker` |
| `ratelimit` | Connection-admission rate-limit hook | `Limiter`, `NoopLimiter` |
| `connection` | One live connection: pumps, heartbeat, backpressure, teardown, Handler seam | `Connection`, `Handler`, `Config`, `Inbound` |
| `manager` | Concurrency-safe registry; lookup by conn/user/session; broadcast; drain | `Manager` |
| `middleware` | HTTP request-id / recover / access-log (forwards Hijacker for WS) | `RequestID`, `Recoverer`, `AccessLog` |
| `health` | Liveness + readiness (dependency checks + drain flag) | `Checker`, `Check` |
| `gateway` | Upgrade + admission pipeline; constructs and starts connections | `Gateway` |
| `api` | Assemble the HTTP surface (routes + middleware chain) | `NewRouter`, `Deps` |
| `wstest` | In-memory `websocket.Conn` for deterministic tests | `FakeConn` |

---

## Request flow (WebSocket handshake)

```
Client GET /ws (Upgrade headers, credential)
  │
  ▼  api: RequestID → Recoverer → AccessLog middleware
  ▼  gateway.HandleWS:
  1. ratelimit.Allow(clientIP)              → 429 if refused
  2. auth.Authenticate(request)             → 401 if unauthorized   (BEFORE upgrade)
  3. manager.AtCapacity()                   → 503 if full           (soft fast-path)
  4. upgrader.Upgrade(w, r)                 → 101 Switching Protocols
  5. session.New(); connection.New(...)
  6. manager.Register(conn)                 → close(1013) if the authoritative
  │                                            capacity/closed check fails
  7. go conn.Serve()                        → HTTP handler returns immediately
```

Submission of code for execution, room joins, etc. are **not** part of this
module; they arrive as WebSocket messages and will be interpreted by a future
`connection.Handler` implementation (see Integration points).

---

## Connection lifecycle

```
CONNECT ─▶ UPGRADE ─▶ AUTHENTICATE ─▶ REGISTER ─▶ ACTIVE ─▶ CLOSING ─▶ CLOSED
                                                    │             ▲
                                    ┌───────────────┼─────────────┘
                                    │  read pump  write pump (+ heartbeat ping)
                                    └───────────────┘
                          ▼ any of: client close · heartbeat timeout ·
                            slow-consumer overflow · write failure ·
                            server shutdown · panic
                          ▼
                     CLEANUP ─▶ metrics.ConnectionClosed(reason)
                              ─▶ handler.OnDisconnect(cause)
                              ─▶ manager.Unregister (registry removal)
```

State machine (`connection.State`): `connecting → active → closing → closed`.
The close **cause** (client close / heartbeat timeout / slow consumer / write
failure / oversized / server shutdown / panic) is carried on the connection
context via `context.WithCancelCause` and mapped to a low-cardinality metrics
reason and log field.

**Heartbeat / dead-connection detection** (standard robust split):

- *Write pump* sends a `ping` every `HeartbeatInterval`.
- *Read pump* sets a read deadline of `PongTimeout`; the pong handler (and any
  inbound frame) extends it. If `PongTimeout` elapses with no pong/data,
  `ReadMessage` times out and the connection is reaped as `heartbeat_timeout`.
- `Config.Validate` enforces `PongTimeout > HeartbeatInterval` so a healthy
  client always answers in time.

---

## Concurrency model

Designed for **thousands of concurrent connections** with **exactly two
goroutines per connection**, both guaranteed to terminate — no goroutine leaks.

- **Per connection:** `Serve()` runs the **read pump** inline (in the goroutine
  the gateway spawns with `go c.Serve()`); it spawns one **write pump**
  goroutine. That's it.
- **Single-writer rule:** gorilla forbids concurrent writes, so *all* writes
  (data frames, pings, the close frame) funnel through the write pump. This is
  what makes the connection race-free without per-write locks.
- **Closing is exactly-once** via `sync.Once`, which cancels the connection
  context (recording the cause). The write pump is the **sole closer** of the
  underlying socket; closing it unblocks the read pump. When both pumps have
  returned, `finalize()` runs cleanup exactly once.
- **Backpressure:** the outbound queue is a **bounded** channel
  (`SendQueueSize`). `Send` is non-blocking: on overflow the client is a slow
  consumer, so that one connection is closed (isolation) — it never blocks or
  degrades other connections. This is the single outbound backpressure decision
  point.
- **Registry:** `manager` uses an `RWMutex` over three maps (by connection id,
  by user id → set, by session id). Writes (register/unregister) take the write
  lock; lookups take the read lock; broadcast snapshots under the read lock and
  sends outside it, so a send never holds the registry lock.
- **Context propagation:** every connection derives from a base context that is
  cancelled on shutdown, cascading cancellation as a safety net alongside the
  manager's coordinated close.
- **Panic containment:** both pumps and the HTTP handlers recover from panics,
  contain them to the single connection/request, log with a correlation id, and
  close cleanly — a panic never crosses connection boundaries or crashes the
  node.

Verified by `go test -race ./...`, including a concurrent
register/lookup/unregister stress test and no-goroutine-leak checks.

---

## Graceful shutdown

On `SIGINT`/`SIGTERM` the composition root:

1. marks the node **not-ready** (`health.SetDraining(true)`) so the load
   balancer drains it;
2. `http.Server.Shutdown` — stops accepting new handshakes (the WS handler
   returns immediately after `go Serve`, so this is quick);
3. `manager.Shutdown(ctx)` — initiates a graceful close on **every** live
   connection and waits for them to drain (bounded by `ShutdownTimeout`); each
   connection emits a WebSocket close frame;
4. cancels the base context as a final safety net.

No acknowledged connection is dropped without a close handshake within the drain
window.

---

## Configuration guide

All configuration is loaded once at startup from the environment and validated;
invalid values abort boot. Defaults are production-sane.

| Env var | Default | Meaning |
|---|---|---|
| `CPIP_LISTEN_ADDR` | `:8080` | HTTP/WS bind address |
| `CPIP_READ_HEADER_TIMEOUT` | `10s` | request-header read timeout (slowloris guard) |
| `CPIP_SHUTDOWN_TIMEOUT` | `30s` | max drain time on graceful shutdown |
| `CPIP_HEARTBEAT_INTERVAL` | `30s` | server ping cadence |
| `CPIP_PONG_TIMEOUT` | `60s` | read/pong deadline (**must be > heartbeat**) |
| `CPIP_WRITE_TIMEOUT` | `10s` | per-write deadline |
| `CPIP_MAX_PAYLOAD_BYTES` | `1048576` | max inbound message size (1 MiB) |
| `CPIP_SEND_QUEUE_SIZE` | `256` | per-connection outbound buffer (backpressure bound) |
| `CPIP_MAX_CONNECTIONS` | `100000` | global concurrent-connection cap per node |
| `CPIP_HANDSHAKE_TIMEOUT` | `10s` | WS upgrade handshake timeout |
| `CPIP_READ_BUFFER_SIZE` | `4096` | upgrader read buffer |
| `CPIP_WRITE_BUFFER_SIZE` | `4096` | upgrader write buffer |
| `CPIP_ALLOWED_ORIGINS` | `*` | comma-separated Origin allow-list (`*` = allow all, dev) |
| `CPIP_AUTH_ALLOW_ANONYMOUS` | `true` | dummy auth: mint anonymous id when none supplied |
| `CPIP_LOG_LEVEL` | `info` | `debug`\|`info`\|`warn`\|`error` |
| `CPIP_LOG_FORMAT` | `json` | `json`\|`text` |

**Validated invariants:** `PongTimeout > HeartbeatInterval`; all sizes/timeouts
positive; non-empty origins; valid log level/format.

---

## Future integration points

This module deliberately exposes seams so later modules bolt on without
transport refactoring:

| Later module | Seam it plugs into | How |
|---|---|---|
| **Room Management** | `connection.Handler.OnConnect` / `OnDisconnect`; `Connection.SetRoomID` | A real `Handler` binds/unbinds room membership; the manager already indexes by user/session and supports targeted delivery. |
| **Presence** | `connection.Handler` + `manager.SendToUser`/`Broadcast` | Awareness updates route through the Handler and fan out via the registry (cross-node fan-out added with Redis pub/sub later). |
| **CRDT relay** | `connection.Handler.OnMessage` (opaque frames) + `manager` targeted send | The transport passes opaque binary frames untouched; the relay Handler fans deltas to room members. |
| **Execution pipeline** | inbound frames via `Handler.OnMessage`; results via `Connection.Send` | Job submission arrives as messages; streamed stdout/stderr are delivered as ordered `Send` frames. |
| **Auth (JWT/OAuth/API keys)** | `auth.Authenticator` | Swap `DummyAuthenticator` for a real implementation at the composition root — zero gateway changes. |
| **Rate limiting** | `ratelimit.Limiter` | Swap `NoopLimiter` for a token-bucket (Redis-backed for cross-node correctness). |
| **Metrics (Prometheus)** | `metrics.Recorder` | Replace `Noop` with a Prometheus recorder; all call sites already emit events. |
| **Health (Redis/Postgres)** | `health.Checker.Register` | Register dependency checks; readiness composes them automatically. |

The single most important seam is **`connection.Handler`** — it is the boundary
between "the platform owns the socket" and "later modules own the meaning of the
bytes."

---

## Testing

```bash
go test -race ./...        # unit, concurrency, and end-to-end (real gorilla client)
```

Coverage by level:

- **config** — validation invariants, env overrides, bad-input rejection.
- **auth** — header/query/anonymous/reject paths.
- **connection** — echo, heartbeat ping emission, pong tracking, slow-consumer
  backpressure close, server shutdown close, read-limit application, and a
  no-goroutine-leak check.
- **manager** — register/lookup, multi-connection-per-user, connection limit,
  duplicate id, idempotent unregister, broadcast/targeted send, a concurrent
  register/unregister stress test (race detector), and shutdown draining.
- **gateway** — real end-to-end handshakes with a gorilla client: upgrade+echo,
  auth rejection (401), origin rejection/acceptance (403/101), capacity
  rejection (503), graceful shutdown closing the client, and health endpoints.
