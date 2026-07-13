# Execution Orchestrator (CPIP — Stage 2, Module 1)

The execution orchestrator is the **single source of truth for execution
lifecycle management** in the Collaborative Programming Infrastructure Platform.
It accepts execution requests, validates them, creates and tracks jobs through a
formal state machine, builds their cancellable execution contexts, and hands
them to a scheduler — while remaining **completely decoupled from the queue,
worker pool, runtime, and sandbox** that will execute code in later modules.

Think of it as the orchestration layer behind Judge0 / Replit / AWS Lambda:
everything up to "a job is ready to run" and everything after "a worker reported
a result", but never the running of code itself.

> **Boundary.** The orchestrator NEVER executes code. It never spawns a process,
> touches Docker, or compiles anything. The language registry is metadata only.
> The scheduler is an interface with an in-memory development implementation.

---

## 1. Folder tree

```
internal/execution/
├── README.md
├── job/          # dependency-free domain: Job, Request, state machine, errors
│   ├── state.go  ├── job.go  └── errors.go
├── config/       # tunable configuration (DI, no globals)
├── language/     # supported-language registry (metadata only)
├── validation/   # pluggable validation pipeline + built-in validators
│   ├── validation.go  └── validators.go
├── context/      # per-job execution context + cancellation manager
├── scheduler/    # scheduler interface + in-memory + no-op implementations
├── registry/     # concurrent job registry with multi-key indexes
├── storage/      # Repository + Archive interfaces + in-memory implementations
├── events/       # non-blocking lifecycle event bus + typed events
├── metrics/      # Recorder interface + Noop
├── logger/       # slog factory + Tracer seam
├── middleware/   # composable decorators over the request path
├── orchestrator/ # the engine: request → validate → create → schedule → track
└── manager/      # composition root + public Service + archival loop
```

The dependency graph is a DAG rooted at `manager`, with `job` as the universal
sink (imports only the standard library).

## 2. Files created

| Package | Files |
|---------|-------|
| `job` | `state.go`, `job.go`, `errors.go`, `state_test.go`, `job_test.go` |
| `config` | `config.go`, `config_test.go` |
| `language` | `language.go`, `language_test.go` |
| `validation` | `validation.go`, `validators.go`, `validation_test.go` |
| `context` | `context.go`, `context_test.go` |
| `scheduler` | `scheduler.go`, `scheduler_test.go` |
| `registry` | `registry.go`, `registry_test.go` |
| `storage` | `storage.go`, `storage_test.go` |
| `events` | `events.go`, `events_test.go` |
| `metrics` | `metrics.go` |
| `logger` | `logger.go` |
| `middleware` | `middleware.go`, `middleware_test.go` |
| `orchestrator` | `orchestrator.go`, `orchestrator_test.go` |
| `manager` | `manager.go`, `manager_test.go` |

## 3. Package responsibilities

| Package | Responsibility | Depends on |
|---------|----------------|------------|
| `job` | The domain model: `Request` (submission input), `Job` (tracked entity), the lifecycle `State` machine (`CanTransition`), `Priority`, `Outcome`, `ResourceProfile`, `Statistics`, and the canonical error set. **Stdlib only.** | — |
| `config` | `Config` with payload limits, execution controls, priority range, retention, and validation toggles; `Default()` + `Validate()`. Injected, never global. | `job` |
| `language` | Concurrency-safe registry of supported languages (metadata: compiler, runtime, resource profile/limits, status). Seeded `Default()`. **No compilation.** | `job` |
| `validation` | Pluggable, independently-testable validator chain (auth, authz, language, code/input size, timeout, memory, priority, metadata, + custom) and a `Pipeline` that aggregates verdicts. | `job`, `config`, `language`, `metrics` |
| `context` | Builds and owns each job's cancellable execution context (tracing IDs, security metadata, resource profile, deadline, future worker/sandbox assignment). Single owner of cancellation. | `job` |
| `scheduler` | The `Scheduler` interface (`Schedule`/`Cancel`/`Retry`/`Reprioritize`) plus an in-memory priority scheduler and a no-op. Queue backends (Redis/Kafka) implement this later. | `job` |
| `registry` | Concurrency-safe in-memory job store with indexes by ID, user, room, session, language, and state. Enforces the state machine atomically via `Transition`; returns immutable snapshots. | `job` |
| `storage` | `Repository` (live persistence) and `Archive` (terminal retention) interfaces + in-memory implementations. The PostgreSQL seam. | `job` |
| `events` | Non-blocking local event bus and the typed lifecycle events. | `job` |
| `metrics` | `Recorder` interface + `Noop`. Vendor-agnostic telemetry seam. | — |
| `logger` | Component-scoped `slog` factory + `Tracer`/`Span` seam. | — |
| `middleware` | Composable decorators (`Logging`, `Recovery`) over the request-facing `Submitter`. Panic recovery keeps one bad request from crashing the process. | `job`, `logger` |
| `orchestrator` | The engine. Owns `SubmitExecution`, `Cancel`, `Retry`, the lifecycle marks (`MarkDispatched`…`MarkTimedOut`), status queries, and archival. Drives every transition; executes nothing. | all of the above |
| `manager` | Composition root. Wires everything via DI, decorates the request path with middleware, runs the archival sweep, and exposes the public `Service`. | `orchestrator` + deps |

## 4. Job lifecycle

A job is created from a validated `Request`, threaded through the pipeline, and
retired to the archive:

```
Request ──validate──▶ Job(Pending) ──▶ Validated ──▶ Queued ──▶ Dispatched
                          │                                        │
                    (reject → no job)                        (worker claims)
                                                                   ▼
   Archived ◀──retention── {Completed|Failed|TimedOut|Cancelled} ◀ Running ──▶ Streaming
                                    ▲          │
                                    │      (recoverable)
                                    └── Retrying ──▶ Queued (RetryCount++)
```

- **Intake** (`SubmitExecution`): validate → create `Job` → register → build
  execution context → `Queued` → `Scheduler.Schedule`.
- **Execution** (future worker/runtime via lifecycle marks): `MarkDispatched` →
  `MarkStarted` → `MarkStreaming` → `MarkCompleted` / `MarkFailed` / `MarkTimedOut`.
- **Cancellation** (`Cancel`): any active state → `Cancelled`, cancelling the
  execution context and descheduling.
- **Retry** (`Retry`): `Failed`/`TimedOut` → `Retrying` → `Queued`, bounded by
  `MaxRetries`.
- **Archival** (background sweep): finished jobs past `ArchiveRetention` →
  `Archived`, moved from the live registry to the `Archive` sink.

## 5. State transition diagram

Enforced by `job.CanTransition` (adjacency map). Self-transitions are rejected;
illegal transitions return `job.ErrIllegalTransition`.

```
 Pending ────▶ Validated ────▶ Queued ────▶ Dispatched ────▶ Running ────▶ Streaming
    │              │             │  ▲            │               │              │
    │              │             │  │ retry      │               │              │
    ▼              ▼             ▼  │            ▼               ▼              ▼
 Cancelled     Cancelled     Cancelled        Cancelled       Completed ◀──────┤
 Failed        Failed        Failed│TimedOut   Failed│TimedOut Failed│TimedOut  │
                                   │                                            │
        Failed ─┐  TimedOut ─┐     └────────── Retrying ──▶ Queued              │
                ▼            ▼                     ▲                             │
             Retrying     Retrying ───────────────┘                             │
                                                                                ▼
   {Completed, Failed, TimedOut, Cancelled} ─────── retention ──────────▶  Archived (terminal)
```

Legal target sets (source → targets):

| From | To |
|------|----|
| Pending | Validated, Failed, Cancelled |
| Validated | Queued, Failed, Cancelled |
| Queued | Dispatched, Failed, TimedOut, Cancelled |
| Dispatched | Running, Failed, TimedOut, Cancelled |
| Running | Streaming, Completed, Failed, TimedOut, Cancelled |
| Streaming | Completed, Failed, TimedOut, Cancelled |
| Completed | Archived |
| Failed | Retrying, Archived |
| TimedOut | Retrying, Archived |
| Cancelled | Archived |
| Retrying | Queued, Failed, Cancelled |
| Archived | — (terminal) |

## 6. Validation pipeline flow

The pipeline is an ordered list of independent validators. Cheap identity checks
run first, payload-size checks next, resource checks last. Default behavior is
**stop-on-first-failure**; `CollectAll()` gathers every failure.

```
Request
  │
  ▼
[authentication] → [authorization] → [language] → [code_size] → [input_size]
  → [timeout] → [memory_profile] → [priority] → [metadata] → [custom…]
  │                                                              │
  ├── any validator returns error ──▶ Result{Failures}  ──▶ ExecutionRejected
  │                                    (wraps the specific job sentinel)
  └── all pass ──▶ Result{OK} ──▶ ExecutionValidated ──▶ job creation
```

Each validator wraps a specific sentinel (`ErrUnsupportedLanguage`,
`ErrCodeTooLarge`, `ErrInvalidPriority`, …), so callers `errors.Is` the exact
cause. Every validator is constructed with its dependencies and unit-tested in
isolation; custom validators are appended via `DefaultValidators(..., custom...)`
or the `Pipeline`.

## 7. Execution request flow

```
 Caller (gateway / REST / gRPC)
     │  Submit(ctx, Request)
     ▼
 manager.Manager ──▶ middleware (Recovery → Logging) ──▶ orchestrator.SubmitExecution
     │                                                        │
     │   1. assign RequestID / CorrelationID                  │  events: ExecutionRequested
     │   2. validation.Pipeline.Validate ────── reject ──────▶│  events: ExecutionRejected  (return err)
     │   3. resolve language + config defaults                │  events: ExecutionValidated
     │   4. job.New → registry.Add (Pending)                  │  events: JobCreated
     │   5. Transition → Validated                            │
     │   6. context.Manager.Create (cancellable, deadline)    │
     │   7. Transition → Queued                               │
     │   8. Scheduler.Schedule ───── unavailable ── roll ────▶│  → Failed, events: JobFailed (return err)
     │   9. Repository.Save (best-effort)                     │  events: JobQueued
     ▼                                                        ▼
 returns Job snapshot (Queued)                     [future] worker pulls from scheduler,
                                                    calls MarkDispatched / MarkStarted / … 
```

## 8. Registry architecture

The registry is the authoritative in-memory store during a job's lifetime. One
`sync.RWMutex` guards the primary map and every secondary index; all mutation
flows through `Transition`/`Update`, and all reads return `Job.Clone()`
snapshots — so a queried job can never race with a concurrent write.

```
                        ┌───────────────── Registry (RWMutex) ─────────────────┐
   byID: id → *Job ────▶│  the single authoritative *Job (mutated under lock)  │
                        └───────────────────────────────────────────────────────┘
   Secondary indexes (key → set of job IDs), kept consistent on Add/Transition/Remove:
     byUser     : userID    → {ids}
     byRoom     : roomID     → {ids}
     bySession  : sessionID  → {ids}
     byLanguage : language   → {ids}
     byState    : State      → {ids}     ◀── re-indexed atomically on every Transition
```

- **Atomic transitions.** `Transition(id, to, mutate)` checks `CanTransition`,
  applies the state change plus an optional field mutation, and re-indexes
  `byState` — all under one lock acquisition. It returns the previous state for
  metrics.
- **Snapshot isolation.** `Get`, `ByUser`, `ByState`, etc. return deep copies.
- **Identity fields are immutable.** `Update` defends against state changes and
  callers must not mutate indexed keys, keeping indexes correct without rebuilds.
- Statistics (`Stats`) and archival candidate selection (`FinishedBefore`) read
  the same indexes without scanning the whole store.

## 9. Concurrency strategy

- **No global mutex.** Each component owns fine-grained locks: the registry map,
  the language registry, the context manager map, the in-memory scheduler, the
  storage maps, and the event bus each have their own `RWMutex`.
- **Registry is the serialization point** for job state: every lifecycle change
  is a single atomic `Transition`, so concurrent `Cancel`/`Retry`/mark calls on
  the same job resolve deterministically — the state machine rejects whichever
  loses the race (e.g. cancel-after-complete → `ErrCancellationConflict`).
- **Immutable snapshots** cross goroutine boundaries; no shared mutable job state
  escapes a lock.
- **Non-blocking event bus.** A slow subscriber is dropped for an event rather
  than stalling the orchestrator's hot path.
- **Cancellation** is delivered once, through the context manager, via
  `context.WithDeadline` + idempotent `Cancel` (`sync.Once`).
- **Middleware `Recovery`** converts a panic in the request path into an error,
  isolating faults to a single request.
- Verified with `go test -race`, including concurrent-submission/cancellation
  and 400-job stress tests across the orchestrator and manager.

## 10. Future integration points

| Area | Seam in place | What connects later |
|------|---------------|---------------------|
| **Redis Streams queue** | `scheduler.Scheduler` interface (`Schedule`/`Cancel`/`Retry`/`Reprioritize`); in-memory impl today | A Redis Streams scheduler implements the interface; injected via `manager.Params.Scheduler`. No orchestrator change. |
| **Worker pool** | Lifecycle marks (`MarkDispatched(workerID)`, `MarkStarted`, `MarkStreaming`, `MarkCompleted/Failed/TimedOut`) + `ExecutionContext` per job | Workers pull from the queue, claim jobs via `MarkDispatched`, and report progress/results through the marks. |
| **Docker sandbox** | `job.ResourceProfile`, `ExecutionOptions`, `context.SecurityMetadata`, and `ExecutionContext.SandboxID`/`Assign` | The sandbox manager enforces the resource profile and network policy and records `SandboxID`/`ContainerID`. |
| **Runtime manager** | `language.Language` metadata (compiler, runtime, `CompileRequired`, `PluginID`) | The runtime manager reads language metadata to compile/run; `PluginID` selects a runtime plugin. |
| **PostgreSQL persistence** | `storage.Repository` + `storage.Archive` interfaces; in-memory today | Implement both over `pgx`; inject via `manager.Params.Repository`/`Archive`. |
| **Monitoring** | `metrics.Recorder` interface + `logger.Tracer`/`Span` seams; every transition, validation, and lifecycle step is instrumented | Inject a Prometheus/OpenTelemetry `Recorder` and a real `Tracer`. Subscribe to the event bus for an audit/event stream. |

---

## Public interface

`manager.Manager` implements `manager.Service` — the minimal surface for the
gateway, room manager, presence, collaboration engine, and future REST/gRPC:

```go
type Service interface {
    Submit(ctx, job.Request) (job.Job, error)
    Cancel(ctx, jobID string) error
    Retry(ctx, jobID string) error

    MarkDispatched(ctx, jobID, workerID string) error
    MarkStarted(ctx, jobID string) error
    MarkStreaming(ctx, jobID string) error
    MarkCompleted(ctx, jobID string) error
    MarkFailed(ctx, jobID, reason string) error
    MarkTimedOut(ctx, jobID string) error

    Status(jobID string) (job.Job, error)
    Statistics(jobID string) (job.Statistics, error)
    ByUser / ByRoom / BySession / ByState / ByLanguage
    Stats() registry.Stats
    Languages() []language.Language
    Events() *events.Bus
}
```

## Testing

```
go test ./internal/execution/...            # unit + integration
go test -race ./internal/execution/...      # race + concurrency/stress
```

Covered: job creation, state transitions (legal + illegal), every validator and
the pipeline, cancellation and cancellation conflicts, retry and retry
exhaustion, scheduler abstraction (priority ordering, capacity/unavailable),
scheduler-unavailable rollback, registry indexes/queries/snapshot-isolation,
execution-context cancellation/deadline/release, archival sweep, middleware
panic-recovery, race-condition and high-concurrency stress (400 concurrent jobs).
