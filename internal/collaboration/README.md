# Collaboration Engine (CPIP — Stage 1, Module 4)

The collaboration engine is the conflict-free, real-time editing core of the
Collaborative Programming Infrastructure Platform. It owns the lifecycle of live
collaborative documents, drives the Yjs synchronization protocol, schedules
snapshots, recovers documents from durable storage, and publishes domain events
that the WebSocket gateway, room manager, presence system, and (future)
execution infrastructure subscribe to.

It is designed to host **thousands of concurrent documents and edit streams** on
a single node with conflict-free synchronization and low latency, and to scale
out later via pluggable persistence and replication — without rewriting the core.

> **CRDT policy.** The engine never implements a CRDT algorithm itself. All
> conflict resolution, update encoding/merging, and state-vector computation is
> delegated to the Yjs-compatible engine (`github.com/reearth/ygo/crdt`), reached
> exclusively through the `yjs` adapter package.

---

## 1. Folder tree

```
internal/collaboration/
├── README.md            # this document
├── manager.go           # composition root + public Service facade + background loops
├── manager_test.go
├── config/              # tunable engine configuration (DI, no globals)
│   └── config.go
├── types/               # dependency-free domain model: states, wire types, errors
│   └── document.go
├── yjs/                 # the ONLY CRDT-touching package: thread-safe DocWrapper
│   └── yjs.go
├── registry/            # in-memory index of live documents + participants + meta
│   └── registry.go
├── sync/                # synchronization engine (SV exchange, delta, batch, late-join)
│   └── sync.go
├── snapshot/            # full/incremental snapshots, compression, retention, rebuild
│   └── snapshot.go
├── recovery/            # reconstruct live state from snapshot chain + update log
│   └── recovery.go
├── storage/             # Repository interface + in-memory implementation
│   └── storage.go
├── events/              # non-blocking local event bus + typed events
│   └── events.go
├── metrics/             # Recorder interface + Noop (vendor-agnostic telemetry seam)
│   └── metrics.go
└── logger/              # slog factory + Tracer seam (observability)
    └── logger.go
```

Every package except `manager` is a leaf or near-leaf; the dependency graph is a
DAG rooted at `manager`, with `types` as the universal sink (imports only stdlib).

## 2. Package responsibilities

| Package    | Responsibility | Depends on |
|------------|----------------|------------|
| `types`    | Domain model: `DocumentState` state machine (`CanTransition`), `SyncStatus`, `SnapshotKind`, wire structs (`Update`, `Snapshot`, `Participant`), per-document metadata, and the canonical error set. Imports **stdlib only**. | — |
| `config`   | `Config` with snapshot/limit/timeout/GC/compression knobs; `Default()` and `Validate()` (normalizes zero values, rejects invalid combinations). Injected, never global. | — |
| `yjs`      | `DocWrapper`: a thread-safe façade over `crdt.Doc`. Serializes access with a single writer lock, exposes apply/encode/state-vector/multi-file operations, and bridges engine update notifications to an observer hook. **Sole owner of the CRDT dependency.** | `crdt` |
| `registry` | Concurrency-safe in-memory index of live `DocumentEntry` values: state, monotonic version, dirty flag, participants, and snapshot/recovery/persistence metadata. Enforces lifecycle transitions and returns lock-safe copies (`DocumentInfo`). | `types`, `yjs` |
| `sync`     | Stateless synchronization engine: SV exchange (step 1/2), incremental & reconnect deltas, late-join full state, sequential batch application, and self-contained merge. | `crdt`, `yjs`, `types`, `metrics` |
| `snapshot` | Full & incremental snapshot creation, transparent gzip compression, retention pruning, and snapshot-chain **reconstruction**. | `storage`, `yjs`, `types`, `metrics`, `crdt` |
| `recovery` | Reconstructs a live document from the snapshot chain + replayed update-log tail, then verifies consistency. Returns a rich `Result`. | `storage`, `snapshot`, `yjs`, `types`, `metrics` |
| `storage`  | `Repository` interface (metadata, update log, snapshots) and a concurrency-safe `InMemoryRepository`. The seam for future PostgreSQL. | `types` |
| `events`   | Non-blocking local `Bus` (drops to slow subscribers rather than blocking) and the typed `Event`/`UpdatePayload` model. | — |
| `metrics`  | `Recorder` interface + `Noop`. Vendor-agnostic telemetry seam. | — |
| `logger`   | `slog` component-scoped factory + `Tracer`/`Span` seam for future distributed tracing. | — |
| `manager`  | Composition root. Wires all collaborators via DI, exposes the public `Service` interface, and runs the saver + janitor background loops. | all of the above |

## 3. Public interface

`manager.Manager` implements `manager.Service` — the minimal surface exposed to
the gateway, room manager, presence system, and future REST/gRPC facades:

```go
type Service interface {
    // Lifecycle
    GetOrCreateDocument(ctx, docID, roomID, filePath) (*yjs.DocWrapper, error)
    ArchiveDocument(ctx, docID) error
    DeleteDocument(ctx, docID) error

    // Synchronization
    ServerStateVector(docID) ([]byte, error)
    HandleSyncStep1(ctx, docID, clientStateVector) ([]byte, error)   // → step-2 delta
    InitialSync(ctx, docID) ([]byte, error)                          // late-join full state
    ApplyIncrementalUpdate(ctx, docID, update) error
    BatchUpdates(ctx, docID, updates) error

    // Participants
    JoinDocument(ctx, docID, participantID) error
    LeaveDocument(ctx, docID, participantID) error
    MarkParticipantSynced(docID, participantID)
    Participants(docID) []types.Participant

    // Durability & introspection
    SaveSnapshot(ctx, docID) error
    Statistics(docID) (types.Statistics, error)
    Events() *events.Bus
}
```

## 4. Document lifecycle (formal state machine)

The lifecycle is enforced by `types.CanTransition`, the single source of truth
consulted by both the document entry and the registry. Illegal transitions
return `types.ErrInvalidDocumentState`.

```
        ┌─────────┐   register    ┌─────────────┐   first edit   ┌────────┐
        │ Created │ ────────────► │ Initialized │ ─────────────► │ Active │
        └─────────┘               └─────────────┘                └────────┘
             │                          │                          │   ▲
             │                          │                     edit │   │ snapshot done
             │                          ▼                          ▼   │
             │                     ┌──────────┐              ┌───────┐  │
             │            archive  │ Archived │              │ Dirty │  │
             │            ◄────────┤          │◄──┐          └───────┘  │
             │                     └──────────┘   │ idle          │     │
             ▼                          │         │  archive      ▼     │
        ┌───────────┐                   │ reopen  │        ┌──────────────────┐
        │ Destroyed │◄── delete ── (any)│         └────────┤ Snapshot Pending │
        └───────────┘                   ▼                  └──────────────────┘
                                  ┌───────────┐                     │ persisted
                                  │ Recovered │                     ▼
                                  └───────────┘              ┌───────────┐
                                        │  ──── active ────► │ Persisted │
                                        └───────────────────►└───────────┘
```

- **Created → Initialized**: registered and Yjs doc allocated.
- **Initialized → Active**: first edit (via `MarkEdited`).
- **Active → Dirty**: unsaved edits accumulate; `PendingUpdates` grows.
- **Dirty → SnapshotPending → Persisted**: the saver (or a forced flush) snapshots.
- **Persisted/Dirty/Active → Archived**: janitor unloads an idle document (flushing first if dirty); CRDT resources are released.
- **Archived → Recovered → Active**: a later `GetOrCreateDocument` rebuilds it.
- **any → Destroyed**: `DeleteDocument` purges memory and storage.

## 5. Synchronization flow

The engine speaks the standard Yjs two-step handshake. A **state vector** is a
compact `{clientID: clock}` map describing everything a peer already knows; the
reply carries exactly the operations it is missing. This single mechanism serves
initial, incremental, late-join, and reconnect synchronization.

```
 Peer (client)                    Gateway                 Manager / sync.Engine
      │                              │                              │
      │  join document               │                              │
      │─────────────────────────────►│  GetOrCreateDocument         │
      │                              │─────────────────────────────►│ (load/recover/create)
      │                              │  JoinDocument                │
      │                              │─────────────────────────────►│ ParticipantJoined ▲events
      │                              │                              │
      │  Sync Step 1 (my SV)         │                              │
      │─────────────────────────────►│  HandleSyncStep1(clientSV)   │
      │                              │─────────────────────────────►│ GenerateSyncStep2:
      │                              │                              │   empty SV → full state (late join)
      │                              │                              │   stale SV → missed ops (reconnect)
      │  Step 2 (delta)  ◄───────────│◄─────────────────────────────│
      │  apply locally               │                              │
      │                              │                              │
      │  local edit → update ────────►│  ApplyIncrementalUpdate      │
      │                              │─────────────────────────────►│ validate size → CRDT apply
      │                              │                              │ MarkEdited (v++) → append log
      │                              │                              │ UpdateApplied ▲event
      │                              │  UpdateGenerated ▲event ◄─────│ (observer hook, for fan-out)
      │  ◄── fan-out to other peers ─│                              │
```

Bandwidth is minimized three ways: reconnect deltas ship only missed ops
(step 2 against a stale SV), `sync.Delta` strips known ops from a relayed update
without loading a document, and snapshots/updates are gzip-compressed at rest.

## 6. Snapshot workflow

```
SaveSnapshot(docID)
  │
  ├─ PlanSnapshot ─ lock-safe read of {doc, version, prev-snapshot-meta}
  ├─ Transition → SnapshotPending
  ├─ snapshot.Create:
  │     ├─ decide kind:  first / every Nth  → FULL   (EncodeStateAsUpdate(nil))
  │     │                otherwise           → INCREMENTAL (delta vs prev SV)
  │     ├─ gzip-compress if enabled and payload ≥ threshold (and it shrinks)
  │     ├─ SaveSnapshot → repo
  │     └─ PruneSnapshots(retentionCount)          # best-effort
  ├─ DeleteUpdates(version+1)                        # log tail now subsumed
  ├─ RecordSnapshot ─ advance meta, reset edit count, clear dirty (atomic)
  ├─ Transition → Persisted
  └─ emit DocumentSnapshotCreated, DocumentSaved, DocumentPersisted
```

A **full** snapshot every *N*th capture bounds recovery-replay length; cheap
**incremental** snapshots fill the gaps. Reconstruction assembles the chain from
the most recent full snapshot forward.

## 7. Recovery flow

```
RecoverDocument(docID)                                       (bounded by RecoveryTimeout)
  │
  ├─ snapshot.Reconstruct ─ ordered decompressed payloads from last FULL forward
  │     └─ apply each to a fresh Yjs doc            → base state @ snapVersion
  ├─ repo.GetUpdates(since = snapVersion) ─ sort by version ─ replay tail
  │     └─ (neither snapshot nor updates ⇒ ErrDocumentNotFound)
  ├─ verify consistency: state vector decodes AND version ≥ snapVersion
  │     └─ (fails ⇒ ErrConsistencyCheckFailed, doc destroyed)
  └─ Result{ Doc, Version, FromSnapshotID, SnapshotPayloads, UpdatesReplayed,
             StateVector, Consistent, Duration }
```

Recovery is used both to rehydrate an archived (unloaded) document on demand and
to restore state after a crash. CRDT idempotency makes replay safe even if the
snapshot already contains some of the replayed operations.

## 8. Concurrency strategy

- **No global mutex.** Each subsystem owns fine-grained locks:
  - `registry` — one `sync.RWMutex` guarding the document map; all entry field
    mutation happens under it, and reads return **copies** (`DocumentInfo`,
    `Participants`). Mutable fields (e.g. `Version`) are never read through a
    live entry pointer outside the lock.
  - `yjs.DocWrapper` — one `RWMutex` serializing the non-reentrant CRDT document
    (writes exclusive, encodes shared), plus a separate lock for the update hook.
  - `events.Bus` — `RWMutex`; `Publish` is **non-blocking** (drops to full
    subscribers and records `EventDropped`) so a slow consumer can never stall an
    edit.
  - `storage.InMemoryRepository` — its own `RWMutex`.
- **Per-document parallelism.** Distinct documents share nothing but the registry
  map lock, held only for O(1) map operations — thousands of documents edit in
  parallel. The stress test drives 40 documents × 8 participants concurrently.
- **Atomic compound operations.** `MarkEdited` (version++, edit++, dirty,
  transition) and `RecordSnapshot` (meta, reset, clear-dirty) each execute under
  a single lock acquisition, so no interleaving can observe a torn state.
- **Update-hook contract.** The CRDT observer runs while the document write lock
  is held; it does only cheap, non-reentrant work (metrics + a non-blocking
  publish) and must never call back into the wrapper.
- Verified with `go test -race` including the high-concurrency stress test.

## 9. Memory ownership model

- The **registry** owns every live `DocumentEntry` and its `yjs.DocWrapper` for
  the document's active lifetime. Nothing else stores a document past the call in
  which it obtained it.
- `GetOrCreateDocument` returns a **borrowed** `*yjs.DocWrapper`. Callers may use
  it but must not retain it across an archive/delete; a subsequent
  `GetOrCreateDocument` returns the current instance (recovering if needed).
- **Release points:** `ArchiveDocument` and `DeleteDocument` call
  `DocWrapper.Destroy()` (releasing the CRDT doc and update observer) *after*
  unregistering, so no new borrow can begin. Recovery destroys its partial
  document on any failure path.
- Read models (`DocumentInfo`, `Statistics`, `types.Participant` slices) are
  value copies — safe to hold and pass across goroutines indefinitely.
- Durable bytes (updates, snapshots) are owned by the `Repository`; the engine
  copies update payloads into the log and never mutates them afterward.

## 10. Future integration points

| Area | Seam already in place | What connects later |
|------|-----------------------|---------------------|
| **Execution infrastructure** | `events.Bus` (`UpdateApplied`, `DocumentSaved`) + `GetOrCreateDocument` returning the live doc | Runner subscribes to edits, reads file contents via `GetTextIn`, streams results back as documents. |
| **PostgreSQL persistence** | `storage.Repository` interface; engine is 100% in-memory today | Implement `Repository` over `pgx`; inject via `Params.Repo`. No engine change. |
| **Redis replication** | `events.Bus` publish/subscribe + binary `Update` payloads | A bridge subscribes to `UpdateGenerated` and republishes to peers; inbound updates enter via `ApplyIncrementalUpdate`. |
| **Multi-file collaboration** | `yjs.DocWrapper` named text types (`InsertTextIn`/`GetTextIn`/`Files`); `DocumentMetadata.FilePath`; `UpdatePayload.FilePath` | Route per-file updates by `FilePath`; one document per project, many files. |
| **Offline editing** | `sync.Delta` / state-vector exchange / `BatchUpdates` (sequential, lossless flush) | Client queues edits offline, then flushes them as a batch and re-exchanges SVs on reconnect. |
| **Monitoring** | `metrics.Recorder` interface + `logger.Tracer`/`Span` seams | Inject a Prometheus/OpenTelemetry `Recorder` and a real `Tracer`; every lifecycle, sync, snapshot, recovery, and participant event is already instrumented. |

---

## Testing

```
go test ./internal/collaboration/...            # unit + integration
go test -race ./internal/collaboration/...       # race + high-concurrency stress
```

Covered: document creation, concurrent edits, concurrent synchronization,
late-join, reconnect delta, snapshot generation/restoration (full, incremental,
compressed), state-vector sync, recovery (snapshot+log, log-only, unknown),
registry participant lifecycle, race-condition and high-concurrency stress,
storage CRUD/prune, config validation, and event bus semantics.
