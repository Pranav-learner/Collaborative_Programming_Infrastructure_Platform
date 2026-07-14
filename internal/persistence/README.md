# PostgreSQL Persistence Layer — Stage 4 Module 1

> Production-grade, modular persistence architecture for CPIP.  
> Business services never write SQL. All durable state flows through typed repositories,
> the Unit of Work, and the Transaction Manager.

---

## Architecture Overview

```
Business Service
      ↓
Repository Interface           (domain-oriented, SQL-free)
      ↓
Unit of Work                   (transaction boundary, repository resolution)
      ↓
Transaction Manager            (isolation levels, rollback, retry)
      ↓
PostgreSQL Adapter             (connection pool, health check, prepared stmts)
      ↓
PostgreSQL
```

### Design Principles

| Principle | Implementation |
|---|---|
| **No SQL in business code** | Repositories encapsulate all queries |
| **Optimistic locking** | Version column + conflict detection on every UPDATE |
| **Soft delete** | `deleted_at` column, filtered out by default |
| **Audit trail** | Every mutation can be recorded via the audit framework |
| **Decoupled** | `repository.Executor` interface abstracts `sql.DB` and `sql.Tx` |
| **Context propagation** | Transaction context flows through `context.Context` |
| **Middleware** | Logging and metrics wrap any Executor transparently |

---

## Package Structure

```
internal/persistence/
├── audit/           Entity mutation audit recording
├── config/          Connection pool and behavior configuration
├── events/          Persistence lifecycle event bus
├── locking/         Optimistic locking conflict detection and retry
├── logger/          Structured logging hooks (slog-based)
├── metrics/         Recorder interface and in-memory implementation
├── middleware/      Logging and metrics Executor decorators
├── migrations/      Schema migration runner with advisory locking
├── postgres/        PostgreSQL adapter (sql.DB wrapper)
├── query/           Typed query builder (filters, sorts, pagination)
├── repository/      Domain entity types and repository interfaces
│   └── postgres/    PostgreSQL implementations of all repositories
├── schema/          Schema version inspector
├── transactions/    Transaction Manager with context injection
└── unitofwork/      Unit of Work: atomic multi-repository operations
```

---

## Repository Architecture

Six domain repositories are defined as interfaces in `repository/` and
implemented against PostgreSQL in `repository/postgres/`:

| Repository | Entity | Features |
|---|---|---|
| `RoomRepository` | rooms, participants | CRUD, soft delete, restore, JSONB metadata |
| `DocumentRepository` | documents | CRUD, soft delete, optimistic lock |
| `ExecutionRepository` | executions | Create, get, list with filters |
| `SandboxRepository` | sandboxes | CRUD, soft delete, optimistic lock |
| `UserSessionRepository` | user_sessions | CRUD, lookup by token, optimistic lock |
| `ArtifactMetadataRepository` | artifact_metadata | CRUD, optimistic lock |

All repositories accept `repository.Executor` — meaning they work identically
whether called inside a transaction (`sql.Tx`) or directly against the pool
(`sql.DB`).

---

## Unit of Work Flow

```
uow.Execute(ctx, func(ctx, provider) error {
    room := provider.Rooms().GetByID(ctx, id)    // reads via tx
    room.Name = "New Name"
    provider.Rooms().Update(ctx, room)            // writes via tx
    provider.Documents().Create(ctx, doc)         // same tx
    return nil                                    // → COMMIT
})
// If fn returns error or panics → automatic ROLLBACK
```

**Key behaviors:**
- Transaction is created once per `Execute` call
- All repositories from the provider share the same `sql.Tx`
- Nested calls to `Execute` propagate the existing transaction (no savepoints)
- `ExecuteReadOnly` opens a `ReadOnly` transaction for query-heavy paths
- `ExecuteWithOptions` allows custom isolation levels

---

## Transaction Lifecycle

1. `TransactionManager.ExecuteInTx()` begins a `sql.Tx`
2. The `sql.Tx` is injected into `context.Context` via `InjectTx`
3. All repositories call `ExtractTx(ctx)` to find the active transaction
4. On success → `tx.Commit()` → emit `TransactionCommitted` event
5. On error or panic → `tx.Rollback()` → emit `TransactionRolledBack` event

---

## Migration System

Migrations are registered in `migrations.Registry` as ordered Go structs with
`Up` and `Down` SQL scripts. The runner:

1. Acquires PostgreSQL advisory lock `7492104` (serializes concurrent boots)
2. Creates `schema_migrations` table if absent
3. Reads applied versions
4. Executes pending `Up` scripts in order
5. Records each version in `schema_migrations`
6. Commits atomically (all-or-nothing)

**Rollback** reverses all applied migrations in descending order.

### Current migrations

| Version | Name |
|---|---|
| 202607140001 | `create_audit_logs` |
| 202607140002 | `create_rooms_and_participants` |
| 202607140003 | `create_documents` |
| 202607140004 | `create_executions` |
| 202607140005 | `create_sandboxes` |
| 202607140006 | `create_user_sessions` |
| 202607140007 | `create_artifact_metadata` |

---

## Audit System

```go
audit.Record(ctx, executor, audit.LogEntry{
    EntityName: "Room",
    EntityID:   "room-1",
    Action:     "UPDATE",
    ActorID:    "user-123",
    Payload:    diff,
})
```

Writes to the `audit_logs` table with JSONB payload. Can run inside or outside
a transaction. The `Action` field supports: `CREATE`, `UPDATE`, `DELETE`, `RESTORE`.

---

## Optimistic Locking

Every entity table carries a `version BIGINT` column. On UPDATE:

```sql
UPDATE rooms SET ..., version = version + 1
WHERE id = $1 AND version = $2;
```

If `RowsAffected == 0`, the repository returns `locking.ErrOptimisticLockConflict`.
Callers wrap retryable operations with:

```go
locking.RetryConflict(ctx, maxRetries, delay, func() error {
    return uow.Execute(ctx, func(ctx, p) error {
        fresh := p.Rooms().GetByID(ctx, id) // re-read
        fresh.Name = "New"
        return p.Rooms().Update(ctx, fresh)  // retry with fresh version
    })
})
```

---

## Connection Pool

The `postgres.Adapter` wraps `database/sql` with:

| Setting | Default |
|---|---|
| MaxOpenConns | 50 |
| MaxIdleConns | 10 |
| ConnMaxLifetime | 30 min |
| ConnMaxIdleTime | 5 min |
| CommandTimeout | 10 sec |
| RetryInterval | 100 ms |
| MaxRetries | 3 |

Health check via `Ping()`. Statement caching via `Prepare()` with double-checked
locking. Pool stats exposed via `Stats()`.

---

## Concurrency Strategy

- **Connection pool**: `database/sql` manages a bounded pool of connections
- **Optimistic locking**: No row-level locks held during reads; conflicts detected at write time
- **Advisory locks**: Migrations serialize across application instances
- **Context cancellation**: All operations honor `ctx.Done()`
- **Retry with backoff**: Both `Adapter.ExecuteWithRetry` and `locking.RetryConflict`

---

## Future Integration Points

| Capability | How it integrates |
|---|---|
| **Redis Cache** | Wrap repository interfaces with a caching decorator (read-through, write-invalidate) |
| **Object Storage** | `ArtifactMetadataRepository` stores metadata; actual blobs go to S3/GCS via a separate adapter |
| **Read Replicas** | Inject a read-only `sql.DB` into `ExecuteReadOnly`; the `Executor` interface already supports this |
| **Distributed Transactions** | Replace `TransactionManager` with a 2PC coordinator; the `UnitOfWork` interface remains stable |
| **CQRS** | Split `RepositoryProvider` into `CommandProvider` and `QueryProvider`; the query layer already supports projections |
| **Event Sourcing** | Emit `events.PersistenceEvent` from every mutation and persist to an event store |
| **Distributed Coordination** | Use advisory locks or an external coordination service (etcd/ZooKeeper) for cross-node migration safety |
