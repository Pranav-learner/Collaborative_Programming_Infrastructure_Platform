# Object Storage & Artifact Management (Stage 4 · Module 3)

The storage module is the platform's **single source of truth for all binary
object management**. Every log, binary, snapshot, archive, template, and uploaded
file that CPIP produces or consumes flows through the **Artifact Manager**, which
sits on top of a vendor-neutral **Storage SDK**. Business logic never touches a
vendor SDK — MinIO (the default), AWS S3, and the local filesystem are
interchangeable behind one interface, and GCS / Azure Blob are drop-in future
adapters.

```
Business Services  (execution · collaboration · sandbox · REST API)
        │
        ▼
Artifact Manager           ← lifecycle, versioning, retention, deletion, restore
        │
        ├── Upload Pipeline / Download Pipeline
        ├── Version Manager · Retention Manager · Cleanup Manager
        ├── Object Storage Manager   ← blob control plane (buckets, health, integrity)
        └── Metadata Store (PostgreSQL)   ← durable system-of-record
        │
        ▼
Storage SDK  (sdk.ObjectStore interface)
        │
        ▼
Storage Adapter   →   MinIO · AWS S3 · Filesystem · (future: GCS · Azure Blob)
```

Two facts define the design:

1. **Bytes vs. metadata are split.** Object *bytes* live in object storage; the
   *metadata* (identity, ownership, versions, retention, lifecycle, statistics)
   lives in PostgreSQL. They are bound by `(bucket, object_key)` and validated by
   a SHA-256 `content_hash`.
2. **Objects are content-addressed.** An object's storage key is derived from the
   SHA-256 of its bytes (`cas/aa/bb/<hash>`), so identical content is stored once
   and referenced by many artifacts (deduplication), and integrity is verifiable
   at any time.

## Packages

| Package | Responsibility |
|---|---|
| `artifacts` | Domain model: `Artifact`, type taxonomy, lifecycle state machine, canonical error set. Dependency-free leaf. |
| `sdk` | The **Storage SDK** — the `ObjectStore` interface every backend implements. THE decoupling seam. |
| `config` | Configuration surface (provider, buckets, compression, retention, cleanup, limits) with validation. |
| `adapters/minio` | Default backend. Thin path-style configuration over the S3 adapter. |
| `adapters/s3` | Stdlib-only S3-compatible adapter (SigV4 signer, XML wire types). |
| `adapters/filesystem` | Dependency-free local backend (atomic writes); dev + test workhorse. |
| `content` | **Content addressing** — SHA-256 hashing, streaming hashers, content-addressed keys, integrity verification, dedup primitives. |
| `compression` | **Compression Manager** — codec registry (gzip) + policy engine (compress-if-worth-it). |
| `metadata` | **Metadata Manager** — the `Store` interface + in-memory reference impl + PostgreSQL impl + schema. |
| `registry` | **Artifact Registry** — logical→physical bucket, type→bucket routing, provider→backend directory, adapter builder. |
| `objectstore` | **Object Storage Manager** — blob control plane: buckets, lookup, integrity, health, statistics. |
| `versioning` | **Version Manager** — lineage history, commit, latest, rollback, prune candidates. |
| `retention` | **Retention Manager** — policy resolution, expiry, legal hold, archive, version-cap pruning. |
| `cleanup` | **Cleanup Manager** — scheduled reaper: expiry enforcement, archival, orphan sweep, dry-run. |
| `upload` | **Upload Pipeline** — validate → hash → dedup → compress → store → verify → register → audit. |
| `download` | **Download Pipeline** — authorize → lookup → integrity → fetch → decompress → stream → audit. |
| `manager` | **Artifact Manager** — the cohesive façade over all of the above. |
| `events` | In-process typed event bus (`ArtifactUploaded`, `RetentionApplied`, …). Future modules subscribe. |
| `metrics` | `Recorder` interface + in-memory / no-op recorders. Upload/download/retention/cleanup metrics. |
| `logger` | Structured slog hooks for every subsystem. |
| `middleware` | Context propagation, ownership `Authorizer`, tracing hook. |
| `service` | **Composition root** — wires everything from a single `Config`; the public entry point. |

## Artifact model

Every artifact carries: ID, object key, bucket, content hash, size, content type,
type, owner, job/room/document/language, version + lineage + is-latest,
compression, encryption metadata (architecture-only), retention policy, lifecycle
state, timestamps (created/updated/deleted), free-form metadata, statistics, and
reserved CDN metadata.

**Lifecycle state machine** (`artifacts/lifecycle.go`):

```
pending → uploading → available → archived
                          │           │
                          ├── expired ─┤
                          ▼            ▼
                       corrupted    deleting → deleted → (restore) → available
```

Transitions are guarded; `available`/`archived` are the only serveable states.

## Upload pipeline

```
Validate  → type/size/bucket checks
Materialize → collect bytes (bounded by MaxObjectSize)
Hash      → SHA-256 over ORIGINAL bytes  (content address)
Dedup     → FindByContentHash: reuse existing object + representation, skip PUT
Compress  → policy-driven gzip; revert if gain < MinRatio
Store     → PUT compressed/verbatim bytes under cas/<hash> key
Verify    → optional readback + decompress + re-hash
Register  → AppendVersion (atomic version + head), emit events
Audit     → metrics, structured log, ArtifactCreated/Uploaded events
```

## Download pipeline

```
Lookup    → by artifact id, or lineage + version (0 = latest)
Authorize → pluggable Authorizer (ownership/roles); nil = allow
Serveable → state must be available/archived
Fetch     → GET object stream from the backend
Decompress→ transparent; caller always receives ORIGINAL bytes
Stream    → tee-hash so integrity can be confirmed at EOF (Output.Verify),
            or Verify:true buffers + validates BEFORE returning
Audit     → metrics, log, ArtifactDownloaded event  (reads never write the SoR)
```

## Version management

A **lineage** (`lineage_id`) groups all versions of one logical artifact. Uploads
with the same `LineageID` append monotonic versions; exactly one is the head
(`is_latest`). `AppendVersion` is atomic (transaction + partial-unique index),
so thousands of concurrent version commits never duplicate a version or leave two
heads. **Rollback** re-points the head at an existing version without destroying
history. Version-cap retention prunes the oldest versions beyond `MaxVersions`
(never the head, never a legally-held version).

## Content addressing

`key = cas/<hash[0:2]>/<hash[2:4]>/<hash>[.ext]` over `sha256(original bytes)`.
The two-level fan-out keeps prefix cardinality low at millions of objects.
Identical content ⇒ identical key ⇒ stored once; the dedup stage reuses the
existing physical object (and its compression representation) and only mints a new
metadata record. Reference counting (`CountReferences`) guarantees shared bytes
are removed only when the last referencing artifact is purged.

## Retention

Policies: `forever`, `until` (TTL / explicit `ExpireAt`), `versions` (keep N).
The Retention Manager resolves caller policies against configured defaults and
per-type TTLs, stamps concrete expiry timestamps (denormalized to an indexed
column), and classifies each artifact as *keep / archive / expire / hold*. **Legal
hold** blocks all deletion and expiry (compliance architecture). The Cleanup
Manager runs the enforcement on a schedule with a **dry-run** mode for safe
rollout, and can sweep orphaned backend objects older than a grace period.

## Concurrency

- Every mutation that touches a lineage (`AppendVersion`, `SetLatest`,
  `UpdateState`) is **atomic** — a DB transaction in Postgres, a single mutex
  critical section in the in-memory store. Verified under `go test -race` with
  concurrent same-lineage uploads.
- The upload/download pipelines are **stateless** and safe for thousands of
  concurrent calls; each request is independent.
- Filesystem writes are atomic (temp file + rename) so concurrent writers to the
  same content-addressed key never expose a torn object.
- Reads never mutate the metadata system-of-record (no write amplification);
  access statistics are surfaced via metrics + events.

## Configuration

Driven entirely by `config.Config` (provider, backend credentials, logical→
physical bucket map, type→bucket routing, compression, retention, cleanup,
`MaxObjectSize`, `MultipartThreshold`, `SignedURLTTL`). `config.Default()` targets
a local MinIO; `Validate()` normalizes and rejects nonsensical values.

## Observability

Structured slog logging + a pluggable metrics `Recorder` (Prometheus/OTel/StatsD
adapters plug in; in-memory + no-op ship). Upload, download, compression,
retention, cleanup, integrity, and backend-health metrics are all emitted, plus a
typed event bus for cross-module subscription and a tracing-hook seam.

## Usage

```go
svc, _ := service.New(service.Params{
    Config: config.Default(),          // MinIO by default; set Provider=filesystem for local
    DB:     pgDB,                       // optional: durable Postgres metadata (else in-memory)
})
_ = svc.Start(ctx)                      // provision buckets, migrate schema, start reaper
defer svc.Close(ctx)

res, _ := svc.Artifacts().Upload(ctx, upload.Request{
    Data: logBytes, Type: artifacts.ExecutionLog, Owner: "user-42", JobID: "job-7",
})
out, _ := svc.Artifacts().Download(ctx, download.Request{ArtifactID: res.Artifact.ID})
defer out.Body.Close()
```

## Testing

`go test -race ./internal/storage/...` covers content hashing, compression policy,
metadata versioning/atomicity/guarded-transitions, and a full end-to-end suite
over a real filesystem backend: upload/download round-trips, verified downloads,
deduplication, versioning + rollback, delete/restore/purge, legal hold, retention
cleanup, metadata reconciliation, oversized rejection, event publication, and
concurrent uploads/downloads under the race detector.

## Future integration points

- **Distributed Coordination (Module 4):** subscribe to the event bus; promote the
  in-process bus to cross-node; add per-object distributed locks for cross-node
  cleanup coordination.
- **CDN:** populate the reserved `CDNMetadata`; issue edge URLs via a new
  `GenerateSignedURL` consumer; warm caches on `ArtifactUploaded`.
- **AWS S3 / GCS / Azure Blob:** implement `sdk.ObjectStore`, register in
  `registry.BuildStore`. Zero business-logic change.
- **Backup system:** subscribe to lifecycle events; use `List` + content hashes for
  incremental, dedup-aware backups; replicate content-addressed objects verbatim.
- **Multipart / large objects, tiered (cold) storage, encryption/KMS:** the SDK
  (`MultipartUploader`), retention (`Archive`), and artifact model
  (`EncryptionMetadata`) already carry the seams; these are explicitly future
  stages.
```
