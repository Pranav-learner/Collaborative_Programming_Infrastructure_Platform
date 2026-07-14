// Package metadata defines the Metadata Manager: the durable system-of-record
// for artifact metadata. Object BYTES live in object storage (MinIO/S3/fs via the
// Storage SDK); this package owns everything ELSE — identity, ownership,
// relationships, versions, statistics, retention, and lifecycle state — in
// PostgreSQL.
//
// The Store interface is the seam. Two implementations ship:
//
//   - memory.Store  — a fully thread-safe, transactionally-correct in-memory
//     implementation. It is the reference semantics, the test backend, and a
//     valid single-node deployment target.
//   - postgres.Store — the production PostgreSQL implementation (database/sql +
//     pgx), which is the durable, multi-node source of truth.
//
// Every method that mutates a lineage (AppendVersion, SetLatest, UpdateState) is
// atomic with respect to concurrent callers, so thousands of concurrent uploads
// and version creations never corrupt the is_latest invariant or duplicate a
// version number.
package metadata

import (
	"context"
	"time"

	"cpip/internal/storage/artifacts"
)

// SortField selects the ordering column for a List query.
type SortField string

const (
	SortByCreatedAt SortField = "created_at"
	SortByUpdatedAt SortField = "updated_at"
	SortBySize      SortField = "size"
	SortByVersion   SortField = "version"
)

// Query is a filter over the artifact metadata table. Zero-valued fields are
// ignored, so an empty Query matches everything (bounded by Limit).
type Query struct {
	// Equality filters (empty = ignored).
	Owner      string
	JobID      string
	RoomID     string
	DocumentID string
	Bucket     string
	LineageID  string
	Type       artifacts.Type
	State      artifacts.State
	// LatestOnly restricts to head-of-lineage records.
	LatestOnly bool
	// IncludeDeleted includes soft-deleted rows (default: excluded).
	IncludeDeleted bool
	// ContentHash filters by exact digest (dedup / integrity lookups).
	ContentHash string
	// CreatedBefore / CreatedAfter bound the creation time (zero = unbounded).
	CreatedBefore time.Time
	CreatedAfter  time.Time

	// Pagination & ordering.
	Sort       SortField
	Descending bool
	Limit      int
	Offset     int
}

// Store is the durable metadata system-of-record. All methods honor context
// cancellation and return errors wrapping the artifacts.Err* sentinels.
type Store interface {
	// Create inserts a fully-populated artifact record. Returns ErrAlreadyExists
	// on primary-key or (lineage,version) collision.
	Create(ctx context.Context, a *artifacts.Artifact) error

	// AppendVersion atomically assigns the next version in a.LineageID, sets a as
	// the head (is_latest=true), demotes the previous head, and inserts a. The
	// assigned version and IsLatest flag are written back into a. This is the
	// race-free primitive behind every versioned upload.
	AppendVersion(ctx context.Context, a *artifacts.Artifact) error

	// Get returns an artifact by ID (including soft-deleted). ErrNotFound if absent.
	Get(ctx context.Context, id string) (*artifacts.Artifact, error)

	// Update replaces the mutable fields of an existing record by ID.
	Update(ctx context.Context, a *artifacts.Artifact) error

	// UpdateState performs a guarded lifecycle transition from expected → next,
	// atomically. Returns ErrIllegalTransition if the stored state != expected or
	// the transition is not permitted by the state machine.
	UpdateState(ctx context.Context, id string, expected, next artifacts.State) error

	// Delete hard-removes a metadata row (used only after bytes are purged).
	Delete(ctx context.Context, id string) error

	// Get helpers for versioning.
	GetLatest(ctx context.Context, lineageID string) (*artifacts.Artifact, error)
	GetVersion(ctx context.Context, lineageID string, version int64) (*artifacts.Artifact, error)
	ListLineage(ctx context.Context, lineageID string) ([]*artifacts.Artifact, error)

	// SetLatest atomically makes artifactID the head of its lineage (rollback /
	// restore), demoting all siblings.
	SetLatest(ctx context.Context, lineageID, artifactID string) error

	// FindByContentHash returns an existing Available artifact in bucket whose
	// content matches hash, enabling object-level deduplication. ErrNotFound if
	// none exists.
	FindByContentHash(ctx context.Context, bucket, hash string) (*artifacts.Artifact, error)

	// CountReferences returns how many non-deleted artifacts reference the same
	// (bucket, object_key) — the safety check before physically deleting bytes.
	CountReferences(ctx context.Context, bucket, objectKey string) (int, error)

	// List / Count run a Query.
	List(ctx context.Context, q Query) ([]*artifacts.Artifact, error)
	Count(ctx context.Context, q Query) (int64, error)

	// FindExpired returns Available/Archived artifacts whose RetainUntil policy
	// has elapsed as of now (legal holds excluded), bounded by limit. Drives the
	// cleanup reaper.
	FindExpired(ctx context.Context, now time.Time, limit int) ([]*artifacts.Artifact, error)

	// Ping verifies backend connectivity.
	Ping(ctx context.Context) error
	// Close releases resources (the in-memory store owns none).
	Close() error
}
