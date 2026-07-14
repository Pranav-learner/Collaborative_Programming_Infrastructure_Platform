// Package artifacts defines the domain model for binary objects managed by the
// storage module: the Artifact record, its type taxonomy, lifecycle state
// machine, and the canonical error set. It is a dependency-free leaf package so
// every other storage package can import it without creating cycles.
//
// An Artifact is the metadata envelope around a stored object. The bytes live in
// object storage (MinIO/S3/filesystem via the Storage SDK); this struct — the
// single source of truth for "what exists" — lives in PostgreSQL via the
// metadata store. The two are bound by (Bucket, ObjectKey) and validated by
// ContentHash.
package artifacts

import "time"

// Artifact is the canonical metadata record for a stored binary object.
type Artifact struct {
	// Identity.
	ID        string `json:"id"`         // stable ULID/UUID, immutable
	ObjectKey string `json:"object_key"` // key within the bucket (often content-addressed)
	Bucket    string `json:"bucket"`     // logical bucket name

	// Content addressing & integrity.
	ContentHash string `json:"content_hash"` // sha256:<hex> of the ORIGINAL (pre-compression) bytes
	Size        int64  `json:"size"`         // original (logical) size in bytes
	ContentType string `json:"content_type"` // MIME type

	// Classification & ownership.
	Type       Type   `json:"type"`
	Owner      string `json:"owner"`       // user/service that owns the artifact
	JobID      string `json:"job_id"`      // execution job that produced it (optional)
	RoomID     string `json:"room_id"`     // collaboration room (optional)
	DocumentID string `json:"document_id"` // collaboration document (optional)
	Language   string `json:"language"`    // programming language (optional)

	// Versioning.
	Version   int64  `json:"version"`    // monotonic version within a lineage
	LineageID string `json:"lineage_id"` // groups all versions of one logical artifact
	IsLatest  bool   `json:"is_latest"`  // whether this is the head version

	// Storage details.
	Compression Compression        `json:"compression"`
	Encryption  EncryptionMetadata `json:"encryption"`
	Retention   RetentionPolicy    `json:"retention"`

	// Lifecycle.
	State     State      `json:"state"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`

	// Extensible attributes.
	Metadata   map[string]string `json:"metadata,omitempty"`
	Statistics Statistics        `json:"statistics"`

	// CDNMetadata is reserved for a future CDN integration (edge URLs, cache
	// keys, TTLs). Populated by a later module; ignored today.
	CDNMetadata map[string]string `json:"cdn_metadata,omitempty"`
}

// Compression captures how an object's bytes are stored on the backend.
type Compression struct {
	Algorithm      Algorithm `json:"algorithm"`
	OriginalSize   int64     `json:"original_size"`
	CompressedSize int64     `json:"compressed_size"`
	Ratio          float64   `json:"ratio"` // compressed/original (1.0 = no gain)
}

// EncryptionMetadata is architecture-only in this stage: fields are defined so
// downstream systems and the DB schema are ready for a future KMS/SSE
// integration, but no encryption is performed here.
type EncryptionMetadata struct {
	Enabled   bool   `json:"enabled"`
	Algorithm string `json:"algorithm,omitempty"` // e.g. "AES256", "aws:kms"
	KeyID     string `json:"key_id,omitempty"`
}

// RetentionMode selects how long an artifact is kept.
type RetentionMode string

const (
	// RetainForever keeps the artifact until explicitly deleted.
	RetainForever RetentionMode = "forever"
	// RetainUntil expires the artifact at ExpireAt.
	RetainUntil RetentionMode = "until"
	// RetainVersions keeps at most MaxVersions of the lineage.
	RetainVersions RetentionMode = "versions"
)

// RetentionPolicy governs expiry, version pruning, legal hold, and archival.
type RetentionPolicy struct {
	Mode        RetentionMode `json:"mode"`
	ExpireAt    *time.Time    `json:"expire_at,omitempty"`
	MaxVersions int           `json:"max_versions,omitempty"`
	// LegalHold, when set, blocks all deletion/expiry regardless of Mode
	// (compliance architecture; enforcement is wired, escalation is future).
	LegalHold bool `json:"legal_hold"`
	// Archive requests a move to cheaper/cold storage on expiry rather than
	// deletion (tiering is a future stage; the flag is honored as a state today).
	Archive bool `json:"archive"`
}

// Statistics accumulates access and processing metrics for an artifact.
type Statistics struct {
	DownloadCount    int64      `json:"download_count"`
	LastAccessedAt   *time.Time `json:"last_accessed_at,omitempty"`
	UploadDurationMs int64      `json:"upload_duration_ms"`
	IntegrityChecks  int64      `json:"integrity_checks"`
}

// Clone returns a deep copy so callers can mutate without affecting the source
// (maps are shared by reference otherwise, which is a classic concurrency bug).
func (a *Artifact) Clone() *Artifact {
	if a == nil {
		return nil
	}
	cp := *a
	if a.Metadata != nil {
		cp.Metadata = make(map[string]string, len(a.Metadata))
		for k, v := range a.Metadata {
			cp.Metadata[k] = v
		}
	}
	if a.CDNMetadata != nil {
		cp.CDNMetadata = make(map[string]string, len(a.CDNMetadata))
		for k, v := range a.CDNMetadata {
			cp.CDNMetadata[k] = v
		}
	}
	if a.DeletedAt != nil {
		t := *a.DeletedAt
		cp.DeletedAt = &t
	}
	if a.Retention.ExpireAt != nil {
		t := *a.Retention.ExpireAt
		cp.Retention.ExpireAt = &t
	}
	if a.Statistics.LastAccessedAt != nil {
		t := *a.Statistics.LastAccessedAt
		cp.Statistics.LastAccessedAt = &t
	}
	return &cp
}

// IsExpired reports whether a RetainUntil policy has elapsed as of now. Legal
// hold suppresses expiry.
func (a *Artifact) IsExpired(now time.Time) bool {
	if a.Retention.LegalHold {
		return false
	}
	if a.Retention.Mode == RetainUntil && a.Retention.ExpireAt != nil {
		return now.After(*a.Retention.ExpireAt)
	}
	return false
}
