// Package manager implements the Artifact Manager: the single, cohesive facade
// through which business services create, read, version, retain, delete, and
// restore artifacts. It is the "single source of truth for all binary object
// management" — the layer above it (execution, collaboration, sandbox, REST API)
// depends only on this package and never touches the SDK, a pipeline, or a vendor
// backend directly.
//
// The manager owns orchestration, not mechanism: it delegates byte movement to
// the upload/download pipelines, lineage to the version manager, policy to the
// retention manager, and blob operations to the object storage manager. Its own
// job is the artifact lifecycle — registration, deletion (soft), restoration,
// physical purge, integrity reconciliation, and version pruning — with every
// transition guarded by the state machine and every mutation race-safe.
package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/content"
	"cpip/internal/storage/download"
	"cpip/internal/storage/events"
	"cpip/internal/storage/logger"
	"cpip/internal/storage/metadata"
	"cpip/internal/storage/metrics"
	"cpip/internal/storage/objectstore"
	"cpip/internal/storage/retention"
	"cpip/internal/storage/sdk"
	"cpip/internal/storage/upload"
	"cpip/internal/storage/versioning"
)

// Manager is the Artifact Manager.
type Manager struct {
	objects *objectstore.Manager
	store   metadata.Store
	uploads *upload.Pipeline
	loads   *download.Pipeline
	ver     *versioning.Manager
	ret     *retention.Manager
	bus     *events.Bus
	rec     metrics.Recorder
	log     *logger.Logger
	signTTL time.Duration
	now     func() time.Time
}

// Params configures a Manager. All collaborators are required except the tuning
// fields.
type Params struct {
	Objects      *objectstore.Manager
	Store        metadata.Store
	Upload       *upload.Pipeline
	Download     *download.Pipeline
	Versioning   *versioning.Manager
	Retention    *retention.Manager
	Events       *events.Bus
	Metrics      metrics.Recorder
	Logger       *logger.Logger
	SignedURLTTL time.Duration
}

// New constructs an Artifact Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	ttl := p.SignedURLTTL
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &Manager{
		objects: p.Objects,
		store:   p.Store,
		uploads: p.Upload,
		loads:   p.Download,
		ver:     p.Versioning,
		ret:     p.Retention,
		bus:     p.Events,
		rec:     rec,
		log:     p.Logger.With("subsystem", "manager"),
		signTTL: ttl,
		now:     time.Now,
	}
}

// --- Creation & versioning ---

// Upload registers a new artifact (or a new version of an existing lineage) via
// the upload pipeline. When the resolved retention policy is version-based, it
// prunes versions beyond the cap after a successful commit.
func (m *Manager) Upload(ctx context.Context, req upload.Request) (*upload.Result, error) {
	res, err := m.uploads.Upload(ctx, req)
	if err != nil {
		return nil, err
	}
	m.rec.SetGauge(metrics.MetricArtifactActive, float64(m.activeCount(ctx)), nil)
	if res.Artifact.Retention.Mode == artifacts.RetainVersions {
		m.pruneLineage(ctx, res.Artifact.LineageID, res.Artifact.Retention)
	}
	return res, nil
}

// Download streams an artifact's original bytes via the download pipeline.
func (m *Manager) Download(ctx context.Context, req download.Request) (*download.Output, error) {
	return m.loads.Download(ctx, req)
}

// --- Lookup ---

// Get returns an artifact record by ID.
func (m *Manager) Get(ctx context.Context, id string) (*artifacts.Artifact, error) {
	return m.store.Get(ctx, id)
}

// List runs a metadata query.
func (m *Manager) List(ctx context.Context, q metadata.Query) ([]*artifacts.Artifact, error) {
	return m.store.List(ctx, q)
}

// Count returns the number of artifacts matching a query.
func (m *Manager) Count(ctx context.Context, q metadata.Query) (int64, error) {
	return m.store.Count(ctx, q)
}

// Exists reports whether an artifact record exists and is not soft-deleted.
func (m *Manager) Exists(ctx context.Context, id string) (bool, error) {
	a, err := m.store.Get(ctx, id)
	if errors.Is(err, artifacts.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return a.State != artifacts.Deleted, nil
}

// History returns every version of a lineage.
func (m *Manager) History(ctx context.Context, lineageID string) ([]*artifacts.Artifact, error) {
	return m.ver.History(ctx, lineageID)
}

// Latest returns the head version of a lineage.
func (m *Manager) Latest(ctx context.Context, lineageID string) (*artifacts.Artifact, error) {
	return m.ver.Latest(ctx, lineageID)
}

// Version returns a specific version of a lineage.
func (m *Manager) Version(ctx context.Context, lineageID string, version int64) (*artifacts.Artifact, error) {
	return m.ver.Version(ctx, lineageID, version)
}

// Rollback re-points a lineage's head at an earlier version.
func (m *Manager) Rollback(ctx context.Context, lineageID string, version int64) (*artifacts.Artifact, error) {
	return m.ver.Rollback(ctx, lineageID, version)
}

// --- Retention ---

// UpdateRetention replaces an artifact's retention policy.
func (m *Manager) UpdateRetention(ctx context.Context, id string, policy artifacts.RetentionPolicy) error {
	return m.ret.UpdatePolicy(ctx, id, policy)
}

// SetLegalHold toggles a legal hold, blocking or unblocking deletion.
func (m *Manager) SetLegalHold(ctx context.Context, id string, hold bool) error {
	return m.ret.SetLegalHold(ctx, id, hold)
}

// --- Deletion, restoration, purge ---

// Delete soft-deletes an artifact: it transitions to Deleted and is hidden from
// listings, but its bytes are retained so it can be restored. A legal hold blocks
// deletion. Byte removal is a separate Purge.
func (m *Manager) Delete(ctx context.Context, id string) error {
	a, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if a.Retention.LegalHold {
		return fmt.Errorf("%w: artifact %s", artifacts.ErrLegalHold, id)
	}
	if a.State == artifacts.Deleted {
		return nil // idempotent
	}
	if err := m.transitionToDeleted(ctx, a); err != nil {
		return err
	}
	m.rec.IncCounter(metrics.MetricArtifactDeleted, nil)
	m.log.Lifecycle(ctx, id, string(a.State), string(artifacts.Deleted), nil)
	m.bus.Emit(events.ArtifactDeleted, "manager", func(e *events.Event) {
		e.ArtifactID = id
		e.LineageID = a.LineageID
		e.Bucket = a.Bucket
		e.Key = a.ObjectKey
		e.Owner = a.Owner
		e.Payload = map[string]any{"soft": true}
	})
	return nil
}

// Restore reverses a soft delete, returning the artifact to Available. It fails
// if the underlying bytes have already been purged.
func (m *Manager) Restore(ctx context.Context, id string) error {
	a, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if a.State != artifacts.Deleted {
		return fmt.Errorf("%w: artifact %s is %s, not deleted", artifacts.ErrIllegalTransition, id, a.State)
	}
	ok, err := m.objects.Exists(ctx, a.Bucket, a.ObjectKey)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: bytes for %s were purged", artifacts.ErrObjectNotFound, id)
	}
	if err := m.store.UpdateState(ctx, id, artifacts.Deleted, artifacts.Available); err != nil {
		return err
	}
	m.rec.IncCounter(metrics.MetricArtifactRestored, nil)
	m.log.Lifecycle(ctx, id, string(artifacts.Deleted), string(artifacts.Available), nil)
	m.bus.Emit(events.ArtifactRestored, "manager", func(e *events.Event) {
		e.ArtifactID = id
		e.LineageID = a.LineageID
		e.Bucket = a.Bucket
		e.Key = a.ObjectKey
		e.Owner = a.Owner
	})
	return nil
}

// Purge physically removes an artifact: it deletes the backing object (only when
// no other live artifact references the same content-addressed key) and then
// removes the metadata row. A legal hold blocks purge. This is irreversible.
func (m *Manager) Purge(ctx context.Context, id string) error {
	a, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if a.Retention.LegalHold {
		return fmt.Errorf("%w: artifact %s", artifacts.ErrLegalHold, id)
	}
	// Mark deleting (best-effort; ignore if already terminal) so concurrent
	// readers stop serving it.
	if a.State != artifacts.Deleted && a.State != artifacts.Deleting {
		_ = m.store.UpdateState(ctx, id, a.State, artifacts.Deleting)
	}
	// Reference counting: delete bytes only if this is the last reference.
	refs, err := m.store.CountReferences(ctx, a.Bucket, a.ObjectKey)
	if err != nil {
		return err
	}
	if refs <= 1 {
		if err := m.objects.Delete(ctx, a.Bucket, a.ObjectKey); err != nil {
			return fmt.Errorf("%w: delete object: %v", artifacts.ErrBackendUnavailable, err)
		}
	}
	if err := m.store.Delete(ctx, id); err != nil {
		return err
	}
	m.rec.IncCounter(metrics.MetricArtifactDeleted, map[string]string{"purge": "true"})
	m.log.Lifecycle(ctx, id, string(a.State), "purged", nil)
	m.bus.Emit(events.ArtifactDeleted, "manager", func(e *events.Event) {
		e.ArtifactID = id
		e.LineageID = a.LineageID
		e.Bucket = a.Bucket
		e.Key = a.ObjectKey
		e.Owner = a.Owner
		e.Payload = map[string]any{"purged": true, "bytes_removed": refs <= 1}
	})
	return nil
}

func (m *Manager) transitionToDeleted(ctx context.Context, a *artifacts.Artifact) error {
	// Two-step guarded transition current → Deleting → Deleted.
	if a.State != artifacts.Deleting {
		if err := m.store.UpdateState(ctx, a.ID, a.State, artifacts.Deleting); err != nil {
			return err
		}
	}
	return m.store.UpdateState(ctx, a.ID, artifacts.Deleting, artifacts.Deleted)
}

// --- Integrity & reconciliation ---

// Verify performs an end-to-end integrity check: it downloads the object,
// decompresses it, and confirms the SHA-256 matches the recorded content hash.
func (m *Manager) Verify(ctx context.Context, id string) error {
	a, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}
	out, err := m.loads.Download(ctx, download.Request{ArtifactID: id, Verify: true})
	if err != nil {
		if errors.Is(err, artifacts.ErrIntegrityMismatch) {
			// Quarantine the corrupted artifact.
			_ = m.store.UpdateState(ctx, id, a.State, artifacts.Corrupted)
		}
		return err
	}
	_ = out.Body.Close()
	m.log.Integrity(ctx, id, a.ContentHash, a.ContentHash, true)
	return nil
}

// Reconcile checks that the metadata record and the physical object agree
// (object present). It reports ErrMetadataInconsistent when the object is missing
// for a serveable artifact — the signal a subscriber or operator acts on.
func (m *Manager) Reconcile(ctx context.Context, id string) error {
	a, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if !a.State.Serveable() {
		return nil
	}
	ok, err := m.objects.Exists(ctx, a.Bucket, a.ObjectKey)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %s missing object %s/%s", artifacts.ErrMetadataInconsistent, id, a.Bucket, a.ObjectKey)
	}
	return nil
}

// --- Signed URLs (direct client transfers) ---

// SignedDownloadURL mints a time-limited GET URL for an artifact's object. It is
// only meaningful for uncompressed objects (the client receives raw stored
// bytes); for compressed artifacts, prefer Download which decompresses.
func (m *Manager) SignedDownloadURL(ctx context.Context, id string) (string, error) {
	a, err := m.store.Get(ctx, id)
	if err != nil {
		return "", err
	}
	if !a.State.Serveable() {
		return "", fmt.Errorf("%w: %s is %s", artifacts.ErrDownloadFailed, id, a.State)
	}
	return m.objects.SignedURL(ctx, a.Bucket, a.ObjectKey, sdk.SignedURLOptions{Method: sdk.SignedGet, Expiry: m.signTTL})
}

// --- Helpers ---

func (m *Manager) pruneLineage(ctx context.Context, lineageID string, policy artifacts.RetentionPolicy) {
	victims, err := m.ver.PruneCandidates(ctx, lineageID, policy)
	if err != nil {
		m.log.Retention(ctx, "", string(policy.Mode), "prune_scan", err)
		return
	}
	for _, v := range victims {
		if err := m.Purge(ctx, v.ID); err != nil {
			m.log.Retention(ctx, v.ID, string(policy.Mode), "prune", err)
			continue
		}
		m.rec.IncCounter(metrics.MetricVersionPruned, nil)
	}
}

func (m *Manager) activeCount(ctx context.Context) int64 {
	n, err := m.store.Count(ctx, metadata.Query{State: artifacts.Available})
	if err != nil {
		return 0
	}
	return n
}

// ContentDigest exposes the canonical digest of an artifact's content.
func (m *Manager) ContentDigest(a *artifacts.Artifact) content.Digest {
	return content.Digest(a.ContentHash)
}
