// Package versioning implements the Version Manager: it owns an artifact's
// lineage — the ordered history of versions that share a LineageID — and the
// operations over it: commit a new version, list history, fetch a specific
// version, identify the head, roll back, and identify prune candidates under a
// version-retention policy.
//
// A version is immutable once committed (its bytes are content-addressed); a new
// upload of the "same" logical artifact appends a new version rather than
// mutating the old one. Rollback is a non-destructive head move: it re-points
// is_latest at an existing version, preserving the full history. Branching is a
// future stage — the LineageID model is designed to accommodate it.
package versioning

import (
	"context"

	"cpip/internal/id"
	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/events"
	"cpip/internal/storage/logger"
	"cpip/internal/storage/metadata"
	"cpip/internal/storage/metrics"
	"cpip/internal/storage/retention"
)

// Manager is the Version Manager.
type Manager struct {
	store metadata.Store
	ret   *retention.Manager
	bus   *events.Bus
	rec   metrics.Recorder
	log   *logger.Logger
}

// Params configures a Manager.
type Params struct {
	Store     metadata.Store
	Retention *retention.Manager
	Events    *events.Bus
	Metrics   metrics.Recorder
	Logger    *logger.Logger
}

// New constructs a Version Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		store: p.Store,
		ret:   p.Retention,
		bus:   p.Events,
		rec:   rec,
		log:   p.Logger.With("subsystem", "versioning"),
	}
}

// NewLineageID mints a fresh lineage identifier for a first-of-its-kind artifact.
func NewLineageID() string { return id.NewWithPrefix("lin") }

// Commit atomically appends a as the newest version of its lineage, making it
// the head. The assigned version is written back into a. It emits
// ArtifactVersionCreated so subscribers (coordination, analytics) converge.
func (m *Manager) Commit(ctx context.Context, a *artifacts.Artifact) error {
	if err := m.store.AppendVersion(ctx, a); err != nil {
		return err
	}
	m.rec.IncCounter(metrics.MetricVersionCreated, map[string]string{"type": string(a.Type)})
	m.log.Version(ctx, a.LineageID, a.ID, a.Version, "committed")
	m.bus.Emit(events.ArtifactVersionCreated, "versioning", func(e *events.Event) {
		e.ArtifactID = a.ID
		e.LineageID = a.LineageID
		e.Bucket = a.Bucket
		e.Key = a.ObjectKey
		e.Owner = a.Owner
		e.Payload = a.Version
	})
	return nil
}

// History returns every version of a lineage, ordered ascending by version.
func (m *Manager) History(ctx context.Context, lineageID string) ([]*artifacts.Artifact, error) {
	hist, err := m.store.ListLineage(ctx, lineageID)
	if err != nil {
		return nil, err
	}
	if len(hist) == 0 {
		return nil, artifacts.ErrNotFound
	}
	return hist, nil
}

// Latest returns the head version of a lineage.
func (m *Manager) Latest(ctx context.Context, lineageID string) (*artifacts.Artifact, error) {
	return m.store.GetLatest(ctx, lineageID)
}

// Version returns a specific version within a lineage.
func (m *Manager) Version(ctx context.Context, lineageID string, version int64) (*artifacts.Artifact, error) {
	return m.store.GetVersion(ctx, lineageID, version)
}

// Rollback re-points the lineage head at an existing version without deleting
// history. The target must exist and be serveable. Returns the promoted version.
func (m *Manager) Rollback(ctx context.Context, lineageID string, targetVersion int64) (*artifacts.Artifact, error) {
	target, err := m.store.GetVersion(ctx, lineageID, targetVersion)
	if err != nil {
		return nil, err
	}
	if !target.State.Serveable() {
		return nil, artifacts.ErrVersionNotFound
	}
	if err := m.store.SetLatest(ctx, lineageID, target.ID); err != nil {
		return nil, err
	}
	m.rec.IncCounter(metrics.MetricVersionRollback, nil)
	m.log.Version(ctx, lineageID, target.ID, targetVersion, "rolled_back")
	m.bus.Emit(events.ArtifactRolledBack, "versioning", func(e *events.Event) {
		e.ArtifactID = target.ID
		e.LineageID = lineageID
		e.Payload = targetVersion
	})
	target.IsLatest = true
	return target, nil
}

// PruneCandidates returns versions eligible for pruning under a RetainVersions
// policy (oldest beyond the cap, excluding head and legally-held). It is
// read-only; the caller performs deletion so reference counting is honored.
func (m *Manager) PruneCandidates(ctx context.Context, lineageID string, policy artifacts.RetentionPolicy) ([]*artifacts.Artifact, error) {
	maxV := m.ret.MaxVersions(policy)
	if maxV <= 0 {
		return nil, nil
	}
	hist, err := m.store.ListLineage(ctx, lineageID)
	if err != nil {
		return nil, err
	}
	return m.ret.PruneVictims(hist, maxV), nil
}
