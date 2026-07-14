// Package retention implements the Retention Manager: the policy engine that
// decides how long an artifact lives, when it expires, when it is archived, and
// whether a legal hold blocks its deletion. It resolves caller-supplied policies
// against configured defaults, stamps concrete expiry timestamps, and evaluates
// artifacts for the cleanup reaper.
//
// Retention is expressed as data (artifacts.RetentionPolicy) so the actual
// deletion/archival is performed elsewhere (cleanup, manager); this package is
// the brain, not the hands. Tiered/cold storage is a future stage — the Archive
// flag is honored today as a lifecycle state, not a physical move.
package retention

import (
	"context"
	"time"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/config"
	"cpip/internal/storage/events"
	"cpip/internal/storage/logger"
	"cpip/internal/storage/metadata"
	"cpip/internal/storage/metrics"
)

// Action is the disposition the reaper should take for an artifact.
type Action string

const (
	// Keep: retention has not elapsed; leave the artifact alone.
	Keep Action = "keep"
	// Archive: expired but policy requests archival rather than deletion.
	Archive Action = "archive"
	// Expire: expired and eligible for deletion.
	Expire Action = "expire"
	// Hold: a legal hold suppresses any expiry/deletion.
	Hold Action = "hold"
)

// Manager is the Retention Manager.
type Manager struct {
	cfg   config.Retention
	store metadata.Store
	bus   *events.Bus
	rec   metrics.Recorder
	log   *logger.Logger
	now   func() time.Time
}

// Params configures a Manager.
type Params struct {
	Config  config.Retention
	Store   metadata.Store
	Events  *events.Bus
	Metrics metrics.Recorder
	Logger  *logger.Logger
}

// New constructs a Retention Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		cfg:   p.Config,
		store: p.Store,
		bus:   p.Events,
		rec:   rec,
		log:   p.Logger.With("subsystem", "retention"),
		now:   time.Now,
	}
}

// Resolve fills an incoming (possibly zero) policy with type-aware defaults and
// stamps a concrete ExpireAt for time-bounded retention. The returned policy is
// what gets persisted on the artifact.
func (m *Manager) Resolve(t artifacts.Type, requested artifacts.RetentionPolicy) artifacts.RetentionPolicy {
	p := requested
	if p.Mode == "" {
		p.Mode = m.cfg.DefaultMode
	}
	switch p.Mode {
	case artifacts.RetainUntil:
		if p.ExpireAt == nil {
			ttl := m.ttlForType(t)
			exp := m.now().UTC().Add(ttl)
			p.ExpireAt = &exp
		}
	case artifacts.RetainVersions:
		if p.MaxVersions <= 0 {
			p.MaxVersions = m.cfg.MaxVersions
		}
	case artifacts.RetainForever:
		p.ExpireAt = nil
	}
	return p
}

func (m *Manager) ttlForType(t artifacts.Type) time.Duration {
	if m.cfg.PerType != nil {
		if d, ok := m.cfg.PerType[t]; ok && d > 0 {
			return d
		}
	}
	return m.cfg.DefaultTTL
}

// Evaluate classifies an artifact's retention disposition as of now.
func (m *Manager) Evaluate(a *artifacts.Artifact, now time.Time) Action {
	if a.Retention.LegalHold {
		return Hold
	}
	if !a.IsExpired(now) {
		return Keep
	}
	if a.Retention.Archive && a.State != artifacts.Archived {
		return Archive
	}
	return Expire
}

// MaxVersions returns the effective version cap for a lineage governed by a
// RetainVersions policy (0 when the policy is not version-based).
func (m *Manager) MaxVersions(p artifacts.RetentionPolicy) int {
	if p.Mode != artifacts.RetainVersions {
		return 0
	}
	if p.MaxVersions > 0 {
		return p.MaxVersions
	}
	return m.cfg.MaxVersions
}

// PruneVictims returns the versions of a lineage that exceed the version cap,
// oldest first, excluding the current head and any legally-held versions. The
// caller (version/cleanup manager) performs the actual deletion.
func (m *Manager) PruneVictims(lineage []*artifacts.Artifact, maxVersions int) []*artifacts.Artifact {
	if maxVersions <= 0 || len(lineage) <= maxVersions {
		return nil
	}
	// lineage is expected ascending by version; keep the newest maxVersions.
	keepFrom := len(lineage) - maxVersions
	var victims []*artifacts.Artifact
	for i := 0; i < keepFrom; i++ {
		v := lineage[i]
		if v.IsLatest || v.Retention.LegalHold || v.State == artifacts.Deleted {
			continue
		}
		victims = append(victims, v)
	}
	return victims
}

// SetLegalHold toggles the legal hold on an artifact, blocking (or unblocking)
// all deletion and expiry. Legal-hold escalation/audit workflows are a future
// stage; the enforcement is wired today.
func (m *Manager) SetLegalHold(ctx context.Context, id string, hold bool) error {
	a, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}
	a.Retention.LegalHold = hold
	if err := m.store.Update(ctx, a); err != nil {
		return err
	}
	action := "legal_hold_set"
	if !hold {
		action = "legal_hold_released"
	}
	m.log.Retention(ctx, id, string(a.Retention.Mode), action, nil)
	return nil
}

// UpdatePolicy replaces the retention policy on an artifact, re-resolving
// defaults and re-stamping expiry.
func (m *Manager) UpdatePolicy(ctx context.Context, id string, policy artifacts.RetentionPolicy) error {
	a, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}
	a.Retention = m.Resolve(a.Type, policy)
	if err := m.store.Update(ctx, a); err != nil {
		return err
	}
	m.bus.Emit(events.RetentionApplied, "retention", func(e *events.Event) {
		e.ArtifactID = id
		e.LineageID = a.LineageID
		e.Payload = a.Retention
	})
	m.rec.IncCounter(metrics.MetricRetentionApplied, map[string]string{"mode": string(a.Retention.Mode)})
	return nil
}
