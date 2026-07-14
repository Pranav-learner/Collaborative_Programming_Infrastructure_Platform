// Package cleanup implements the Cleanup Manager: the background reaper that
// enforces retention over time. On a schedule it scans for expired artifacts,
// archives or purges them per policy, and (optionally) sweeps orphaned backend
// objects that no metadata record references.
//
// The reaper is deliberately decoupled from the Artifact Manager via the small
// Reaper interface, so cleanup depends on a capability (Purge), not a concrete
// type. Every destructive action honors a legal hold and a DryRun mode, so the
// reaper can be rolled out in observe-only mode before it is trusted to delete.
package cleanup

import (
	"context"
	"sync"
	"time"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/config"
	"cpip/internal/storage/content"
	"cpip/internal/storage/events"
	"cpip/internal/storage/logger"
	"cpip/internal/storage/metadata"
	"cpip/internal/storage/metrics"
	"cpip/internal/storage/objectstore"
	"cpip/internal/storage/retention"
	"cpip/internal/storage/sdk"
)

// Reaper is the capability the cleanup manager needs to physically remove an
// artifact (bytes + metadata, with reference counting). *manager.Manager
// satisfies it; the interface keeps cleanup decoupled from that concrete type.
type Reaper interface {
	Purge(ctx context.Context, id string) error
}

// Report summarizes one cleanup cycle.
type Report struct {
	Scanned      int
	Archived     int
	Expired      int
	Purged       int
	OrphansFound int
	OrphansSwept int
	Failed       int
	DryRun       bool
	Duration     time.Duration
}

// Manager is the Cleanup Manager.
type Manager struct {
	cfg     config.Cleanup
	store   metadata.Store
	objects *objectstore.Manager
	ret     *retention.Manager
	reaper  Reaper
	bus     *events.Bus
	rec     metrics.Recorder
	log     *logger.Logger
	now     func() time.Time

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	running bool
}

// Params configures a Manager.
type Params struct {
	Config    config.Cleanup
	Store     metadata.Store
	Objects   *objectstore.Manager
	Retention *retention.Manager
	Reaper    Reaper
	Events    *events.Bus
	Metrics   metrics.Recorder
	Logger    *logger.Logger
}

// New constructs a Cleanup Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		cfg:     p.Config,
		store:   p.Store,
		objects: p.Objects,
		ret:     p.Retention,
		reaper:  p.Reaper,
		bus:     p.Events,
		rec:     rec,
		log:     p.Logger.With("subsystem", "cleanup"),
		now:     time.Now,
	}
}

// Start launches the background reaper loop. It is a no-op when cleanup is
// disabled or already running. Stop (or ctx cancellation) halts it.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running || !m.cfg.Enabled {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.done = make(chan struct{})
	m.running = true
	go m.loop(loopCtx)
}

func (m *Manager) loop(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := m.RunOnce(ctx); err != nil {
				m.log.Cleanup(ctx, "cycle_error", 0, 0, 1, 0)
			}
		}
	}
}

// Stop halts the background loop and waits for the in-flight cycle to finish.
func (m *Manager) Stop() {
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.running = false
	m.cancel = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// RunOnce executes a single retention-enforcement cycle: scan expired artifacts,
// then archive or purge each per its policy. Bounded by BatchSize.
func (m *Manager) RunOnce(ctx context.Context) (Report, error) {
	start := m.now()
	rep := Report{DryRun: m.cfg.DryRun}
	m.bus.Emit(events.CleanupStarted, "cleanup", nil)

	now := m.now().UTC()
	expired, err := m.store.FindExpired(ctx, now, m.cfg.BatchSize)
	if err != nil {
		return rep, err
	}
	rep.Scanned = len(expired)
	m.rec.AddCounter(metrics.MetricCleanupScanned, float64(len(expired)), nil)

	for _, a := range expired {
		if ctx.Err() != nil {
			break
		}
		switch m.ret.Evaluate(a, now) {
		case retention.Hold, retention.Keep:
			continue
		case retention.Archive:
			m.archive(ctx, a, &rep)
		case retention.Expire:
			m.expire(ctx, a, &rep)
		}
	}

	rep.Duration = metrics.ObserveDuration(m.rec, metrics.MetricCleanupDuration, start, nil)
	m.log.Cleanup(ctx, "cycle_complete", rep.Scanned, rep.Purged, rep.Failed, rep.Duration)
	m.bus.Emit(events.CleanupCompleted, "cleanup", func(e *events.Event) { e.Payload = rep })
	return rep, nil
}

func (m *Manager) archive(ctx context.Context, a *artifacts.Artifact, rep *Report) {
	if m.cfg.DryRun {
		rep.Archived++
		m.log.Retention(ctx, a.ID, string(a.Retention.Mode), "would_archive", nil)
		return
	}
	if err := m.store.UpdateState(ctx, a.ID, a.State, artifacts.Archived); err != nil {
		rep.Failed++
		m.log.Retention(ctx, a.ID, string(a.Retention.Mode), "archive", err)
		return
	}
	rep.Archived++
	m.rec.IncCounter(metrics.MetricRetentionApplied, map[string]string{"action": "archive"})
	m.bus.Emit(events.RetentionApplied, "cleanup", func(e *events.Event) {
		e.ArtifactID = a.ID
		e.LineageID = a.LineageID
		e.Payload = "archived"
	})
}

func (m *Manager) expire(ctx context.Context, a *artifacts.Artifact, rep *Report) {
	rep.Expired++
	if m.cfg.DryRun {
		m.log.Retention(ctx, a.ID, string(a.Retention.Mode), "would_purge", nil)
		return
	}
	// Move to Expired for observability, then purge (bytes + row, ref-counted).
	_ = m.store.UpdateState(ctx, a.ID, a.State, artifacts.Expired)
	if err := m.reaper.Purge(ctx, a.ID); err != nil {
		rep.Failed++
		m.rec.IncCounter(metrics.MetricCleanupFailed, nil)
		m.log.Retention(ctx, a.ID, string(a.Retention.Mode), "purge", err)
		return
	}
	rep.Purged++
	m.rec.IncCounter(metrics.MetricCleanupDeleted, nil)
	m.rec.IncCounter(metrics.MetricRetentionApplied, map[string]string{"action": "expire"})
	m.bus.Emit(events.RetentionApplied, "cleanup", func(e *events.Event) {
		e.ArtifactID = a.ID
		e.LineageID = a.LineageID
		e.Payload = "expired"
	})
}

// SweepOrphans scans backend objects and deletes those that (a) no live metadata
// record references and (b) are older than the configured OrphanGrace, so an
// in-flight upload is never reaped. It is bounded by maxPerBucket per bucket.
// Content-addressed keys can be shared, so the reference check is by object key.
func (m *Manager) SweepOrphans(ctx context.Context, maxPerBucket int) (Report, error) {
	start := m.now()
	rep := Report{DryRun: m.cfg.DryRun}
	cutoff := m.now().UTC().Add(-m.cfg.OrphanGrace)

	for _, logical := range m.objects.Registry().LogicalBuckets() {
		marker := ""
		scanned := 0
		for {
			if ctx.Err() != nil {
				break
			}
			res, err := m.objects.List(ctx, logical, sdk.ListOptions{Recursive: true, MaxKeys: 1000, StartAfter: marker})
			if err != nil {
				return rep, err
			}
			for _, o := range res.Objects {
				marker = o.Key
				scanned++
				// Only consider content-addressed objects; leave foreign keys alone.
				if !content.IsContentAddressed(o.Key) {
					continue
				}
				if !o.LastModified.IsZero() && o.LastModified.After(cutoff) {
					continue // too fresh; may be an in-flight upload
				}
				refs, err := m.store.CountReferences(ctx, logical, o.Key)
				if err != nil {
					rep.Failed++
					continue
				}
				if refs > 0 {
					continue
				}
				rep.OrphansFound++
				if m.cfg.DryRun {
					m.log.Backend(ctx, "cleanup", "would_sweep_orphan:"+logical+"/"+o.Key, nil)
					continue
				}
				if err := m.objects.Delete(ctx, logical, o.Key); err != nil {
					rep.Failed++
					continue
				}
				rep.OrphansSwept++
				m.rec.IncCounter(metrics.MetricCleanupDeleted, map[string]string{"kind": "orphan"})
			}
			if maxPerBucket > 0 && scanned >= maxPerBucket {
				break
			}
			if !res.IsTruncated || len(res.Objects) == 0 {
				break
			}
		}
	}
	rep.Duration = time.Since(start)
	m.log.Cleanup(ctx, "orphan_sweep_complete", rep.OrphansFound, rep.OrphansSwept, rep.Failed, rep.Duration)
	return rep, nil
}
