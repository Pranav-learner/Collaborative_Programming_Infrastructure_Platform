// Package service is the composition root of the object storage & artifact
// management module — the storage analogue of the cache module's state.Manager.
// It constructs and wires every subsystem (registry, object storage manager,
// compression, retention, versioning, upload/download pipelines, artifact
// manager, cleanup reaper) from a single Config and a handful of injected
// dependencies, then exposes them behind one cohesive Service facade.
//
// Business services depend on THIS package (or the interfaces it hands out),
// never on a vendor SDK. The design realizes the module's layering:
//
//	Business Services → Artifact Manager → Storage SDK → Storage Adapter → MinIO/S3/GCS/…
//
// A Service can be built three ways for its metadata system-of-record, in
// priority order: an explicitly-injected metadata.Store; a *sql.DB (a Postgres
// store is constructed and migrated); or neither, yielding the in-memory
// reference store (valid for tests and single-node use).
package service

import (
	"context"
	"database/sql"
	"log/slog"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/cleanup"
	"cpip/internal/storage/compression"
	"cpip/internal/storage/config"
	"cpip/internal/storage/download"
	"cpip/internal/storage/events"
	"cpip/internal/storage/logger"
	mgr "cpip/internal/storage/manager"
	"cpip/internal/storage/metadata"
	"cpip/internal/storage/metrics"
	"cpip/internal/storage/objectstore"
	"cpip/internal/storage/registry"
	"cpip/internal/storage/retention"
	"cpip/internal/storage/sdk"
	"cpip/internal/storage/upload"
	"cpip/internal/storage/versioning"
)

// Service is the wired storage module facade.
type Service struct {
	cfg config.Config

	reg       *registry.Registry
	objects   *objectstore.Manager
	meta      metadata.Store
	comp      *compression.Manager
	ret       *retention.Manager
	ver       *versioning.Manager
	uploads   *upload.Pipeline
	loads     *download.Pipeline
	artifacts *mgr.Manager
	reaper    *cleanup.Manager

	bus *events.Bus
	rec metrics.Recorder
	log *logger.Logger

	pgStore  *metadata.PostgresStore // non-nil when we built it (for Migrate)
	ownsMeta bool
	ownsReg  bool
}

// Params configures a Service. Only Config is required.
type Params struct {
	Config config.Config

	// Metadata system-of-record selection (priority: Metadata > DB > in-memory).
	Metadata metadata.Store
	DB       *sql.DB

	// Backend selection (priority: Registry > Store > built from Config).
	Registry *registry.Registry
	Store    sdk.ObjectStore

	// Cross-cutting.
	Authorizer download.Authorizer
	Events     *events.Bus
	Metrics    metrics.Recorder
	Logger     *slog.Logger
}

// New constructs and wires the entire module. Call Start after New to provision
// buckets, migrate the schema, and launch the cleanup reaper.
func New(p Params) (*Service, error) {
	cfg, err := p.Config.Validate()
	if err != nil {
		return nil, err
	}

	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	bus := p.Events
	if bus == nil {
		bus = events.NewBus()
	}
	log := logger.New(p.Logger)

	// --- Registry / backend ---
	reg := p.Registry
	ownsReg := false
	if reg == nil {
		if p.Store != nil {
			reg, err = registry.New(registry.Params{
				DefaultProvider: cfg.Provider,
				Stores:          map[config.Provider]sdk.ObjectStore{cfg.Provider: p.Store},
				Buckets:         cfg.Buckets,
				TypeBuckets:     cfg.TypeBuckets,
				DefaultBucket:   cfg.DefaultBucket,
			})
		} else {
			reg, err = registry.FromConfig(cfg)
			ownsReg = true
		}
		if err != nil {
			return nil, err
		}
	}

	// --- Metadata system-of-record ---
	meta := p.Metadata
	var pgStore *metadata.PostgresStore
	ownsMeta := false
	if meta == nil {
		if p.DB != nil {
			pgStore = metadata.NewPostgresStore(p.DB)
			meta = pgStore
		} else {
			meta = metadata.NewMemoryStore()
		}
		ownsMeta = true
	}

	// --- Subsystems ---
	objects := objectstore.New(objectstore.Params{Registry: reg, Metrics: rec, Logger: log, Events: bus})
	comp := compression.New(cfg.Compression)
	ret := retention.New(retention.Params{Config: cfg.Retention, Store: meta, Events: bus, Metrics: rec, Logger: log})
	ver := versioning.New(versioning.Params{Store: meta, Retention: ret, Events: bus, Metrics: rec, Logger: log})

	uploads := upload.New(upload.Params{
		Objects: objects, Store: meta, Compression: comp, Retention: ret, Versioning: ver,
		Config: cfg, Events: bus, Metrics: rec, Logger: log,
	})
	loads := download.New(download.Params{
		Objects: objects, Store: meta, Compression: comp, Authorizer: p.Authorizer,
		Events: bus, Metrics: rec, Logger: log,
	})
	artifactMgr := mgr.New(mgr.Params{
		Objects: objects, Store: meta, Upload: uploads, Download: loads,
		Versioning: ver, Retention: ret, Events: bus, Metrics: rec, Logger: log,
		SignedURLTTL: cfg.SignedURLTTL,
	})
	reaper := cleanup.New(cleanup.Params{
		Config: cfg.Cleanup, Store: meta, Objects: objects, Retention: ret, Reaper: artifactMgr,
		Events: bus, Metrics: rec, Logger: log,
	})

	return &Service{
		cfg: cfg, reg: reg, objects: objects, meta: meta, comp: comp, ret: ret, ver: ver,
		uploads: uploads, loads: loads, artifacts: artifactMgr, reaper: reaper,
		bus: bus, rec: rec, log: log, pgStore: pgStore, ownsMeta: ownsMeta, ownsReg: ownsReg,
	}, nil
}

// Start provisions buckets, migrates the Postgres schema (when applicable), and
// launches the cleanup reaper. Idempotent for buckets and schema.
func (s *Service) Start(ctx context.Context) error {
	if s.pgStore != nil {
		if err := s.pgStore.Migrate(ctx); err != nil {
			return err
		}
	}
	if err := s.meta.Ping(ctx); err != nil {
		return err
	}
	if err := s.objects.EnsureBuckets(ctx); err != nil {
		return err
	}
	s.reaper.Start(ctx)
	s.log.Backend(ctx, string(s.cfg.Provider), "storage_service_started", nil)
	return nil
}

// --- Public accessors (the seams business services depend on) ---

// Artifacts returns the Artifact Manager: the primary API for binary objects.
func (s *Service) Artifacts() *mgr.Manager { return s.artifacts }

// Objects returns the Object Storage Manager (blob-level control plane).
func (s *Service) Objects() *objectstore.Manager { return s.objects }

// Metadata returns the metadata system-of-record.
func (s *Service) Metadata() metadata.Store { return s.meta }

// Versioning returns the Version Manager.
func (s *Service) Versioning() *versioning.Manager { return s.ver }

// Retention returns the Retention Manager.
func (s *Service) Retention() *retention.Manager { return s.ret }

// Compression returns the Compression Manager.
func (s *Service) Compression() *compression.Manager { return s.comp }

// Cleanup returns the Cleanup Manager (reaper).
func (s *Service) Cleanup() *cleanup.Manager { return s.reaper }

// Registry returns the routing registry.
func (s *Service) Registry() *registry.Registry { return s.reg }

// Events returns the module event bus (future modules subscribe here).
func (s *Service) Events() *events.Bus { return s.bus }

// Config returns the validated, normalized configuration in effect.
func (s *Service) Config() config.Config { return s.cfg }

// --- Health & lifecycle ---

// Health reports whether the metadata store and every backend are reachable.
func (s *Service) Health(ctx context.Context) Health {
	h := Health{Backends: s.objects.Health(ctx)}
	h.MetadataUp = s.meta.Ping(ctx) == nil
	h.Healthy = h.MetadataUp
	for _, b := range h.Backends {
		if !b.Healthy {
			h.Healthy = false
		}
	}
	return h
}

// Health is the aggregate module health snapshot.
type Health struct {
	Healthy    bool
	MetadataUp bool
	Backends   []objectstore.BackendHealth
}

// Close stops the reaper, closes the event bus, and releases owned resources
// (the registry's backends and the metadata store) — but never resources that
// were injected by the caller.
func (s *Service) Close(ctx context.Context) error {
	s.reaper.Stop()
	s.bus.Close()
	var firstErr error
	if s.ownsReg {
		if err := s.reg.Close(); err != nil {
			firstErr = err
		}
	}
	if s.ownsMeta {
		if err := s.meta.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	_ = ctx
	return firstErr
}

// ensure the reaper's Purger contract is satisfied by the artifact manager.
var _ cleanup.Reaper = (*mgr.Manager)(nil)

// ensure the module error sentinels remain referenced for documentation tools.
var _ = artifacts.ErrNotFound
