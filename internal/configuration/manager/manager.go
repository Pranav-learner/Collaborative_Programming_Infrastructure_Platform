package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cpip/internal/configuration/config"
	"cpip/internal/configuration/events"
	"cpip/internal/configuration/featureflags"
	"cpip/internal/configuration/logger"
	"cpip/internal/configuration/metrics"
	"cpip/internal/configuration/profiles"
	"cpip/internal/configuration/registry"
	"cpip/internal/configuration/runtime"
	"cpip/internal/configuration/secrets"
	"cpip/internal/configuration/validation"
	"cpip/internal/configuration/versioning"
	"cpip/internal/configuration/watcher"
)

// Manager orchestrates all configuration loading, reload, and validation.
type Manager struct {
	mu             sync.RWMutex
	cfg            config.PlatformConfig
	registry       *registry.Registry
	profileMgr     *profiles.ProfileManager
	validator      *validation.Validator
	versions       *versioning.VersionManager
	watcher        *watcher.Watcher
	runtimeEngine  *runtime.Engine
	secretsManager *secrets.SecretManager
	ffPlatform     *featureflags.Platform
	bus            *events.Bus
	metrics        metrics.Recorder
	logger         *logger.Logger
	cancelWatch    context.CancelFunc
}

// NewManager constructs an orchestrator Manager.
func NewManager(
	cfg config.PlatformConfig,
	reg *registry.Registry,
	prof *profiles.ProfileManager,
	val *validation.Validator,
	ver *versioning.VersionManager,
	watch *watcher.Watcher,
	rRun *runtime.Engine,
	sec *secrets.SecretManager,
	ff *featureflags.Platform,
	bus *events.Bus,
	rec metrics.Recorder,
	log *logger.Logger,
) *Manager {
	return &Manager{
		cfg:            cfg,
		registry:       reg,
		profileMgr:     prof,
		validator:      val,
		versions:       ver,
		watcher:        watch,
		runtimeEngine:  rRun,
		secretsManager: sec,
		ffPlatform:     ff,
		bus:            bus,
		metrics:        rec,
		logger:         log,
	}
}

// Load loads all configuration data from registered providers in priority order.
// It resolves environment profiles, applies overrides, validates the schema,
// and records a new version snapshot.
func (m *Manager) Load(ctx context.Context) (*versioning.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	mergedResolved := make(map[string]string)
	providersList := m.registry.All()

	// Load from lowest priority provider first, so higher priority (lowest priority number) overrides it.
	for i := len(providersList) - 1; i >= 0; i-- {
		p := providersList[i]
		data, err := p.Load(ctx)
		if err != nil {
			m.metrics.Inc(metrics.MetricProviderErrors)
			if m.logger != nil {
				m.logger.Error("Failed to load provider config", "provider", p.Name(), "error", err)
			}
			continue
		}
		resolvedData := m.profileMgr.ResolveConfig(data)
		for k, v := range resolvedData {
			mergedResolved[k] = v
		}
	}

	// Incorporate dynamic runtime overrides
	overrides := m.runtimeEngine.AllOverrides()
	for k, v := range overrides {
		mergedResolved[k] = v
	}

	resolved := mergedResolved

	// Validate configuration if enabled
	if m.cfg.EnableValidation {
		m.metrics.Inc(metrics.MetricConfigValidations)
		if err := m.validator.Validate(resolved); err != nil {
			if m.bus != nil {
				m.bus.Publish(events.Event{
					Type:      events.ValidationFailed,
					Timestamp: time.Now(),
					Detail:    err.Error(),
				})
			}
			return nil, err
		}
		if m.bus != nil {
			m.bus.Publish(events.Event{
				Type:      events.ConfigurationValidated,
				Timestamp: time.Now(),
			})
		}
	}

	// Record snapshot
	meta := versioning.ChangeMetadata{
		Actor:       "system",
		Action:      "load",
		Description: "Configuration loaded from providers",
	}
	snap := m.versions.RecordSnapshot(resolved, meta)

	m.metrics.Set(metrics.MetricCurrentVersion, float64(snap.Version))

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.ConfigurationLoaded,
			Timestamp: time.Now(),
			Version:   snap.Version,
			Profile:   string(m.cfg.ActiveProfile),
		})
	}

	return snap, nil
}

// Reload forces a fresh configuration load, diffing it against the current snapshot.
func (m *Manager) Reload(ctx context.Context) (*versioning.Snapshot, error) {
	current := m.versions.Current()
	nextSnap, err := m.Load(ctx)
	if err != nil {
		return nil, err
	}

	if current != nil {
		diff := versioning.DiffSnapshots(current.Data, nextSnap.Data)
		if m.logger != nil {
			m.logger.Info("Configuration reloaded",
				"version", nextSnap.Version,
				"added", len(diff.Added),
				"changed", len(diff.Changed),
				"removed", len(diff.Removed),
			)
		}
	}

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.ConfigurationReloaded,
			Timestamp: time.Now(),
			Version:   nextSnap.Version,
		})
	}

	return nextSnap, nil
}

// Rollback restores configuration to a specific version.
func (m *Manager) Rollback(ctx context.Context, version int) (*versioning.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	targetSnap, err := m.versions.GetByVersion(version)
	if err != nil {
		return nil, err
	}

	// Validate target snapshot content against the schemas
	if m.cfg.EnableValidation {
		if err := m.validator.Validate(targetSnap.Data); err != nil {
			return nil, fmt.Errorf("rollback aborted: target version %d failed validation: %w", version, err)
		}
	}

	// Rollback in-memory overrides back to what they were or record as a new snapshot
	meta := versioning.ChangeMetadata{
		Actor:       "system",
		Action:      "rollback",
		Description: fmt.Sprintf("Rolled back to version %d", version),
	}

	newSnap := m.versions.RecordSnapshot(targetSnap.Data, meta)
	m.metrics.Inc(metrics.MetricConfigRollbacks)
	m.metrics.Set(metrics.MetricCurrentVersion, float64(newSnap.Version))

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.ConfigurationRolledBack,
			Timestamp: time.Now(),
			Version:   newSnap.Version,
			Detail:    fmt.Sprintf("Rolled back to version %d", version),
		})
	}

	return newSnap, nil
}

// StartWatcher starts background polling for watched files.
func (m *Manager) StartWatcher(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cancelWatch != nil {
		return // watcher already running
	}

	watchCtx, cancel := context.WithCancel(ctx)
	m.cancelWatch = cancel

	m.watcher.Start(watchCtx)
}

// StopWatcher shuts down the configuration watcher.
func (m *Manager) StopWatcher() {
	m.mu.Lock()
	cancel := m.cancelWatch
	m.mu.Unlock()

	if cancel != nil {
		cancel()
		m.watcher.Stop()
		m.mu.Lock()
		m.cancelWatch = nil
		m.mu.Unlock()
	}
}

// GetRuntimeEngine returns the dynamic runtime engine.
func (m *Manager) GetRuntimeEngine() *runtime.Engine {
	return m.runtimeEngine
}

// GetSecretManager returns the SecretManager.
func (m *Manager) GetSecretManager() *secrets.SecretManager {
	return m.secretsManager
}

// GetFeatureFlagPlatform returns the FeatureFlagPlatform.
func (m *Manager) GetFeatureFlagPlatform() *featureflags.Platform {
	return m.ffPlatform
}

// Registry returns the provider registry.
func (m *Manager) Registry() *registry.Registry {
	return m.registry
}

// ProfileManager returns the ProfileManager.
func (m *Manager) ProfileManager() *profiles.ProfileManager {
	return m.profileMgr
}

// Validator returns the Validator instance.
func (m *Manager) Validator() *validation.Validator {
	return m.validator
}

// VersionManager returns the VersionManager.
func (m *Manager) VersionManager() *versioning.VersionManager {
	return m.versions
}

// Watcher returns the Watcher.
func (m *Manager) Watcher() *watcher.Watcher {
	return m.watcher
}

// Bus returns the Event Bus.
func (m *Manager) Bus() *events.Bus {
	return m.bus
}

// Metrics returns the metrics recorder.
func (m *Manager) Metrics() metrics.Recorder {
	return m.metrics
}

// Logger returns the Logger.
func (m *Manager) Logger() *logger.Logger {
	return m.logger
}

// Config returns the PlatformConfig.
func (m *Manager) Config() config.PlatformConfig {
	return m.cfg
}

// SetConfig updates PlatformConfig at runtime.
func (m *Manager) SetConfig(cfg config.PlatformConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg
}

// ValidateCurrent runs validation on the current snapshot.
func (m *Manager) ValidateCurrent() error {
	current := m.versions.Current()
	if current == nil {
		return errors.New("no active configuration snapshot to validate")
	}
	return m.validator.Validate(current.Data)
}
