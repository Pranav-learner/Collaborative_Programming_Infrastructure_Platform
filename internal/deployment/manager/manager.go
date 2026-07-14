package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cpip/internal/deployment/config"
	"cpip/internal/deployment/events"
	"cpip/internal/deployment/logger"
	"cpip/internal/deployment/metrics"
	"cpip/internal/deployment/profiles"
	"cpip/internal/deployment/providers"
	"cpip/internal/deployment/rollback"
	"cpip/internal/deployment/services"
	"cpip/internal/deployment/validation"
)

// Manager orchestrates all deployment lifecycles, validations, and rollbacks.
type Manager struct {
	mu          sync.RWMutex
	cfg         config.PlatformConfig
	providers   map[string]providers.Provider
	profileMgr  *profiles.ProfileManager
	validator   *validation.Validator
	rollbackReg *rollback.Registry
	bus         *events.Bus
	metrics     metrics.Recorder
	log         *logger.Logger
}

// NewManager constructs a Manager instance.
func NewManager(
	cfg config.PlatformConfig,
	profMgr *profiles.ProfileManager,
	val *validation.Validator,
	rReg *rollback.Registry,
	bus *events.Bus,
	rec metrics.Recorder,
	log *logger.Logger,
) *Manager {
	return &Manager{
		cfg:         cfg,
		providers:   make(map[string]providers.Provider),
		profileMgr:  profMgr,
		validator:   val,
		rollbackReg: rReg,
		bus:         bus,
		metrics:     rec,
		log:         log,
	}
}

// RegisterProvider registers a deployment engine.
func (m *Manager) RegisterProvider(p providers.Provider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.providers[p.Name()] = p
}

// GetProvider retrieves a deployment engine.
func (m *Manager) GetProvider(name string) (providers.Provider, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.providers[name]
	return p, ok
}

// Deploy resolves the profile and target provider, validates the service configurations, and executes deployment.
func (m *Manager) Deploy(ctx context.Context, svcs []services.Service) (providers.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	provName := m.cfg.DefaultProvider
	profile := string(m.cfg.ActiveProfile)

	p, ok := m.providers[provName]
	if !ok {
		return providers.Result{}, fmt.Errorf("default provider %q is not registered", provName)
	}

	// 1. Apply Profile Overrides
	resolved, err := m.profileMgr.ApplyProfile(m.cfg.ActiveProfile, svcs)
	if err != nil {
		return providers.Result{}, fmt.Errorf("failed to apply profile: %w", err)
	}

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.ProfileSelected,
			Timestamp: time.Now(),
			Profile:   profile,
			Provider:  provName,
			Detail:    fmt.Sprintf("Profile %q loaded", profile),
		})
	}

	// 2. Validate Configurations
	if m.cfg.ValidationEnabled {
		vRes, err := m.validator.Validate(resolved)
		if err != nil {
			return providers.Result{}, fmt.Errorf("validation system error: %w", err)
		}
		if !vRes.IsValid {
			errMsg := fmt.Sprintf("validation failed: %s", errors.New(stringsJoin(vRes.Errors, "; ")))
			if m.bus != nil {
				m.bus.Publish(events.Event{
					Type:      events.ValidationFailed,
					Timestamp: time.Now(),
					Profile:   profile,
					Provider:  provName,
					Detail:    errMsg,
				})
			}
			return providers.Result{Success: false, Provider: provName, Profile: profile, Timestamp: time.Now()}, errors.New(errMsg)
		}
		if m.bus != nil {
			m.bus.Publish(events.Event{
				Type:      events.ValidationSucceeded,
				Timestamp: time.Now(),
				Profile:   profile,
				Provider:  provName,
			})
		}
	}

	// 3. Trigger Deployment
	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.DeploymentStarted,
			Timestamp: time.Now(),
			Profile:   profile,
			Provider:  provName,
		})
	}

	res, err := p.Deploy(ctx, profile, resolved)
	if err != nil {
		m.rollbackReg.RecordSnapshot(profile, resolved, fmt.Sprintf("Deployment failed: %v", err), rollback.StatusFailed)
		if m.bus != nil {
			m.bus.Publish(events.Event{
				Type:      events.DeploymentFailed,
				Timestamp: time.Now(),
				Profile:   profile,
				Provider:  provName,
				Detail:    err.Error(),
				Error:     err,
			})
		}
		return res, err
	}

	// 4. Record History and Notify
	snap := m.rollbackReg.RecordSnapshot(profile, resolved, "Deployment succeeded", rollback.StatusSuccess)

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.DeploymentSucceeded,
			Timestamp: time.Now(),
			Profile:   profile,
			Provider:  provName,
			Version:   snap.Version,
		})

		for _, s := range resolved {
			m.bus.Publish(events.Event{
				Type:      events.ServiceDeployed,
				Timestamp: time.Now(),
				Profile:   profile,
				Provider:  provName,
				Detail:    fmt.Sprintf("Service %q deployed", s.Name),
			})
		}
	}

	return res, nil
}

// Update updates the existing deployment.
func (m *Manager) Update(ctx context.Context, svcs []services.Service) (providers.Result, error) {
	return m.Deploy(ctx, svcs)
}

// Rollback restores configurations back to a previous historic version.
func (m *Manager) Rollback(ctx context.Context, targetVersion int) (rollback.RollbackReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	provName := m.cfg.DefaultProvider
	profile := string(m.cfg.ActiveProfile)

	p, ok := m.providers[provName]
	if !ok {
		return rollback.RollbackReport{}, fmt.Errorf("default provider %q is not registered", provName)
	}

	// 1. Fetch Target Snapshot from History
	snap, err := m.rollbackReg.GetByVersion(profile, targetVersion)
	if err != nil {
		return rollback.RollbackReport{}, err
	}

	// Validate target snapshot services config
	if m.cfg.ValidationEnabled {
		vRes, err := m.validator.Validate(snap.Services)
		if err != nil || !vRes.IsValid {
			return rollback.RollbackReport{}, fmt.Errorf("aborted rollback: target version %d has invalid configurations", targetVersion)
		}
	}

	// 2. Trigger Rollback Deployment
	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.RollbackStarted,
			Timestamp: time.Now(),
			Profile:   profile,
			Provider:  provName,
			Version:   targetVersion,
		})
	}

	_, err = p.Deploy(ctx, profile, snap.Services)
	if err != nil {
		return rollback.RollbackReport{}, fmt.Errorf("rollback execution failed: %w", err)
	}

	// Record forward rollback snapshot transaction
	newSnap := m.rollbackReg.RecordSnapshot(profile, snap.Services, fmt.Sprintf("Rolled back to version %d", targetVersion), rollback.StatusSuccess)

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.RollbackCompleted,
			Timestamp: time.Now(),
			Profile:   profile,
			Provider:  provName,
			Version:   newSnap.Version,
			Detail:    fmt.Sprintf("Successfully rolled back to version %d", targetVersion),
		})
	}

	return rollback.RollbackReport{
		Success:       true,
		TargetVersion: targetVersion,
		NewVersion:    newSnap.Version,
		Timestamp:     time.Now(),
		Detail:        fmt.Sprintf("Rolled back to version %d", targetVersion),
	}, nil
}

// Scale updates the replicas count of a target service dynamically.
func (m *Manager) Scale(ctx context.Context, serviceName string, replicas int) (providers.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	provName := m.cfg.DefaultProvider
	p, ok := m.providers[provName]
	if !ok {
		return providers.Result{}, fmt.Errorf("default provider %q is not registered", provName)
	}

	return p.Scale(ctx, serviceName, replicas)
}

// Validate executes configuration verification rules.
func (m *Manager) Validate(ctx context.Context, svcs []services.Service) (validation.ValidationResult, error) {
	resolved, err := m.profileMgr.ApplyProfile(m.cfg.ActiveProfile, svcs)
	if err != nil {
		return validation.ValidationResult{}, err
	}
	return m.validator.Validate(resolved)
}

// Status returns current deployment state and health check stats.
func (m *Manager) Status(ctx context.Context) (providers.StatusResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	provName := m.cfg.DefaultProvider
	p, ok := m.providers[provName]
	if !ok {
		return providers.StatusResult{}, fmt.Errorf("default provider %q is not registered", provName)
	}

	res, err := p.Status(ctx, string(m.cfg.ActiveProfile))
	if err == nil {
		if snap, ok := m.rollbackReg.Current(string(m.cfg.ActiveProfile)); ok {
			res.ActiveVersion = snap.Version
		}
	}
	return res, err
}

// Destroy cleans up the active deployment environment.
func (m *Manager) Destroy(ctx context.Context) (providers.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	provName := m.cfg.DefaultProvider
	p, ok := m.providers[provName]
	if !ok {
		return providers.Result{}, fmt.Errorf("default provider %q is not registered", provName)
	}

	return p.Destroy(ctx, string(m.cfg.ActiveProfile))
}

// Generate creates the deployment config YAMLs or manifests.
func (m *Manager) Generate(ctx context.Context, svcs []services.Service) (providers.GeneratedArtifacts, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	provName := m.cfg.DefaultProvider
	p, ok := m.providers[provName]
	if !ok {
		return providers.GeneratedArtifacts{}, fmt.Errorf("default provider %q is not registered", provName)
	}

	resolved, err := m.profileMgr.ApplyProfile(m.cfg.ActiveProfile, svcs)
	if err != nil {
		return providers.GeneratedArtifacts{}, err
	}

	return p.Generate(ctx, string(m.cfg.ActiveProfile), resolved)
}

// Config returns the PlatformConfig.
func (m *Manager) Config() config.PlatformConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg
}

// SetConfig updates PlatformConfig at runtime.
func (m *Manager) SetConfig(cfg config.PlatformConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg
}

func stringsJoin(elems []string, sep string) string {
	return strings.Join(elems, sep)
}
