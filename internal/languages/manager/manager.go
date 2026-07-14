package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cpip/internal/languages/config"
	"cpip/internal/languages/events"
	"cpip/internal/languages/metrics"
	"cpip/internal/languages/plugins"
	"cpip/internal/languages/profiles"
	"cpip/internal/languages/sdk"
	"cpip/internal/languages/templates"
	"cpip/internal/languages/validation"
)

var (
	ErrPluginNotFound = errors.New("plugin not found")
)

// Manager is the composition root and orchestrator of the extensible Language Plugin Framework.
type Manager struct {
	mu        sync.RWMutex
	cfg       config.Config
	registry  *profiles.ProfileRegistry
	templates *templates.TemplateManager
	bus       *events.Bus
	recorder  metrics.Recorder
	plugins   map[string]*plugins.ManagedPlugin
}

// NewManager creates a new Manager instance.
func NewManager(cfg config.Config, rec metrics.Recorder) *Manager {
	if rec == nil {
		rec = metrics.NewInMemRecorder()
	}
	return &Manager{
		cfg:       cfg,
		registry:  profiles.NewProfileRegistry(cfg),
		templates: templates.NewTemplateManager(),
		bus:       events.NewBus(),
		recorder:  rec,
		plugins:   make(map[string]*plugins.ManagedPlugin),
	}
}

// RegisterPlugin validates and registers a language plugin SDK to the framework.
func (m *Manager) RegisterPlugin(sdk sdk.PluginSDK) error {
	meta := sdk.Metadata()

	// 1. Validation Step
	if m.cfg.ValidationEnabled {
		if err := validation.ValidateMetadata(meta, m.cfg.VersionPolicy); err != nil {
			return fmt.Errorf("metadata validation failed: %w", err)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already registered
	if _, exists := m.plugins[meta.ID]; exists {
		return fmt.Errorf("plugin %s is already registered", meta.ID)
	}

	managed := plugins.NewManagedPlugin(sdk, m.bus, m.recorder)
	m.plugins[meta.ID] = managed

	// Move status validation
	_ = managed.Transition(plugins.StateValidated)

	return nil
}

// LoadPlugin shifts the plugin from Validated to Loaded state.
func (m *Manager) LoadPlugin(pluginID string) error {
	p, err := m.getPlugin(pluginID)
	if err != nil {
		return err
	}
	return p.Transition(plugins.StateLoaded)
}

// InitializePlugin loads and initializes the plugin with environment-specific config.
func (m *Manager) InitializePlugin(ctx context.Context, pluginID string, cfg config.PluginConfig) error {
	p, err := m.getPlugin(pluginID)
	if err != nil {
		return err
	}

	if p.State() == plugins.StateValidated {
		if err := p.Transition(plugins.StateLoaded); err != nil {
			return err
		}
	}

	if err := p.Initialize(ctx, cfg); err != nil {
		return fmt.Errorf("plugin initialization failed: %w", err)
	}

	return p.Transition(plugins.StateReady)
}

// UnloadPlugin safely unloads the plugin to transition to Unloaded.
func (m *Manager) UnloadPlugin(pluginID string) error {
	p, err := m.getPlugin(pluginID)
	if err != nil {
		return err
	}
	return p.Transition(plugins.StateUnloaded)
}

// ReloadPlugin executes hot-reloading by unloading, loading, and initializing again.
func (m *Manager) ReloadPlugin(ctx context.Context, pluginID string, cfg config.PluginConfig) error {
	p, err := m.getPlugin(pluginID)
	if err != nil {
		return err
	}

	state := p.State()
	if state == plugins.StateReady || state == plugins.StateInitialized {
		if err := p.Transition(plugins.StateUnloaded); err != nil {
			return err
		}
	}

	if err := p.Transition(plugins.StateLoaded); err != nil {
		return err
	}

	if err := p.Initialize(ctx, cfg); err != nil {
		return err
	}

	m.bus.Publish(events.Event{
		Type:      events.PluginReloaded,
		PluginID:  pluginID,
		Version:   p.Version(),
		Timestamp: time.Now(),
	})

	return p.Transition(plugins.StateReady)
}

// RemovePlugin transitions the plugin to Removed and deletes it from active registry.
func (m *Manager) RemovePlugin(pluginID string) error {
	p, err := m.getPlugin(pluginID)
	if err != nil {
		return err
	}

	if err := p.Transition(plugins.StateRemoved); err != nil {
		return err
	}

	m.mu.Lock()
	delete(m.plugins, pluginID)
	m.mu.Unlock()

	return nil
}

// GetPlugin retrieves a registered plugin by ID.
func (m *Manager) GetPlugin(pluginID string) (*plugins.ManagedPlugin, error) {
	return m.getPlugin(pluginID)
}

// ListPlugins returns all registered plugins.
func (m *Manager) ListPlugins() []*plugins.ManagedPlugin {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]*plugins.ManagedPlugin, 0, len(m.plugins))
	for _, p := range m.plugins {
		list = append(list, p)
	}
	return list
}

// Profiles returns the Profile Registry.
func (m *Manager) Profiles() *profiles.ProfileRegistry {
	return m.registry
}

// Templates returns the Template Manager.
func (m *Manager) Templates() *templates.TemplateManager {
	return m.templates
}

// EventBus returns the framework lifecycle event bus.
func (m *Manager) EventBus() *events.Bus {
	return m.bus
}

// Helper to thread-safely access the map
func (m *Manager) getPlugin(pluginID string) (*plugins.ManagedPlugin, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p, ok := m.plugins[pluginID]
	if !ok {
		return nil, ErrPluginNotFound
	}
	return p, nil
}
