package plugins

import (
	"context"
	"sync"
	"time"

	"cpip/internal/languages/compiler"
	"cpip/internal/languages/config"
	"cpip/internal/languages/events"
	"cpip/internal/languages/metrics"
	"cpip/internal/languages/runtime"
	"cpip/internal/languages/sdk"
	"cpip/internal/languages/types"
)

// ManagedPlugin wraps an underlying SDK plugin with lifecycle tracking, event publishing, and telemetry.
type ManagedPlugin struct {
	mu       sync.RWMutex
	sdk      sdk.PluginSDK
	state    State
	stats    types.LanguageStats
	bus      *events.Bus
	recorder metrics.Recorder
}

// NewManagedPlugin creates a new thread-safe ManagedPlugin.
func NewManagedPlugin(sdk sdk.PluginSDK, bus *events.Bus, recorder metrics.Recorder) *ManagedPlugin {
	p := &ManagedPlugin{
		sdk:      sdk,
		state:    StateRegistered,
		bus:      bus,
		recorder: recorder,
	}
	p.recorder.RecordRegister(sdk.Metadata().ID)
	p.publish(events.PluginRegistered, nil)
	return p
}

// State returns the current plugin state.
func (p *ManagedPlugin) State() State {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// Stats returns the execution stats of the plugin.
func (p *ManagedPlugin) Stats() types.LanguageStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// Transition moves the plugin to the next state, publishing events and metrics.
func (p *ManagedPlugin) Transition(next State) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := ValidateTransition(p.state, next); err != nil {
		return err
	}

	p.state = next

	// Trigger metrics and events according to state changes
	meta := p.sdk.Metadata()
	switch next {
	case StateValidated:
		p.publish(events.PluginValidated, nil)
	case StateLoaded:
		p.recorder.RecordLoad(meta.ID)
		p.publish(events.PluginLoaded, nil)
	case StateInitialized:
		p.publish(events.PluginInitialized, nil)
	case StateReady:
		p.publish(events.PluginReady, nil)
	case StateUnloaded:
		p.recorder.RecordUnload(meta.ID)
		p.publish(events.PluginUnloaded, nil)
	case StateRemoved:
		p.publish(events.PluginRemoved, nil)
	}

	return nil
}

func (p *ManagedPlugin) Initialize(ctx context.Context, cfg config.PluginConfig) error {
	if err := p.sdk.Initialize(ctx, cfg); err != nil {
		return err
	}
	return p.Transition(StateInitialized)
}

func (p *ManagedPlugin) Validate(ctx context.Context, source string) error {
	return p.sdk.Validate(ctx, source)
}

func (p *ManagedPlugin) Compile(ctx context.Context, req compiler.CompileRequest) (compiler.CompileResult, error) {
	start := time.Now()
	res, err := p.sdk.Compile(ctx, req)
	dur := time.Since(start)

	p.mu.Lock()
	p.stats.TotalCompilationTime += dur.Milliseconds()
	p.mu.Unlock()

	return res, err
}

func (p *ManagedPlugin) Run(ctx context.Context, input runtime.RunInput) (runtime.RunResult, error) {
	meta := p.sdk.Metadata()
	p.publish(events.PluginExecutionStarted, input.SessionID)

	p.mu.Lock()
	originalState := p.state
	p.stats.TotalExecutions++
	p.mu.Unlock()

	_ = p.Transition(StateExecuting)

	start := time.Now()
	res, err := p.sdk.Run(ctx, input)
	dur := time.Since(start)

	p.mu.Lock()
	p.stats.TotalExecutionTime += dur.Milliseconds()
	success := err == nil && res.ExitCode == 0
	if success {
		p.stats.SuccessfulExecutions++
	} else {
		p.stats.FailedExecutions++
	}
	p.mu.Unlock()

	_ = p.Transition(originalState)

	p.recorder.RecordExecution(meta.ID, success, 0, dur.Milliseconds())
	p.publish(events.PluginExecutionCompleted, res)

	return res, err
}

func (p *ManagedPlugin) Cleanup(ctx context.Context, sessionID string) error {
	return p.sdk.Cleanup(ctx, sessionID)
}

func (p *ManagedPlugin) Capabilities() []string {
	return p.sdk.Capabilities()
}

func (p *ManagedPlugin) Metadata() types.LanguageMetadata {
	return p.sdk.Metadata()
}

func (p *ManagedPlugin) Health(ctx context.Context) error {
	return p.sdk.Health(ctx)
}

func (p *ManagedPlugin) Version() string {
	return p.sdk.Version()
}

func (p *ManagedPlugin) publish(t events.Type, payload any) {
	meta := p.sdk.Metadata()
	p.bus.Publish(events.Event{
		Type:      t,
		PluginID:  meta.ID,
		Version:   meta.PluginVersion,
		Timestamp: time.Now(),
		Payload:   payload,
	})
}
