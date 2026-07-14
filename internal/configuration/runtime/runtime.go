package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cpip/internal/configuration/events"
	"cpip/internal/configuration/metrics"
)

// Listener is notified when runtime configuration values change.
type Listener func(key, value string)

// Engine holds dynamic, runtime-modifiable configurations.
type Engine struct {
	mu        sync.RWMutex
	overrides map[string]string
	listeners []Listener
	metrics   metrics.Recorder
	bus       *events.Bus
}

// NewEngine creates a new dynamic runtime configuration Engine.
func NewEngine(rec metrics.Recorder, bus *events.Bus) *Engine {
	return &Engine{
		overrides: make(map[string]string),
		metrics:   rec,
		bus:       bus,
	}
}

// SetOverride sets or updates a runtime-specific configuration override.
func (e *Engine) SetOverride(ctx context.Context, key, value string) {
	e.mu.Lock()
	oldVal, existed := e.overrides[key]
	e.overrides[key] = value
	listeners := make([]Listener, len(e.listeners))
	copy(listeners, e.listeners)
	e.mu.Unlock()

	e.metrics.Inc(metrics.MetricConfigReloads)

	if !existed || oldVal != value {
		// Notify event bus
		if e.bus != nil {
			e.bus.Publish(events.Event{
				Type:      events.RuntimeConfigUpdated,
				Timestamp: time.Now(),
				Key:       key,
				Detail:    fmt.Sprintf("Override updated. Old: %q, New: %q", oldVal, value),
			})
		}

		// Trigger active listeners asynchronously
		for _, l := range listeners {
			go l(key, value)
		}
	}
}

// RemoveOverride clears a runtime-specific configuration override.
func (e *Engine) RemoveOverride(ctx context.Context, key string) {
	e.mu.Lock()
	oldVal, existed := e.overrides[key]
	delete(e.overrides, key)
	listeners := make([]Listener, len(e.listeners))
	copy(listeners, e.listeners)
	e.mu.Unlock()

	if existed {
		if e.bus != nil {
			e.bus.Publish(events.Event{
				Type:      events.RuntimeConfigUpdated,
				Timestamp: time.Now(),
				Key:       key,
				Detail:    fmt.Sprintf("Override removed. Old: %q", oldVal),
			})
		}

		for _, l := range listeners {
			go l(key, "")
		}
	}
}

// GetOverride retrieves a runtime override if present.
func (e *Engine) GetOverride(key string) (string, bool) {
	e.mu.RLock()
	defer wRUnlock(&e.mu)
	val, ok := e.overrides[key]
	return val, ok
}

// AllOverrides returns a copy of all current overrides.
func (e *Engine) AllOverrides() map[string]string {
	e.mu.RLock()
	defer wRUnlock(&e.mu)
	out := make(map[string]string, len(e.overrides))
	for k, v := range e.overrides {
		out[k] = v
	}
	return out
}

// Watch registers a listener callback to monitor runtime overrides.
func (e *Engine) Watch(l Listener) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.listeners = append(e.listeners, l)
}

// Helper to handle defer lock releasing.
func wRUnlock(mu *sync.RWMutex) {
	mu.RUnlock()
}
