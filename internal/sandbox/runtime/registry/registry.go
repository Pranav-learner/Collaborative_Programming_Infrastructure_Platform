package registry

import (
	"fmt"
	"sync"

	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/runtime/features"
)

// RuntimeRegistry manages registered runtimes in a thread-safe catalog.
type RuntimeRegistry struct {
	mu          sync.RWMutex
	descriptors map[string]runtime.RuntimeDescriptor
}

// NewRuntimeRegistry initializes a new RuntimeRegistry instance.
func NewRuntimeRegistry() *RuntimeRegistry {
	return &RuntimeRegistry{
		descriptors: make(map[string]runtime.RuntimeDescriptor),
	}
}

// Register adds or updates a runtime descriptor in the registry.
func (r *RuntimeRegistry) Register(desc runtime.RuntimeDescriptor) error {
	if desc.RuntimeID == "" {
		return fmt.Errorf("cannot register runtime with empty RuntimeID")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.descriptors[desc.RuntimeID] = desc
	return nil
}

// Get retrieves a runtime descriptor by its ID.
func (r *RuntimeRegistry) Get(id string) (runtime.RuntimeDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	desc, ok := r.descriptors[id]
	if !ok {
		return runtime.RuntimeDescriptor{}, fmt.Errorf("runtime %s not registered", id)
	}
	return desc, nil
}

// List returns all registered descriptors.
func (r *RuntimeRegistry) List() []runtime.RuntimeDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]runtime.RuntimeDescriptor, 0, len(r.descriptors))
	for _, d := range r.descriptors {
		list = append(list, d)
	}
	return list
}

// LookupByCapability returns all runtime IDs that support a given feature capability.
func (r *RuntimeRegistry) LookupByCapability(feature features.Feature) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var matched []string
	for id, desc := range r.descriptors {
		if desc.HasFeature(feature) {
			matched = append(matched, id)
		}
	}
	return matched
}

// GetDefault returns the default runtime descriptor. If none is flagged as default, returns the first one.
func (r *RuntimeRegistry) GetDefault() (runtime.RuntimeDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.descriptors) == 0 {
		return runtime.RuntimeDescriptor{}, fmt.Errorf("no runtimes registered")
	}

	for _, desc := range r.descriptors {
		if desc.DefaultRuntime {
			return desc, nil
		}
	}

	// Fallback to docker if available
	if d, ok := r.descriptors["docker"]; ok {
		return d, nil
	}

	// Return first available
	for _, desc := range r.descriptors {
		return desc, nil
	}

	return runtime.RuntimeDescriptor{}, fmt.Errorf("no default runtime found")
}
