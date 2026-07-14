// Package registry manages the ordered set of registered configuration providers.
package registry

import (
	"fmt"
	"sort"
	"sync"

	"cpip/internal/configuration/providers"
)

// Registry holds all registered providers in priority order.
type Registry struct {
	mu        sync.RWMutex
	providers []providers.Provider
	byName    map[string]providers.Provider
}

// NewRegistry creates an empty provider registry.
func NewRegistry() *Registry {
	return &Registry{
		byName: make(map[string]providers.Provider),
	}
}

// Register adds a provider. Duplicate names are rejected.
func (r *Registry) Register(p providers.Provider) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.byName[p.Name()]; exists {
		return fmt.Errorf("provider %q already registered", p.Name())
	}

	r.providers = append(r.providers, p)
	r.byName[p.Name()] = p

	sort.Slice(r.providers, func(i, j int) bool {
		return r.providers[i].Priority() < r.providers[j].Priority()
	})

	return nil
}

// Get returns a provider by name.
func (r *Registry) Get(name string) (providers.Provider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.byName[name]
	return p, ok
}

// All returns all providers in priority order (lowest priority number = highest precedence).
func (r *Registry) All() []providers.Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]providers.Provider, len(r.providers))
	copy(out, r.providers)
	return out
}

// Count returns the number of registered providers.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.providers)
}
