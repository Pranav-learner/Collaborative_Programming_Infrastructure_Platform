package policies

import (
	"fmt"
	"sync"
)

type MemRegistry struct {
	mu               sync.RWMutex
	securityPolicies map[string]map[int]SecurityPolicy // ID -> Version -> Policy
	resourcePolicies map[string]map[int]ResourcePolicy // ID -> Version -> Policy
}

func NewMemRegistry() *MemRegistry {
	return &MemRegistry{
		securityPolicies: make(map[string]map[int]SecurityPolicy),
		resourcePolicies: make(map[string]map[int]ResourcePolicy),
	}
}

// Security policy operations
func (r *MemRegistry) RegisterSecurityPolicy(policy SecurityPolicy) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if policy.ID == "" {
		return fmt.Errorf("policy ID cannot be empty")
	}
	if policy.Version <= 0 {
		return fmt.Errorf("policy version must be positive")
	}

	versions, exists := r.securityPolicies[policy.ID]
	if !exists {
		versions = make(map[int]SecurityPolicy)
		r.securityPolicies[policy.ID] = versions
	}

	if _, ok := versions[policy.Version]; ok {
		return fmt.Errorf("security policy %s with version %d already registered", policy.ID, policy.Version)
	}

	versions[policy.Version] = policy
	return nil
}

func (r *MemRegistry) GetSecurityPolicy(id string, version int) (SecurityPolicy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, exists := r.securityPolicies[id]
	if !exists {
		return SecurityPolicy{}, fmt.Errorf("security policy not found: %s", id)
	}

	p, ok := versions[version]
	if !ok {
		return SecurityPolicy{}, fmt.Errorf("security policy %s version %d not found", id, version)
	}

	return p, nil
}

func (r *MemRegistry) GetLatestSecurityPolicy(id string) (SecurityPolicy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, exists := r.securityPolicies[id]
	if !exists || len(versions) == 0 {
		return SecurityPolicy{}, fmt.Errorf("security policy not found: %s", id)
	}

	var latest SecurityPolicy
	first := true
	for _, p := range versions {
		if first || p.Version > latest.Version {
			latest = p
			first = false
		}
	}
	return latest, nil
}

func (r *MemRegistry) ListSecurityPolicies() []SecurityPolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []SecurityPolicy
	for _, versions := range r.securityPolicies {
		for _, p := range versions {
			list = append(list, p)
		}
	}
	return list
}

// Resource policy operations
func (r *MemRegistry) RegisterResourcePolicy(policy ResourcePolicy) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if policy.ID == "" {
		return fmt.Errorf("policy ID cannot be empty")
	}
	if policy.Version <= 0 {
		return fmt.Errorf("policy version must be positive")
	}

	versions, exists := r.resourcePolicies[policy.ID]
	if !exists {
		versions = make(map[int]ResourcePolicy)
		r.resourcePolicies[policy.ID] = versions
	}

	if _, ok := versions[policy.Version]; ok {
		return fmt.Errorf("resource policy %s with version %d already registered", policy.ID, policy.Version)
	}

	versions[policy.Version] = policy
	return nil
}

func (r *MemRegistry) GetResourcePolicy(id string, version int) (ResourcePolicy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, exists := r.resourcePolicies[id]
	if !exists {
		return ResourcePolicy{}, fmt.Errorf("resource policy not found: %s", id)
	}

	p, ok := versions[version]
	if !ok {
		return ResourcePolicy{}, fmt.Errorf("resource policy %s version %d not found", id, version)
	}

	return p, nil
}

func (r *MemRegistry) GetLatestResourcePolicy(id string) (ResourcePolicy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, exists := r.resourcePolicies[id]
	if !exists || len(versions) == 0 {
		return ResourcePolicy{}, fmt.Errorf("resource policy not found: %s", id)
	}

	var latest ResourcePolicy
	first := true
	for _, p := range versions {
		if first || p.Version > latest.Version {
			latest = p
			first = false
		}
	}
	return latest, nil
}

func (r *MemRegistry) ListResourcePolicies() []ResourcePolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []ResourcePolicy
	for _, versions := range r.resourcePolicies {
		for _, p := range versions {
			list = append(list, p)
		}
	}
	return list
}
