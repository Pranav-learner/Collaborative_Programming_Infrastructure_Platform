package policies

import (
	"cpip/internal/sandbox/security/profiles"
)

// SecurityPolicy combines a security profile with any custom runtime adjustments.
type SecurityPolicy struct {
	ID          string                   `json:"id"`
	Version     int                      `json:"version"`
	Profile     profiles.SecurityProfile `json:"profile"`
	CustomRules map[string]any           `json:"custom_rules,omitempty"`
}

// ResourcePolicy defines the resource configuration limits.
type ResourcePolicy struct {
	ID          string                   `json:"id"`
	Version     int                      `json:"version"`
	Profile     profiles.ResourceProfile `json:"profile"`
	CustomRules map[string]any           `json:"custom_rules,omitempty"`
}

// Registry defines interface for managing security and resource policies.
type Registry interface {
	RegisterSecurityPolicy(policy SecurityPolicy) error
	GetSecurityPolicy(id string, version int) (SecurityPolicy, error)
	GetLatestSecurityPolicy(id string) (SecurityPolicy, error)
	ListSecurityPolicies() []SecurityPolicy

	RegisterResourcePolicy(policy ResourcePolicy) error
	GetResourcePolicy(id string, version int) (ResourcePolicy, error)
	GetLatestResourcePolicy(id string) (ResourcePolicy, error)
	ListResourcePolicies() []ResourcePolicy
}

// Validator validates policies against platform bounds.
type Validator interface {
	ValidateSecurityPolicy(policy SecurityPolicy) error
	ValidateResourcePolicy(policy ResourcePolicy) error
}
