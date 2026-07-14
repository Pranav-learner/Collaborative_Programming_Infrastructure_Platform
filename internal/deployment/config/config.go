package config

import "time"

// Profile represents a target deployment environment profile.
type Profile string

const (
	ProfileLocal       Profile = "local"
	ProfileDevelopment Profile = "development"
	ProfileTesting     Profile = "testing"
	ProfileStaging     Profile = "staging"
	ProfileProduction  Profile = "production"
)

// PlatformConfig configures the deployment platform manager.
type PlatformConfig struct {
	DefaultProvider   string        `json:"default_provider"` // "kubernetes" or "compose"
	DefaultNamespace  string        `json:"default_namespace"`
	ActiveProfile     Profile       `json:"active_profile"`
	MaxHistoryLimit   int           `json:"max_history_limit"`
	DeploymentTimeout time.Duration `json:"deployment_timeout"`
	ValidationEnabled bool          `json:"validation_enabled"`
}

// DefaultPlatformConfig returns a production-grade default configuration.
func DefaultPlatformConfig() PlatformConfig {
	return PlatformConfig{
		DefaultProvider:   "kubernetes",
		DefaultNamespace:  "cpip-system",
		ActiveProfile:     ProfileDevelopment,
		MaxHistoryLimit:   10,
		DeploymentTimeout: 10 * time.Minute,
		ValidationEnabled: true,
	}
}
