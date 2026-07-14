// Package config provides self-configuration for the configuration platform itself.
package config

import "time"

// Profile represents an environment deployment profile.
type Profile string

const (
	ProfileDevelopment Profile = "development"
	ProfileTesting     Profile = "testing"
	ProfileStaging     Profile = "staging"
	ProfileProduction  Profile = "production"
	ProfileLocal       Profile = "local"
)

// PlatformConfig configures the configuration platform's own behaviour.
type PlatformConfig struct {
	ActiveProfile    Profile       `json:"active_profile" yaml:"active_profile"`
	ProviderOrder    []string      `json:"provider_order" yaml:"provider_order"`
	ReloadInterval   time.Duration `json:"reload_interval" yaml:"reload_interval"`
	WatchInterval    time.Duration `json:"watch_interval" yaml:"watch_interval"`
	EnableValidation bool          `json:"enable_validation" yaml:"enable_validation"`
	EnableAudit      bool          `json:"enable_audit" yaml:"enable_audit"`
	EnableMetrics    bool          `json:"enable_metrics" yaml:"enable_metrics"`
	SecretMaskChar   string        `json:"secret_mask_char" yaml:"secret_mask_char"`
	MaxVersions      int           `json:"max_versions" yaml:"max_versions"`
}

// DefaultPlatformConfig returns production-safe defaults.
func DefaultPlatformConfig() PlatformConfig {
	return PlatformConfig{
		ActiveProfile:    ProfileDevelopment,
		ProviderOrder:    []string{"env", "yaml", "json"},
		ReloadInterval:   30 * time.Second,
		WatchInterval:    5 * time.Second,
		EnableValidation: true,
		EnableAudit:      true,
		EnableMetrics:    true,
		SecretMaskChar:   "•",
		MaxVersions:      50,
	}
}
