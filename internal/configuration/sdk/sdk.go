package sdk

import (
	"context"
	"strconv"
	"time"

	"cpip/internal/configuration/featureflags"
	"cpip/internal/configuration/manager"
	"cpip/internal/configuration/versioning"
)

// SDK provides the single public interface for all configurations, secrets, and features flags.
type SDK struct {
	mgr *manager.Manager
}

// NewSDK wraps a configuration Manager into a clean SDK.
func NewSDK(mgr *manager.Manager) *SDK {
	return &SDK{mgr: mgr}
}

// Manager returns the underlying Manager orchestrator.
func (s *SDK) Manager() *manager.Manager {
	return s.mgr
}

// Get retrieves a string configuration value by key.
func (s *SDK) Get(key string) (string, bool) {
	current := s.mgr.VersionManager().Current()
	if current == nil {
		return "", false
	}
	val, ok := current.Data[key]
	return val, ok
}

// GetDefault retrieves a string configuration value, falling back to def if missing.
func (s *SDK) GetDefault(key, def string) string {
	if val, ok := s.Get(key); ok {
		return val
	}
	return def
}

// GetInt retrieves an integer configuration value by key.
func (s *SDK) GetInt(key string) (int, bool) {
	val, ok := s.Get(key)
	if !ok {
		return 0, false
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, false
	}
	return i, true
}

// GetIntDefault retrieves an integer configuration value, falling back to def if missing or malformed.
func (s *SDK) GetIntDefault(key string, def int) int {
	if val, ok := s.GetInt(key); ok {
		return val
	}
	return def
}

// GetBool retrieves a boolean configuration value by key.
func (s *SDK) GetBool(key string) (bool, bool) {
	val, ok := s.Get(key)
	if !ok {
		return false, false
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return false, false
	}
	return b, true
}

// GetBoolDefault retrieves a boolean configuration value, falling back to def if missing or malformed.
func (s *SDK) GetBoolDefault(key string, def bool) bool {
	if val, ok := s.GetBool(key); ok {
		return val
	}
	return def
}

// GetDuration retrieves a duration configuration value by key.
func (s *SDK) GetDuration(key string) (time.Duration, bool) {
	val, ok := s.Get(key)
	if !ok {
		return 0, false
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, false
	}
	return d, true
}

// GetDurationDefault retrieves a duration configuration value, falling back to def if missing or malformed.
func (s *SDK) GetDurationDefault(key string, def time.Duration) time.Duration {
	if val, ok := s.GetDuration(key); ok {
		return val
	}
	return def
}

// GetSecret retrieves a secret value from the secret manager.
func (s *SDK) GetSecret(ctx context.Context, key string) (string, error) {
	return s.mgr.GetSecretManager().Get(ctx, key)
}

// EvaluateFlag evaluates a feature flag against the target context.
func (s *SDK) EvaluateFlag(key string, ctx featureflags.TargetContext) bool {
	return s.mgr.GetFeatureFlagPlatform().Evaluate(key, ctx)
}

// SetOverride sets a dynamic runtime configuration override.
func (s *SDK) SetOverride(ctx context.Context, key, value string) {
	s.mgr.GetRuntimeEngine().SetOverride(ctx, key, value)
	// Force a load to propagate overrides and update the current version snapshot
	_, _ = s.mgr.Load(ctx)
}

// RemoveOverride removes a dynamic runtime configuration override.
func (s *SDK) RemoveOverride(ctx context.Context, key string) {
	s.mgr.GetRuntimeEngine().RemoveOverride(ctx, key)
	// Force a load to propagate overrides and update the current version snapshot
	_, _ = s.mgr.Load(ctx)
}

// Reload reloads configuration from all providers.
func (s *SDK) Reload(ctx context.Context) (*versioning.Snapshot, error) {
	return s.mgr.Reload(ctx)
}

// Snapshot returns the current active configuration snapshot.
func (s *SDK) Snapshot() *versioning.Snapshot {
	return s.mgr.VersionManager().Current()
}
