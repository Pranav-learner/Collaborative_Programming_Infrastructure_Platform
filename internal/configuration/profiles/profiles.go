package profiles

import (
	"strings"

	"cpip/internal/configuration/config"
)

// ProfileManager manages active environment profiles and inheritance resolution.
type ProfileManager struct {
	activeProfile config.Profile
}

// NewProfileManager constructs a ProfileManager for the given active profile.
func NewProfileManager(active config.Profile) *ProfileManager {
	if active == "" {
		active = config.ProfileDevelopment
	}
	return &ProfileManager{activeProfile: active}
}

// Active returns the currently active profile.
func (pm *ProfileManager) Active() config.Profile {
	return pm.activeProfile
}

// GetInheritanceChain returns the list of profiles to check in order of priority (most specific first).
// For instance: local -> development -> common.
func (pm *ProfileManager) GetInheritanceChain() []string {
	chain := []string{string(pm.activeProfile)}

	switch pm.activeProfile {
	case config.ProfileLocal:
		chain = append(chain, string(config.ProfileDevelopment), "common")
	case config.ProfileTesting:
		chain = append(chain, string(config.ProfileDevelopment), "common")
	case config.ProfileDevelopment, config.ProfileStaging, config.ProfileProduction:
		chain = append(chain, "common")
	default:
		chain = append(chain, "common")
	}

	return chain
}

// ResolveKey returns the profile-scoped version of a key.
// E.g., if active profile is "production", and the key is "database.url",
// it searches in order:
// 1. "production.database.url"
// 2. "common.database.url"
// 3. "database.url"
func (pm *ProfileManager) ResolveKey(key string, data map[string]string) (string, bool) {
	chain := pm.GetInheritanceChain()

	// Try with profile prefix first
	for _, prof := range chain {
		profKey := prof + "." + key
		if val, exists := data[profKey]; exists {
			return val, true
		}
	}

	// Fallback to flat/unscoped key
	if val, exists := data[key]; exists {
		return val, true
	}

	return "", false
}

// ResolveConfig merges and flattens a raw map by applying profile inheritance overrides.
// It maps scoped keys (e.g. "development.database.host") to flat keys (e.g. "database.host")
// respecting precedence order.
func (pm *ProfileManager) ResolveConfig(raw map[string]string) map[string]string {
	resolved := make(map[string]string)
	chain := pm.GetInheritanceChain()

	// First, load all unscoped keys
	for k, v := range raw {
		if !isScoped(k, chain) {
			resolved[k] = v
		}
	}

	// Apply profiles in reverse order of the chain (least specific first, so most specific overrides)
	for i := len(chain) - 1; i >= 0; i-- {
		prof := chain[i]
		prefix := prof + "."
		for k, v := range raw {
			if strings.HasPrefix(k, prefix) {
				cleanKey := strings.TrimPrefix(k, prefix)
				resolved[cleanKey] = v
			}
		}
	}

	return resolved
}

func isScoped(key string, chain []string) bool {
	for _, prof := range chain {
		if strings.HasPrefix(key, prof+".") {
			return true
		}
	}
	return false
}
