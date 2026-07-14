package featureflags

import (
	"hash/fnv"
	"fmt"
	"sync"
	"time"

	"cpip/internal/configuration/events"
	"cpip/internal/configuration/metrics"
)

// TargetContext provides properties used to evaluate target-based feature flags.
type TargetContext struct {
	UserID    string            `json:"user_id"`
	Role      string            `json:"role"`
	Profile   string            `json:"profile"`
	SessionID string            `json:"session_id"`
	Custom    map[string]string `json:"custom"`
}

// FeatureFlag defines rollout constraints, overrides, and targeting rules for a flag.
type FeatureFlag struct {
	Key            string   `json:"key"`
	Enabled        bool     `json:"enabled"`
	RolloutPercent int      `json:"rollout_percent"` // 0 to 100
	AllowedUsers   []string `json:"allowed_users"`
	AllowedRoles   []string `json:"allowed_roles"`
	TargetProfiles []string `json:"target_profiles"`
	IsKillSwitch   bool     `json:"is_kill_switch"`
}

// Platform evaluates feature flags.
type Platform struct {
	mu      sync.RWMutex
	flags   map[string]FeatureFlag
	metrics metrics.Recorder
	bus     *events.Bus
}

// NewPlatform creates a Feature Flag Platform.
func NewPlatform(rec metrics.Recorder, bus *events.Bus) *Platform {
	return &Platform{
		flags:   make(map[string]FeatureFlag),
		metrics: rec,
		bus:     bus,
	}
}

// RegisterFlag registers or updates a feature flag.
func (p *Platform) RegisterFlag(ff FeatureFlag) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flags[ff.Key] = ff

	if p.bus != nil {
		p.bus.Publish(events.Event{
			Type:      events.FeatureFlagChanged,
			Timestamp: time.Now(),
			Key:       ff.Key,
			Detail:    fmt.Sprintf("Registered flag. Enabled: %t", ff.Enabled),
		})
	}
}

// Evaluate determines whether a feature flag is enabled for the given context.
func (p *Platform) Evaluate(key string, ctx TargetContext) bool {
	p.mu.RLock()
	ff, exists := p.flags[key]
	p.mu.RUnlock()

	p.metrics.Inc(metrics.MetricFeatureFlagEvals)

	if !exists {
		return false
	}

	// 1. Kill Switch evaluation (immediate bypass)
	if ff.IsKillSwitch {
		return false
	}

	// 2. Global switch
	if !ff.Enabled {
		return false
	}

	// 3. User targeting override (whitelist)
	if len(ff.AllowedUsers) > 0 {
		userMatched := false
		for _, u := range ff.AllowedUsers {
			if u == ctx.UserID {
				userMatched = true
				break
			}
		}
		if !userMatched {
			return false
		}
	}

	// 4. Role targeting override (whitelist)
	if len(ff.AllowedRoles) > 0 {
		roleMatched := false
		for _, r := range ff.AllowedRoles {
			if r == ctx.Role {
				roleMatched = true
				break
			}
		}
		if !roleMatched {
			return false
		}
	}

	// 5. Environment Profile gating
	if len(ff.TargetProfiles) > 0 && ctx.Profile != "" {
		profileMatched := false
		for _, prof := range ff.TargetProfiles {
			if prof == ctx.Profile {
				profileMatched = true
				break
			}
		}
		if !profileMatched {
			return false
		}
	}

	// 6. Percentage Rollout evaluation (via consistent hashing)
	if ff.RolloutPercent > 0 {
		if ff.RolloutPercent >= 100 {
			return true
		}

		// Use UserID or SessionID for stable rollout bucket hashing
		hashKey := ctx.UserID
		if hashKey == "" {
			hashKey = ctx.SessionID
		}
		if hashKey == "" {
			hashKey = key // fallback to flag name to avoid bias but will be static per flag
		}

		bucket := hashString(hashKey) % 100
		return int(bucket) < ff.RolloutPercent
	}

	return true
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
