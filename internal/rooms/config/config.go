// Package config defines the configuration defaults for the room subsystem.
package config

import (
	"time"
)

// Config holds default settings for rooms and the manager's background cleanup.
type Config struct {
	// DefaultMaxParticipants caps concurrent membership per room by default.
	DefaultMaxParticipants int
	// DefaultIdleTimeout is how long without activity before a room becomes Idle.
	DefaultIdleTimeout     time.Duration
	// DefaultExpireTimeout is how long a room may remain Idle/Waiting before it begins Expiring.
	DefaultExpireTimeout   time.Duration
	// DefaultRecoveryTimeout is how long a disconnected participant remains a member.
	DefaultRecoveryTimeout time.Duration
	// CleanupInterval is the frequency of the background janitor cleanup loop.
	CleanupInterval        time.Duration
}

// Default returns a Config populated with production-sane defaults.
func Default() Config {
	return Config{
		DefaultMaxParticipants: 100,
		DefaultIdleTimeout:     5 * time.Minute,
		DefaultExpireTimeout:   10 * time.Minute,
		DefaultRecoveryTimeout: 1 * time.Minute,
		CleanupInterval:        30 * time.Second,
	}
}
