// Package config defines the configuration properties for the Presence & Awareness System.
package config

import "time"

// Config holds defaults for the presence subsystem.
type Config struct {
	// HeartbeatInterval is the frequency of expected heartbeat pings.
	HeartbeatInterval time.Duration
	// IdleTimeout is the threshold of inactivity before a user transitions to Idle.
	IdleTimeout       time.Duration
	// AwayTimeout is the threshold of inactivity before a user transitions to Away.
	AwayTimeout       time.Duration
	// TypingTimeout is how long typing status remains active without updates.
	TypingTimeout     time.Duration
	// BroadcastInterval is the frequency at which throttled user presence updates are flushed.
	BroadcastInterval time.Duration
	// CursorThrottle is the minimum duration between cursor updates.
	CursorThrottle    time.Duration
	// SelectionThrottle is the minimum duration between text selection updates.
	SelectionThrottle time.Duration
	// RecoveryTimeout is the grace period a disconnected participant has to reconnect.
	RecoveryTimeout   time.Duration
	// MaxMetadataSize limits the size of metadata payload in bytes.
	MaxMetadataSize   int
}

// Default returns a Config populated with production-sane defaults.
func Default() Config {
	return Config{
		HeartbeatInterval: 10 * time.Second,
		IdleTimeout:       5 * time.Minute,
		AwayTimeout:       15 * time.Minute,
		TypingTimeout:     5 * time.Second,
		BroadcastInterval: 100 * time.Millisecond,
		CursorThrottle:    50 * time.Millisecond,
		SelectionThrottle: 50 * time.Millisecond,
		RecoveryTimeout:   1 * time.Minute,
		MaxMetadataSize:   1024,
	}
}
