// Package config defines the configuration surface for the execution
// orchestrator. Configuration is injected at construction time (dependency
// injection); there is no global state and no package-level mutable state.
package config

import (
	"errors"
	"time"

	"cpip/internal/execution/job"
)

// Config defines the tunable parameters of the execution orchestrator.
type Config struct {
	// --- Payload limits ---

	// MaxCodeSize is the maximum source-code size, in bytes.
	MaxCodeSize int64 `json:"max_code_size"`
	// MaxStdinSize is the maximum standard-input size, in bytes.
	MaxStdinSize int64 `json:"max_stdin_size"`
	// MaxMetadataEntries caps the number of metadata key/value pairs.
	MaxMetadataEntries int `json:"max_metadata_entries"`
	// MaxMetadataKeyLen caps the length of a metadata key.
	MaxMetadataKeyLen int `json:"max_metadata_key_len"`
	// MaxMetadataValueLen caps the length of a metadata value.
	MaxMetadataValueLen int `json:"max_metadata_value_len"`

	// --- Execution controls ---

	// DefaultTimeout is applied when a request omits a timeout.
	DefaultTimeout time.Duration `json:"default_timeout"`
	// MaxTimeout is the ceiling on a requested timeout.
	MaxTimeout time.Duration `json:"max_timeout"`
	// MaxMemoryBytes is the ceiling on a requested memory profile.
	MaxMemoryBytes int64 `json:"max_memory_bytes"`
	// MaxRetries is the maximum number of retries permitted per job.
	MaxRetries int `json:"max_retries"`

	// --- Priority range ---

	// MinPriority and MaxPriority bound the accepted priority values (inclusive).
	MinPriority job.Priority `json:"min_priority"`
	MaxPriority job.Priority `json:"max_priority"`

	// --- Scheduling / retention ---

	// QueueTimeout bounds how long a job may wait in the queue before it is
	// considered timed out (enforced by future queue modules; carried here).
	QueueTimeout time.Duration `json:"queue_timeout"`
	// ArchiveRetention is how long a finished job is retained in the live registry
	// before the archival sweep moves it to the archive store.
	ArchiveRetention time.Duration `json:"archive_retention"`
	// ArchiveSweepInterval is the tick interval of the archival sweep loop.
	ArchiveSweepInterval time.Duration `json:"archive_sweep_interval"`

	// --- Validation toggles ---

	// RequireAuthentication rejects unauthenticated requests when true.
	RequireAuthentication bool `json:"require_authentication"`
	// EnableAuthorization runs the authorization validator when true.
	EnableAuthorization bool `json:"enable_authorization"`
}

// Default returns a production-sensible default configuration.
func Default() Config {
	return Config{
		MaxCodeSize:         256 * 1024,  // 256 KiB
		MaxStdinSize:        1024 * 1024, // 1 MiB
		MaxMetadataEntries:  32,
		MaxMetadataKeyLen:   128,
		MaxMetadataValueLen: 1024,

		DefaultTimeout: 10 * time.Second,
		MaxTimeout:     60 * time.Second,
		MaxMemoryBytes: 512 * 1024 * 1024, // 512 MiB
		MaxRetries:     3,

		MinPriority: job.PriorityLow,
		MaxPriority: job.PriorityCritical,

		QueueTimeout:         30 * time.Second,
		ArchiveRetention:     15 * time.Minute,
		ArchiveSweepInterval: 1 * time.Minute,

		RequireAuthentication: true,
		EnableAuthorization:   false,
	}
}

// ErrInvalidConfig indicates a configuration value is out of range.
var ErrInvalidConfig = errors.New("execution/config: invalid configuration")

// Validate normalizes zero-valued fields to their defaults and rejects
// nonsensical values, returning a normalized copy. Callers may pass a partially
// populated Config and rely on sane defaults for the rest.
func (c Config) Validate() (Config, error) {
	d := Default()

	if c.MaxCodeSize <= 0 {
		c.MaxCodeSize = d.MaxCodeSize
	}
	if c.MaxStdinSize <= 0 {
		c.MaxStdinSize = d.MaxStdinSize
	}
	if c.MaxMetadataEntries <= 0 {
		c.MaxMetadataEntries = d.MaxMetadataEntries
	}
	if c.MaxMetadataKeyLen <= 0 {
		c.MaxMetadataKeyLen = d.MaxMetadataKeyLen
	}
	if c.MaxMetadataValueLen <= 0 {
		c.MaxMetadataValueLen = d.MaxMetadataValueLen
	}
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = d.DefaultTimeout
	}
	if c.MaxTimeout <= 0 {
		c.MaxTimeout = d.MaxTimeout
	}
	if c.DefaultTimeout > c.MaxTimeout {
		return Config{}, errors.Join(ErrInvalidConfig, errors.New("default_timeout must be <= max_timeout"))
	}
	if c.MaxMemoryBytes <= 0 {
		c.MaxMemoryBytes = d.MaxMemoryBytes
	}
	if c.MaxRetries < 0 {
		return Config{}, errors.Join(ErrInvalidConfig, errors.New("max_retries must be >= 0"))
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = d.MaxRetries
	}
	if c.MinPriority > c.MaxPriority {
		return Config{}, errors.Join(ErrInvalidConfig, errors.New("min_priority must be <= max_priority"))
	}
	if c.QueueTimeout <= 0 {
		c.QueueTimeout = d.QueueTimeout
	}
	if c.ArchiveRetention <= 0 {
		c.ArchiveRetention = d.ArchiveRetention
	}
	if c.ArchiveSweepInterval <= 0 {
		c.ArchiveSweepInterval = d.ArchiveSweepInterval
	}
	return c, nil
}
