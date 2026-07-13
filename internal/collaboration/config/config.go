// Package config defines the configuration surface for the collaboration
// engine. Configuration is injected at construction time (dependency injection);
// there is no global state and no package-level mutable configuration.
package config

import (
	"errors"
	"time"
)

// Config defines the tunable parameters for the Collaboration Engine.
type Config struct {
	// --- Snapshotting ---

	// SnapshotInterval is the maximum wall-clock age of unsaved edits before a
	// dirty document is snapshotted by the background saver.
	SnapshotInterval time.Duration `json:"snapshot_interval"`
	// SnapshotEditsThreshold is the number of edits that forces a snapshot
	// irrespective of SnapshotInterval.
	SnapshotEditsThreshold int `json:"snapshot_edits_threshold"`
	// IncrementalSnapshotThreshold is the number of incremental snapshots taken
	// between full snapshots. A full snapshot is taken every Nth snapshot to
	// bound recovery-replay length. Zero disables incremental snapshots.
	IncrementalSnapshotThreshold int `json:"incremental_snapshot_threshold"`
	// RetentionCount is how many snapshots to retain per document.
	RetentionCount int `json:"retention_count"`

	// --- Sizing / limits ---

	// MaxDocumentSize is the maximum serialized size of a document, in bytes.
	MaxDocumentSize int64 `json:"max_document_size"`
	// MaxUpdateSize is the maximum size of a single incremental update, in bytes.
	MaxUpdateSize int64 `json:"max_update_size"`
	// MaxPendingUpdatesLimit is the maximum number of unpersisted updates retained
	// before a synchronous snapshot flush is forced.
	MaxPendingUpdatesLimit int `json:"max_pending_updates_limit"`
	// BatchSize is the maximum number of updates merged together in a batch
	// synchronization operation.
	BatchSize int `json:"batch_size"`

	// --- Timeouts ---

	// SyncTimeout bounds a single synchronization handshake operation.
	SyncTimeout time.Duration `json:"sync_timeout"`
	// RecoveryTimeout bounds a document recovery operation.
	RecoveryTimeout time.Duration `json:"recovery_timeout"`

	// --- Background loops ---

	// BackgroundSaveInterval is the tick interval of the background saver loop.
	BackgroundSaveInterval time.Duration `json:"background_save_interval"`
	// GCInterval is the tick interval of the janitor (idle archival + GC) loop.
	GCInterval time.Duration `json:"gc_interval"`
	// IdleTimeout is the inactivity duration after which a document is archived.
	IdleTimeout time.Duration `json:"idle_timeout"`

	// --- CRDT / compression ---

	// EnableGC enables Yjs garbage collection of deleted content in live docs.
	EnableGC bool `json:"enable_gc"`
	// EnableCompression enables gzip compression of snapshot/update payloads at rest.
	EnableCompression bool `json:"enable_compression"`
	// CompressionThreshold is the minimum payload size (bytes) eligible for
	// compression; smaller payloads are stored verbatim to avoid overhead.
	CompressionThreshold int `json:"compression_threshold"`
}

// Default returns a production-sensible default configuration.
func Default() Config {
	return Config{
		SnapshotInterval:             5 * time.Second,
		SnapshotEditsThreshold:       100,
		IncrementalSnapshotThreshold: 10,
		RetentionCount:               5,

		MaxDocumentSize:        16 * 1024 * 1024, // 16 MiB
		MaxUpdateSize:          1 * 1024 * 1024,  // 1 MiB
		MaxPendingUpdatesLimit: 1000,
		BatchSize:              256,

		SyncTimeout:     10 * time.Second,
		RecoveryTimeout: 30 * time.Second,

		BackgroundSaveInterval: 1 * time.Second,
		GCInterval:             30 * time.Second,
		IdleTimeout:            15 * time.Minute,

		EnableGC:             true,
		EnableCompression:    true,
		CompressionThreshold: 4 * 1024, // 4 KiB
	}
}

// ErrInvalidConfig indicates a configuration value is out of range.
var ErrInvalidConfig = errors.New("collaboration/config: invalid configuration")

// Validate normalizes zero-valued fields to their defaults and rejects
// nonsensical values. It returns a normalized copy so callers may pass a
// partially-populated Config and rely on sane defaults.
func (c Config) Validate() (Config, error) {
	d := Default()

	if c.SnapshotInterval <= 0 {
		c.SnapshotInterval = d.SnapshotInterval
	}
	if c.SnapshotEditsThreshold <= 0 {
		c.SnapshotEditsThreshold = d.SnapshotEditsThreshold
	}
	if c.IncrementalSnapshotThreshold < 0 {
		return Config{}, errors.Join(ErrInvalidConfig, errors.New("incremental_snapshot_threshold must be >= 0"))
	}
	if c.RetentionCount <= 0 {
		c.RetentionCount = d.RetentionCount
	}
	if c.MaxDocumentSize <= 0 {
		c.MaxDocumentSize = d.MaxDocumentSize
	}
	if c.MaxUpdateSize <= 0 {
		c.MaxUpdateSize = d.MaxUpdateSize
	}
	if c.MaxUpdateSize > c.MaxDocumentSize {
		return Config{}, errors.Join(ErrInvalidConfig, errors.New("max_update_size must be <= max_document_size"))
	}
	if c.MaxPendingUpdatesLimit <= 0 {
		c.MaxPendingUpdatesLimit = d.MaxPendingUpdatesLimit
	}
	if c.BatchSize <= 0 {
		c.BatchSize = d.BatchSize
	}
	if c.SyncTimeout <= 0 {
		c.SyncTimeout = d.SyncTimeout
	}
	if c.RecoveryTimeout <= 0 {
		c.RecoveryTimeout = d.RecoveryTimeout
	}
	if c.BackgroundSaveInterval <= 0 {
		c.BackgroundSaveInterval = d.BackgroundSaveInterval
	}
	if c.GCInterval <= 0 {
		c.GCInterval = d.GCInterval
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = d.IdleTimeout
	}
	if c.CompressionThreshold <= 0 {
		c.CompressionThreshold = d.CompressionThreshold
	}
	return c, nil
}
