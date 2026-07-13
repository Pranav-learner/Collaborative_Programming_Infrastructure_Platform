package config

import "time"

// Config defines the configuration parameters for the Collaboration Engine.
type Config struct {
	// SnapshotInterval specifies how often snapshots are taken.
	SnapshotInterval time.Duration `json:"snapshot_interval"`
	// SnapshotEditsThreshold specifies how many edits trigger a snapshot.
	SnapshotEditsThreshold int `json:"snapshot_edits_threshold"`
	// MaxDocumentSize specifies the maximum size of a document.
	MaxDocumentSize int64 `json:"max_document_size"`
	// MaxPendingUpdatesLimit specifies the maximum number of pending updates.
	MaxPendingUpdatesLimit int `json:"max_pending_updates_limit"`
	// RetentionCount specifies how many snapshots to retain.
	RetentionCount int `json:"retention_count"`
	// IdleTimeout specifies the duration of inactivity before archiving a document.
	IdleTimeout time.Duration `json:"idle_timeout"`
	// BackgroundSaveInterval specifies how often the background save process runs.
	BackgroundSaveInterval time.Duration `json:"background_save_interval"`
}

// Default returns the default configuration for the Collaboration Engine.
func Default() Config {
	return Config{
		SnapshotInterval:       5 * time.Second,
		SnapshotEditsThreshold: 100,
		MaxDocumentSize:        10 * 1024 * 1024, // 10MB
		MaxPendingUpdatesLimit: 1000,
		RetentionCount:         5,
		IdleTimeout:            15 * time.Minute,
		BackgroundSaveInterval: 1 * time.Second,
	}
}
