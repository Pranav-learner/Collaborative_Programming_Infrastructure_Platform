package cleanup

import "time"

// PolicyType defines the cleanup mode.
type PolicyType string

const (
	PolicyImmediate   PolicyType = "Immediate"
	PolicyGracePeriod PolicyType = "GracePeriod"
	PolicyRetention   PolicyType = "Retention"
	PolicyCustom      PolicyType = "Custom"
)

// CleanupPolicy governs the behavior of the CleanupManager.
type CleanupPolicy struct {
	Type          PolicyType    `json:"type"`
	GracePeriod   time.Duration `json:"grace_period"`
	Retention     time.Duration `json:"retention"`
	KeepArtifacts bool          `json:"keep_artifacts"`
	ArchiveLogs   bool          `json:"archive_logs"`
	DebugMode     bool          `json:"debug_mode"`
}

// DefaultImmediatePolicy cleans up everything immediately.
var DefaultImmediatePolicy = CleanupPolicy{
	Type:          PolicyImmediate,
	GracePeriod:   0,
	Retention:     0,
	KeepArtifacts: false,
	ArchiveLogs:   false,
	DebugMode:     false,
}

// DefaultKeepArtifactsPolicy retains artifacts for debugging.
var DefaultKeepArtifactsPolicy = CleanupPolicy{
	Type:          PolicyGracePeriod,
	GracePeriod:   5 * time.Minute,
	Retention:     24 * time.Hour,
	KeepArtifacts: true,
	ArchiveLogs:   true,
	DebugMode:     true,
}
