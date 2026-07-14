package config

import "time"

// Config is the top-level configuration for the Plugin Framework.
type Config struct {
	PluginDirs        []string                 `json:"plugin_dirs"`
	ValidationEnabled bool                     `json:"validation_enabled"`
	VersionPolicy     string                   `json:"version_policy"` // "strict", "loose"
	ProfileDefaults   ProfileDefaultConfig    `json:"profile_defaults"`
	ResourceDefaults  ResourceDefaultConfig   `json:"resource_defaults"`
}

// ProfileDefaultConfig holds default limits for execution profiles.
type ProfileDefaultConfig struct {
	Timeout     time.Duration `json:"timeout"`
	MemoryLimit int64         `json:"memory_limit"`
	CPULimit    int           `json:"cpu_limit"`
	FileLimit   int64         `json:"file_limit"`
	OutputLimit int64         `json:"output_limit"`
}

// ResourceDefaultConfig holds defaults for reusable resource policies.
type ResourceDefaultConfig struct {
	Small  ResourceLimits `json:"small"`
	Medium ResourceLimits `json:"medium"`
	Large  ResourceLimits `json:"large"`
}

// ResourceLimits defines the exact resource limits.
type ResourceLimits struct {
	CPUMillicores int           `json:"cpu_millicores"`
	MemoryBytes   int64         `json:"memory_bytes"`
	PidsLimit     int           `json:"pids_limit"`
	TmpfsBytes    int64         `json:"tmpfs_bytes"`
	WallTimeout   time.Duration `json:"wall_timeout"`
}

// PluginConfig is the config passed to an individual plugin during Initialize.
type PluginConfig struct {
	WorkspaceDir string            `json:"workspace_dir"`
	Env          map[string]string `json:"env"`
	Custom       map[string]string `json:"custom"`
}
