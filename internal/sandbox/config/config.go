package config

import "time"

// Config holds settings for sandbox container isolation.
type Config struct {
	WorkspaceRoot      string            `json:"workspace_root"`
	ImageRegistry      string            `json:"image_registry"`
	ContainerNamingPat string            `json:"container_naming_pattern"`
	CleanupInterval    time.Duration     `json:"cleanup_interval"`
	ImageCacheEnabled  bool              `json:"image_cache_enabled"`
	ContainerTimeout   time.Duration     `json:"container_timeout"`
	NetworkName        string            `json:"network_name"`
	LanguageImages     map[string]string `json:"language_images"`
}

// Default returns a Config seeded with CPIP standard options.
func Default() Config {
	return Config{
		WorkspaceRoot:      "sandbox_workspaces",
		ImageRegistry:      "", // Host local or DockerHub by default
		ContainerNamingPat: "cpip-sandbox-%s",
		CleanupInterval:    5 * time.Minute,
		ImageCacheEnabled:  true,
		ContainerTimeout:   10 * time.Second,
		NetworkName:        "cpip-sandbox-network",
		LanguageImages: map[string]string{
			"python3":    "python:3.12-alpine",
			"go":         "golang:1.26-alpine",
			"bash":       "alpine:latest",
			"c":          "gcc:14-alpine",
			"cpp":        "gcc:14-alpine",
			"java":       "openjdk:21-slim",
			"javascript": "node:20-alpine",
		},
	}
}
