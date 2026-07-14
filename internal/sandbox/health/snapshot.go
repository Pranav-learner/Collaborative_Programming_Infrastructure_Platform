package health

import "time"

// SandboxHealthSnapshot captures the structured health and metrics state of a sandbox.
type SandboxHealthSnapshot struct {
	SandboxID       string    `json:"sandbox_id"`
	ContainerHealth string    `json:"container_health"` // "healthy", "unhealthy", "unknown"
	RuntimeHealth   string    `json:"runtime_health"`   // "healthy", "unhealthy"
	CPU             float64   `json:"cpu"`
	Memory          int64     `json:"memory"`
	Disk            int64     `json:"disk"`
	OutputSize      int64     `json:"output_size"`
	ProcessCount    int64     `json:"process_count"`
	Filesystem      string    `json:"filesystem"` // "ok", "corrupted", "readonly"
	Heartbeat       time.Time `json:"heartbeat"`
	Status          string    `json:"status"` // "running", "exited", etc.
	LastUpdated     time.Time `json:"last_updated"`
	HealthScore     int       `json:"health_score"` // 0-100 score
}
