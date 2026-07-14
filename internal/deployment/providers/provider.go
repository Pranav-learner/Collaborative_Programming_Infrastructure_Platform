package providers

import (
	"context"
	"time"

	"cpip/internal/deployment/services"
	"cpip/internal/deployment/validation"
)

// Result captures execution details of a deployment operation.
type Result struct {
	Success   bool      `json:"success"`
	Provider  string    `json:"provider"`
	Profile   string    `json:"profile"`
	Timestamp time.Time `json:"timestamp"`
	Detail    string    `json:"detail,omitempty"`
}

// StatusResult exposes service health and status metrics.
type StatusResult struct {
	Profile       string            `json:"profile"`
	Provider      string            `json:"provider"`
	IsHealthy     bool              `json:"is_healthy"`
	ActiveVersion int               `json:"active_version"`
	Services      map[string]string `json:"services,omitempty"` // serviceName -> status (Running, Failed, Pending)
}

// GeneratedArtifacts wraps output configurations.
type GeneratedArtifacts struct {
	Provider string            `json:"provider"`
	Files    map[string]string `json:"files"` // fileName -> fileContent
}

// Provider defines the standard interface for all cloud-native deployment engines.
type Provider interface {
	Name() string
	Deploy(ctx context.Context, profile string, services []services.Service) (Result, error)
	Update(ctx context.Context, profile string, services []services.Service) (Result, error)
	Rollback(ctx context.Context, profile string, targetVersion int) (Result, error)
	Scale(ctx context.Context, serviceName string, replicas int) (Result, error)
	Validate(ctx context.Context, profile string, services []services.Service) (validation.ValidationResult, error)
	Status(ctx context.Context, profile string) (StatusResult, error)
	Destroy(ctx context.Context, profile string) (Result, error)
	Generate(ctx context.Context, profile string, services []services.Service) (GeneratedArtifacts, error)
}
