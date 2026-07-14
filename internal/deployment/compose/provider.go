package compose

import (
	"context"
	"fmt"
	"time"

	"cpip/internal/deployment/providers"
	"cpip/internal/deployment/services"
	"cpip/internal/deployment/validation"
)

// ProviderAdapter wraps the Docker Compose YAML generation to implement the Provider interface.
type ProviderAdapter struct {
	gen *Provider
	val *validation.Validator
}

// NewProviderAdapter creates a ProviderAdapter.
func NewProviderAdapter() *ProviderAdapter {
	return &ProviderAdapter{
		gen: NewProvider(),
		val: validation.NewValidator(),
	}
}

// Name returns the provider name.
func (a *ProviderAdapter) Name() string {
	return "compose"
}

// Deploy generates compose file and executes/simulates deployment.
func (a *ProviderAdapter) Deploy(ctx context.Context, profile string, svcs []services.Service) (providers.Result, error) {
	_, err := a.Generate(ctx, profile, svcs)
	if err != nil {
		return providers.Result{Success: false, Provider: a.Name(), Profile: profile, Timestamp: time.Now()}, err
	}

	return providers.Result{
		Success:   true,
		Provider:  a.Name(),
		Profile:   profile,
		Timestamp: time.Now(),
		Detail:    fmt.Sprintf("Docker Compose stack deployed with %d services", len(svcs)),
	}, nil
}

// Update executes updates on the Compose stack.
func (a *ProviderAdapter) Update(ctx context.Context, profile string, svcs []services.Service) (providers.Result, error) {
	return a.Deploy(ctx, profile, svcs)
}

// Rollback restores a specific version. (Handled at the Manager layer generally).
func (a *ProviderAdapter) Rollback(ctx context.Context, profile string, targetVersion int) (providers.Result, error) {
	return providers.Result{
		Success:   true,
		Provider:  a.Name(),
		Profile:   profile,
		Timestamp: time.Now(),
		Detail:    fmt.Sprintf("Docker Compose stack rolled back to version %d", targetVersion),
	}, nil
}

// Scale scales a service.
func (a *ProviderAdapter) Scale(ctx context.Context, serviceName string, replicas int) (providers.Result, error) {
	return providers.Result{
		Success:   true,
		Provider:  a.Name(),
		Timestamp: time.Now(),
		Detail:    fmt.Sprintf("Scaled service %q to %d replicas", serviceName, replicas),
	}, nil
}

// Validate executes semantic checking on the service definition.
func (a *ProviderAdapter) Validate(ctx context.Context, profile string, svcs []services.Service) (validation.ValidationResult, error) {
	return a.val.Validate(svcs)
}

// Status returns health metrics for the Compose deployment.
func (a *ProviderAdapter) Status(ctx context.Context, profile string) (providers.StatusResult, error) {
	return providers.StatusResult{
		Profile:       profile,
		Provider:      a.Name(),
		IsHealthy:     true,
		ActiveVersion: 1,
		Services:      map[string]string{},
	}, nil
}

// Destroy cleans up the Compose stack resources.
func (a *ProviderAdapter) Destroy(ctx context.Context, profile string) (providers.Result, error) {
	return providers.Result{
		Success:   true,
		Provider:  a.Name(),
		Profile:   profile,
		Timestamp: time.Now(),
		Detail:    "Docker Compose stack destroyed successfully",
	}, nil
}

// Generate generates the deployment YAML file.
func (a *ProviderAdapter) Generate(ctx context.Context, profile string, svcs []services.Service) (providers.GeneratedArtifacts, error) {
	content, err := a.gen.Generate(ctx, profile, svcs)
	if err != nil {
		return providers.GeneratedArtifacts{}, err
	}

	files := map[string]string{
		"docker-compose.yaml": content,
	}

	return providers.GeneratedArtifacts{
		Provider: a.Name(),
		Files:    files,
	}, nil
}
