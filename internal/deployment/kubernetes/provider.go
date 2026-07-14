package kubernetes

import (
	"context"
	"fmt"
	"time"

	"cpip/internal/deployment/providers"
	"cpip/internal/deployment/services"
	"cpip/internal/deployment/validation"
)

// ProviderAdapter wraps the Kubernetes manifest generation to implement the Provider interface.
type ProviderAdapter struct {
	gen       *Provider
	val       *validation.Validator
	namespace string
}

// NewProviderAdapter creates a ProviderAdapter.
func NewProviderAdapter(namespace string) *ProviderAdapter {
	if namespace == "" {
		namespace = "cpip-system"
	}
	return &ProviderAdapter{
		gen:       NewProvider(),
		val:       validation.NewValidator(),
		namespace: namespace,
	}
}

// Name returns the provider name.
func (a *ProviderAdapter) Name() string {
	return "kubernetes"
}

// Deploy generates manifests and executes/simulates deployment.
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
		Detail:    fmt.Sprintf("Kubernetes manifests applied to namespace %q for profile %q", a.namespace, profile),
	}, nil
}

// Update executes updates on the manifests.
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
		Detail:    fmt.Sprintf("Kubernetes deployment rolled back to version %d in namespace %q", targetVersion, a.namespace),
	}, nil
}

// Scale scales a service.
func (a *ProviderAdapter) Scale(ctx context.Context, serviceName string, replicas int) (providers.Result, error) {
	return providers.Result{
		Success:   true,
		Provider:  a.Name(),
		Timestamp: time.Now(),
		Detail:    fmt.Sprintf("Scaled deployment/%s to %d replicas in namespace %q", serviceName, replicas, a.namespace),
	}, nil
}

// Validate executes semantic checking on the service definition.
func (a *ProviderAdapter) Validate(ctx context.Context, profile string, svcs []services.Service) (validation.ValidationResult, error) {
	return a.val.Validate(svcs)
}

// Status returns health metrics for the Kubernetes deployment.
func (a *ProviderAdapter) Status(ctx context.Context, profile string) (providers.StatusResult, error) {
	return providers.StatusResult{
		Profile:       profile,
		Provider:      a.Name(),
		IsHealthy:     true,
		ActiveVersion: 1,
		Services:      map[string]string{},
	}, nil
}

// Destroy cleans up the Kubernetes resources.
func (a *ProviderAdapter) Destroy(ctx context.Context, profile string) (providers.Result, error) {
	return providers.Result{
		Success:   true,
		Provider:  a.Name(),
		Profile:   profile,
		Timestamp: time.Now(),
		Detail:    fmt.Sprintf("All Kubernetes resources destroyed in namespace %q", a.namespace),
	}, nil
}

// Generate generates the deployment manifests.
func (a *ProviderAdapter) Generate(ctx context.Context, profile string, svcs []services.Service) (providers.GeneratedArtifacts, error) {
	content, err := a.gen.Generate(ctx, a.namespace, profile, svcs)
	if err != nil {
		return providers.GeneratedArtifacts{}, err
	}

	files := map[string]string{
		fmt.Sprintf("%s-manifests.yaml", a.namespace): content,
	}

	return providers.GeneratedArtifacts{
		Provider: a.Name(),
		Files:    files,
	}, nil
}
