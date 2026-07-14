package sdk

import (
	"context"

	"cpip/internal/deployment/manager"
	"cpip/internal/deployment/providers"
	"cpip/internal/deployment/services"
	"cpip/internal/deployment/validation"
)

// Client wraps the internal deployment manager to expose a high-level API.
type Client struct {
	mgr *manager.Manager
}

// NewClient initializes the SDK Client.
func NewClient(mgr *manager.Manager) *Client {
	return &Client{mgr: mgr}
}

// Deploy triggers a deployment.
func (c *Client) Deploy(ctx context.Context, svcs []services.Service) (providers.Result, error) {
	return c.mgr.Deploy(ctx, svcs)
}

// Update updates an existing deployment.
func (c *Client) Update(ctx context.Context, svcs []services.Service) (providers.Result, error) {
	return c.mgr.Update(ctx, svcs)
}

// Rollback triggers a rollback operation to a specific version.
func (c *Client) Rollback(ctx context.Context, version int) (providers.Result, error) {
	report, err := c.mgr.Rollback(ctx, version)
	if err != nil {
		return providers.Result{}, err
	}
	return providers.Result{
		Success:   report.Success,
		Timestamp: report.Timestamp,
		Detail:    report.Detail,
	}, nil
}

// Scale rescales a target service.
func (c *Client) Scale(ctx context.Context, serviceName string, replicas int) (providers.Result, error) {
	return c.mgr.Scale(ctx, serviceName, replicas)
}

// Validate executes the validation engine checks.
func (c *Client) Validate(ctx context.Context, svcs []services.Service) (validation.ValidationResult, error) {
	return c.mgr.Validate(ctx, svcs)
}

// Status checks the status of the current deployment.
func (c *Client) Status(ctx context.Context) (providers.StatusResult, error) {
	return c.mgr.Status(ctx)
}

// Destroy cleans up the active deployment.
func (c *Client) Destroy(ctx context.Context) (providers.Result, error) {
	return c.mgr.Destroy(ctx)
}

// Generate returns the generated YAML manifests.
func (c *Client) Generate(ctx context.Context, svcs []services.Service) (providers.GeneratedArtifacts, error) {
	return c.mgr.Generate(ctx, svcs)
}
