package middleware

import (
	"context"
	"time"

	"cpip/internal/deployment/logger"
	"cpip/internal/deployment/metrics"
	"cpip/internal/deployment/providers"
	"cpip/internal/deployment/services"
	"cpip/internal/deployment/validation"
)

// ObservabilityProvider wraps a Provider with telemetry and logs.
type ObservabilityProvider struct {
	next    providers.Provider
	metrics metrics.Recorder
	log     *logger.Logger
}

// NewObservabilityProvider creates an Instrumented Provider.
func NewObservabilityProvider(next providers.Provider, rec metrics.Recorder, log *logger.Logger) *ObservabilityProvider {
	return &ObservabilityProvider{
		next:    next,
		metrics: rec,
		log:     log,
	}
}

// Name delegates.
func (p *ObservabilityProvider) Name() string {
	return p.next.Name()
}

// Deploy instrumented execution.
func (p *ObservabilityProvider) Deploy(ctx context.Context, profile string, svcs []services.Service) (providers.Result, error) {
	p.metrics.Inc(metrics.MetricDeployAttempts)
	start := time.Now()
	res, err := p.next.Deploy(ctx, profile, svcs)
	duration := time.Since(start)

	if err != nil {
		p.metrics.Inc(metrics.MetricDeployFailures)
		if p.log != nil {
			p.log.Error("Deployment failed", "provider", p.Name(), "profile", profile, "duration", duration, "error", err)
		}
		return res, err
	}

	p.metrics.Inc(metrics.MetricDeploySuccesses)
	if p.log != nil {
		p.log.Info("Deployment succeeded", "provider", p.Name(), "profile", profile, "duration", duration)
	}
	return res, nil
}

// Update instrumented execution.
func (p *ObservabilityProvider) Update(ctx context.Context, profile string, svcs []services.Service) (providers.Result, error) {
	start := time.Now()
	res, err := p.next.Update(ctx, profile, svcs)
	duration := time.Since(start)

	if err != nil {
		if p.log != nil {
			p.log.Error("Update failed", "provider", p.Name(), "profile", profile, "duration", duration, "error", err)
		}
		return res, err
	}

	if p.log != nil {
		p.log.Info("Update succeeded", "provider", p.Name(), "profile", profile, "duration", duration)
	}
	return res, nil
}

// Rollback instrumented execution.
func (p *ObservabilityProvider) Rollback(ctx context.Context, profile string, targetVersion int) (providers.Result, error) {
	p.metrics.Inc(metrics.MetricRollbackRuns)
	start := time.Now()
	res, err := p.next.Rollback(ctx, profile, targetVersion)
	duration := time.Since(start)

	if err != nil {
		p.metrics.Inc(metrics.MetricRollbackFailures)
		if p.log != nil {
			p.log.Error("Rollback failed", "provider", p.Name(), "profile", profile, "version", targetVersion, "duration", duration, "error", err)
		}
		return res, err
	}

	if p.log != nil {
		p.log.Info("Rollback succeeded", "provider", p.Name(), "profile", profile, "version", targetVersion, "duration", duration)
	}
	return res, nil
}

// Scale instrumented execution.
func (p *ObservabilityProvider) Scale(ctx context.Context, serviceName string, replicas int) (providers.Result, error) {
	res, err := p.next.Scale(ctx, serviceName, replicas)
	if err != nil {
		if p.log != nil {
			p.log.Error("Scaling failed", "provider", p.Name(), "service", serviceName, "replicas", replicas, "error", err)
		}
		return res, err
	}
	if p.log != nil {
		p.log.Info("Scaling succeeded", "provider", p.Name(), "service", serviceName, "replicas", replicas)
	}
	return res, nil
}

// Validate instrumented execution.
func (p *ObservabilityProvider) Validate(ctx context.Context, profile string, svcs []services.Service) (validation.ValidationResult, error) {
	p.metrics.Inc(metrics.MetricValidationRuns)
	res, err := p.next.Validate(ctx, profile, svcs)
	if err != nil || !res.IsValid {
		p.metrics.Inc(metrics.MetricValidationErrors)
		if p.log != nil {
			p.log.Warn("Validation errors found", "provider", p.Name(), "profile", profile, "errors", res.Errors)
		}
	}
	return res, err
}

// Status delegates.
func (p *ObservabilityProvider) Status(ctx context.Context, profile string) (providers.StatusResult, error) {
	return p.next.Status(ctx, profile)
}

// Destroy delegates.
func (p *ObservabilityProvider) Destroy(ctx context.Context, profile string) (providers.Result, error) {
	return p.next.Destroy(ctx, profile)
}

// Generate delegates.
func (p *ObservabilityProvider) Generate(ctx context.Context, profile string, svcs []services.Service) (providers.GeneratedArtifacts, error) {
	return p.next.Generate(ctx, profile, svcs)
}
