package middleware

import (
	"context"
	"time"

	"cpip/internal/configuration/logger"
	"cpip/internal/configuration/metrics"
	"cpip/internal/configuration/providers"
)

// ObservedProvider wraps any Provider to record access metrics and logs.
type ObservedProvider struct {
	inner   providers.Provider
	logger  *logger.Logger
	metrics metrics.Recorder
}

// NewObservedProvider decorates a configuration Provider.
func NewObservedProvider(inner providers.Provider, log *logger.Logger, rec metrics.Recorder) *ObservedProvider {
	return &ObservedProvider{
		inner:   inner,
		logger:  log,
		metrics: rec,
	}
}

func (op *ObservedProvider) Name() string { return op.inner.Name() }

func (op *ObservedProvider) Load(ctx context.Context) (map[string]string, error) {
	start := time.Now()
	op.metrics.Inc(metrics.MetricConfigLoads)

	data, err := op.inner.Load(ctx)

	duration := time.Since(start)
	if err != nil {
		op.metrics.Inc(metrics.MetricProviderErrors)
		if op.logger != nil {
			op.logger.Error("Provider failed to load",
				"provider", op.inner.Name(),
				"error", err,
				"duration_ms", duration.Milliseconds(),
			)
		}
		return nil, err
	}

	if op.logger != nil {
		op.logger.Debug("Provider loaded configurations",
			"provider", op.inner.Name(),
			"keys_count", len(data),
			"duration_ms", duration.Milliseconds(),
		)
	}

	return data, nil
}

func (op *ObservedProvider) Get(ctx context.Context, key string) (string, bool, error) {
	val, ok, err := op.inner.Get(ctx, key)
	if err != nil {
		op.metrics.Inc(metrics.MetricProviderErrors)
		if op.logger != nil {
			op.logger.Error("Provider Get error", "provider", op.inner.Name(), "key", key, "error", err)
		}
	}
	return val, ok, err
}

func (op *ObservedProvider) Set(ctx context.Context, key, value string) error {
	err := op.inner.Set(ctx, key, value)
	if err != nil {
		op.metrics.Inc(metrics.MetricProviderErrors)
		if op.logger != nil {
			op.logger.Error("Provider Set error", "provider", op.inner.Name(), "key", key, "error", err)
		}
	}
	return err
}

func (op *ObservedProvider) Watch() bool   { return op.inner.Watch() }
func (op *ObservedProvider) Priority() int { return op.inner.Priority() }

// ObservedSecretProvider wraps any SecretProvider to record access metrics and logs.
type ObservedSecretProvider struct {
	ObservedProvider
	innerSec providers.SecretProvider
}

// NewObservedSecretProvider decorates a SecretProvider.
func NewObservedSecretProvider(inner providers.SecretProvider, log *logger.Logger, rec metrics.Recorder) *ObservedSecretProvider {
	return &ObservedSecretProvider{
		ObservedProvider: ObservedProvider{
			inner:   inner,
			logger:  log,
			metrics: rec,
		},
		innerSec: inner,
	}
}

func (sp *ObservedSecretProvider) GetSecret(ctx context.Context, key string) (string, error) {
	start := time.Now()
	val, err := sp.innerSec.GetSecret(ctx, key)
	duration := time.Since(start)

	if err != nil {
		sp.metrics.Inc(metrics.MetricProviderErrors)
		if sp.logger != nil {
			sp.logger.Error("Secret provider lookup error",
				"provider", sp.innerSec.Name(),
				"key", key,
				"error", err,
			)
		}
		return "", err
	}

	if sp.logger != nil {
		sp.logger.Debug("Secret looked up successfully",
			"provider", sp.innerSec.Name(),
			"key", key,
			"duration_ms", duration.Milliseconds(),
		)
	}

	return val, nil
}

func (sp *ObservedSecretProvider) RotateSecret(ctx context.Context, key string) (string, error) {
	start := time.Now()
	newVal, err := sp.innerSec.RotateSecret(ctx, key)
	duration := time.Since(start)

	if err != nil {
		sp.metrics.Inc(metrics.MetricProviderErrors)
		if sp.logger != nil {
			sp.logger.Error("Secret provider rotation error",
				"provider", sp.innerSec.Name(),
				"key", key,
				"error", err,
			)
		}
		return "", err
	}

	if sp.logger != nil {
		sp.logger.Info("Secret rotated in provider",
			"provider", sp.innerSec.Name(),
			"key", key,
			"duration_ms", duration.Milliseconds(),
		)
	}

	return newVal, nil
}

// Interface compliance check
var (
	_ providers.Provider       = (*ObservedProvider)(nil)
	_ providers.SecretProvider = (*ObservedSecretProvider)(nil)
)
