// Package providers — Environment variable provider.
package providers

import (
	"context"
	"os"
	"strings"
)

// EnvProvider reads configuration from OS environment variables.
type EnvProvider struct {
	prefix   string
	priority int
}

// NewEnvProvider creates an environment variable provider. If prefix is non-empty,
// only variables starting with that prefix are included and the prefix is stripped.
func NewEnvProvider(prefix string, priority int) *EnvProvider {
	return &EnvProvider{prefix: prefix, priority: priority}
}

func (p *EnvProvider) Name() string { return "env" }

func (p *EnvProvider) Load(_ context.Context) (map[string]string, error) {
	result := make(map[string]string)
	for _, entry := range os.Environ() {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := parts[0], parts[1]
		if p.prefix != "" {
			if !strings.HasPrefix(key, p.prefix) {
				continue
			}
			key = strings.TrimPrefix(key, p.prefix)
		}
		// Normalize: UPPER_SNAKE → lower.dot
		key = strings.ToLower(strings.ReplaceAll(key, "_", "."))
		result[key] = value
	}
	return result, nil
}

func (p *EnvProvider) Get(_ context.Context, key string) (string, bool, error) {
	// Convert dot notation back to env var style
	envKey := p.prefix + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	val, ok := os.LookupEnv(envKey)
	return val, ok, nil
}

func (p *EnvProvider) Set(_ context.Context, key, value string) error {
	envKey := p.prefix + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	return os.Setenv(envKey, value)
}

func (p *EnvProvider) Watch() bool  { return false }
func (p *EnvProvider) Priority() int { return p.priority }
