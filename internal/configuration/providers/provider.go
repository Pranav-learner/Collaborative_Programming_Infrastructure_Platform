// Package providers defines the Provider interface and common types that all
// configuration backends must implement. This is the extension point for Vault,
// Consul, K8s ConfigMaps, AWS Parameter Store, and any future backend.
package providers

import "context"

// Provider is the contract every configuration backend must satisfy.
type Provider interface {
	// Name returns a unique identifier for this provider (e.g. "env", "yaml", "vault").
	Name() string

	// Load reads all key-value pairs from this provider and returns them.
	Load(ctx context.Context) (map[string]string, error)

	// Get retrieves a single value by key. Returns ("", false) if absent.
	Get(ctx context.Context, key string) (string, bool, error)

	// Set writes a key-value pair. Not all providers support writes.
	Set(ctx context.Context, key, value string) error

	// Watch returns true if this provider supports change notification.
	Watch() bool

	// Priority returns the provider's merge priority (lower = higher priority).
	// When the same key exists in multiple providers, the lowest priority wins.
	Priority() int
}

// SecretProvider extends Provider with secret-specific operations.
type SecretProvider interface {
	Provider

	// GetSecret retrieves a secret value. Implementations must never log the value.
	GetSecret(ctx context.Context, key string) (string, error)

	// RotateSecret generates or sets a new secret value for the given key.
	RotateSecret(ctx context.Context, key string) (string, error)
}

// ReadOnlyError is returned when Set is called on a read-only provider.
type ReadOnlyError struct {
	Provider string
}

func (e *ReadOnlyError) Error() string {
	return "provider " + e.Provider + " is read-only"
}
