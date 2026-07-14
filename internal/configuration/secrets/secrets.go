package secrets

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"cpip/internal/configuration/events"
	"cpip/internal/configuration/logger"
	"cpip/internal/configuration/metrics"
	"cpip/internal/configuration/providers"
)

var (
	ErrSecretNotFound = errors.New("secret not found")
)

// SecretMetadata holds metadata for a tracked secret.
type SecretMetadata struct {
	Version      int       `json:"version"`
	CreatedAt    time.Time `json:"created_at"`
	LastRotated  time.Time `json:"last_rotated"`
	ProviderName string    `json:"provider_name"`
}

// SecretManager manages secret lookups, rotation, and version tracking.
type SecretManager struct {
	mu        sync.RWMutex
	providers []providers.SecretProvider
	metadata  map[string]*SecretMetadata
	maskChar  string
	logger    *logger.Logger
	metrics   metrics.Recorder
	bus       *events.Bus
}

// NewSecretManager constructs a SecretManager.
func NewSecretManager(maskChar string, log *logger.Logger, rec metrics.Recorder, bus *events.Bus) *SecretManager {
	if maskChar == "" {
		maskChar = "•"
	}
	return &SecretManager{
		metadata: make(map[string]*SecretMetadata),
		maskChar: maskChar,
		logger:   log,
		metrics:  rec,
		bus:      bus,
	}
}

// RegisterProvider registers a secret provider.
func (sm *SecretManager) RegisterProvider(p providers.SecretProvider) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.providers = append(sm.providers, p)
	if sm.bus != nil {
		sm.bus.Publish(events.Event{
			Type:      events.ProviderRegistered,
			Timestamp: time.Now(),
			Provider:  p.Name(),
			Detail:    "Registered secret provider",
		})
	}
}

// Get retrieves a secret value from registered providers in priority order.
func (sm *SecretManager) Get(ctx context.Context, key string) (string, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.metrics.Inc(metrics.MetricSecretLookups)

	for _, p := range sm.providers {
		val, err := p.GetSecret(ctx, key)
		if err == nil {
			if sm.logger != nil {
				sm.logger.SecretAccess(key, p.Name())
			}

			// Upsert metadata
			meta, exists := sm.metadata[key]
			if !exists {
				meta = &SecretMetadata{
					Version:      1,
					CreatedAt:    time.Now(),
					LastRotated:  time.Now(),
					ProviderName: p.Name(),
				}
				sm.metadata[key] = meta
			}

			if sm.bus != nil {
				sm.bus.Publish(events.Event{
					Type:      events.SecretLoaded,
					Timestamp: time.Now(),
					Key:       key,
					Provider:  p.Name(),
					Version:   meta.Version,
				})
			}

			return val, nil
		}
	}

	return "", fmt.Errorf("%w: %s", ErrSecretNotFound, key)
}

// Mask masks a secret value using the configured masking character.
func (sm *SecretManager) Mask(secret string) string {
	if len(secret) == 0 {
		return ""
	}
	if len(secret) <= 4 {
		return sm.maskChar + sm.maskChar + sm.maskChar + sm.maskChar
	}
	// Show last 3 characters, mask the rest
	maskedLen := len(secret) - 3
	if maskedLen > 8 {
		maskedLen = 8 // cap mask length
	}
	out := ""
	for i := 0; i < maskedLen; i++ {
		out += sm.maskChar
	}
	return out + secret[len(secret)-3:]
}

// Rotate triggers secret rotation in the provider that hosts the secret.
func (sm *SecretManager) Rotate(ctx context.Context, key string) (string, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.metrics.Inc(metrics.MetricSecretRotations)

	for _, p := range sm.providers {
		// Check if secret exists first
		_, err := p.GetSecret(ctx, key)
		if err == nil {
			newVal, err := p.RotateSecret(ctx, key)
			if err != nil {
				return "", fmt.Errorf("failed to rotate secret %s in provider %s: %w", key, p.Name(), err)
			}

			meta, exists := sm.metadata[key]
			if !exists {
				meta = &SecretMetadata{Version: 1, CreatedAt: time.Now()}
				sm.metadata[key] = meta
			}
			meta.Version++
			meta.LastRotated = time.Now()
			meta.ProviderName = p.Name()

			if sm.bus != nil {
				sm.bus.Publish(events.Event{
					Type:      events.SecretRotated,
					Timestamp: time.Now(),
					Key:       key,
					Provider:  p.Name(),
					Version:   meta.Version,
				})
			}

			return newVal, nil
		}
	}

	return "", fmt.Errorf("rotation failed: secret %s not found in any provider", key)
}

// GetMetadata retrieves metadata for a tracked secret.
func (sm *SecretManager) GetMetadata(key string) (SecretMetadata, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	meta, ok := sm.metadata[key]
	if !ok {
		return SecretMetadata{}, false
	}
	return *meta, true
}

// GenerateRandomKey is a utility to generate strong random keys (e.g. for AES keys).
func GenerateRandomKey(bytes int) (string, error) {
	b := make([]byte, bytes)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
