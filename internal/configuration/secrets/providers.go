package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"cpip/internal/configuration/providers"
)

// MemorySecretProvider stores secrets in-memory in plain text (for testing/development).
type MemorySecretProvider struct {
	mu       sync.RWMutex
	secrets  map[string]string
	priority int
}

func NewMemorySecretProvider(priority int) *MemorySecretProvider {
	return &MemorySecretProvider{
		secrets:  make(map[string]string),
		priority: priority,
	}
}

func (p *MemorySecretProvider) Name() string { return "memory_secrets" }
func (p *MemorySecretProvider) Load(_ context.Context) (map[string]string, error) {
	return nil, errors.New("load not supported for secret provider")
}
func (p *MemorySecretProvider) Get(_ context.Context, _ string) (string, bool, error) {
	return "", false, errors.New("direct get not supported; use GetSecret")
}
func (p *MemorySecretProvider) Set(_ context.Context, _, _ string) error {
	return errors.New("direct set not supported")
}
func (p *MemorySecretProvider) Watch() bool    { return false }
func (p *MemorySecretProvider) Priority() int  { return p.priority }

func (p *MemorySecretProvider) GetSecret(_ context.Context, key string) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	val, ok := p.secrets[key]
	if !ok {
		return "", fmt.Errorf("secret not found in memory: %s", key)
	}
	return val, nil
}

func (p *MemorySecretProvider) SetSecret(key, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.secrets[key] = value
}

func (p *MemorySecretProvider) RotateSecret(_ context.Context, key string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.secrets[key]
	if !ok {
		return "", fmt.Errorf("cannot rotate non-existent secret: %s", key)
	}
	// Generate random 16-byte hex secret
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	newVal := hex.EncodeToString(b)
	p.secrets[key] = newVal
	return newVal, nil
}

// EnvSecretProvider resolves secrets from environment variables.
type EnvSecretProvider struct {
	prefix   string
	priority int
}

func NewEnvSecretProvider(prefix string, priority int) *EnvSecretProvider {
	return &EnvSecretProvider{prefix: prefix, priority: priority}
}

func (p *EnvSecretProvider) Name() string { return "env_secrets" }
func (p *EnvSecretProvider) Load(_ context.Context) (map[string]string, error) {
	return nil, errors.New("load not supported for secret provider")
}
func (p *EnvSecretProvider) Get(_ context.Context, _ string) (string, bool, error) {
	return "", false, errors.New("direct get not supported; use GetSecret")
}
func (p *EnvSecretProvider) Set(_ context.Context, _, _ string) error {
	return errors.New("direct set not supported")
}
func (p *EnvSecretProvider) Watch() bool    { return false }
func (p *EnvSecretProvider) Priority() int  { return p.priority }

func (p *EnvSecretProvider) GetSecret(_ context.Context, key string) (string, error) {
	// Normalize key to env variable name: DB_PASSWORD -> prefix + DB_PASSWORD
	envKey := p.prefix + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	val, ok := os.LookupEnv(envKey)
	if !ok {
		return "", fmt.Errorf("secret environment variable %s not set", envKey)
	}
	return val, nil
}

func (p *EnvSecretProvider) RotateSecret(_ context.Context, _ string) (string, error) {
	return "", errors.New("rotation not supported for environment variables")
}

// EncryptedFileSecretProvider loads and decrypts secrets from a file using AES-GCM.
type EncryptedFileSecretProvider struct {
	mu       sync.RWMutex
	path     string
	key      []byte
	secrets  map[string]string
	priority int
}

// NewEncryptedFileSecretProvider constructs the provider. The key must be 32 bytes (256-bit).
func NewEncryptedFileSecretProvider(path string, keyHex string, priority int) (*EncryptedFileSecretProvider, error) {
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("invalid encryption key: must be 32-byte hex string: %w", err)
	}

	return &EncryptedFileSecretProvider{
		path:     path,
		key:      key,
		secrets:  make(map[string]string),
		priority: priority,
	}, nil
}

func (p *EncryptedFileSecretProvider) Name() string { return "encrypted_file_secrets" }
func (p *EncryptedFileSecretProvider) Load(_ context.Context) (map[string]string, error) {
	return nil, errors.New("load not supported for secret provider")
}
func (p *EncryptedFileSecretProvider) Get(_ context.Context, _ string) (string, bool, error) {
	return "", false, errors.New("direct get not supported; use GetSecret")
}
func (p *EncryptedFileSecretProvider) Set(_ context.Context, _, _ string) error {
	return errors.New("direct set not supported")
}
func (p *EncryptedFileSecretProvider) Watch() bool    { return true }
func (p *EncryptedFileSecretProvider) Priority() int  { return p.priority }

func (p *EncryptedFileSecretProvider) GetSecret(ctx context.Context, key string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.secrets) == 0 {
		if err := p.loadAndDecrypt(); err != nil {
			return "", err
		}
	}

	val, ok := p.secrets[key]
	if !ok {
		return "", fmt.Errorf("secret not found in encrypted file: %s", key)
	}
	return val, nil
}

func (p *EncryptedFileSecretProvider) RotateSecret(ctx context.Context, key string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.secrets) == 0 {
		_ = p.loadAndDecrypt()
	}

	_, ok := p.secrets[key]
	if !ok {
		return "", fmt.Errorf("cannot rotate non-existent secret: %s", key)
	}

	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	newVal := hex.EncodeToString(b)
	p.secrets[key] = newVal

	if err := p.encryptAndSave(); err != nil {
		return "", fmt.Errorf("failed to save rotated secrets: %w", err)
	}

	return newVal, nil
}

// WriteSecret programmatic writer for testing or initialization
func (p *EncryptedFileSecretProvider) WriteSecret(key, value string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.secrets) == 0 {
		_ = p.loadAndDecrypt()
	}

	p.secrets[key] = value
	return p.encryptAndSave()
}

func (p *EncryptedFileSecretProvider) loadAndDecrypt() error {
	f, err := os.Open(p.path)
	if err != nil {
		if os.IsNotExist(err) {
			p.secrets = make(map[string]string)
			return nil
		}
		return err
	}
	defer f.Close()

	ciphertext, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	if len(ciphertext) == 0 {
		p.secrets = make(map[string]string)
		return nil
	}

	block, err := aes.NewCipher(p.key)
	if err != nil {
		return err
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	nonceSize := aesGCM.NonceSize()
	if len(ciphertext) < nonceSize {
		return errors.New("ciphertext too short")
	}

	nonce, encryptedData := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := aesGCM.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return fmt.Errorf("failed to decrypt secrets file: %w", err)
	}

	var data map[string]string
	if err := json.Unmarshal(plaintext, &data); err != nil {
		return fmt.Errorf("failed to unmarshal decrypted secrets: %w", err)
	}

	p.secrets = data
	return nil
}

func (p *EncryptedFileSecretProvider) encryptAndSave() error {
	plaintext, err := json.Marshal(p.secrets)
	if err != nil {
		return err
	}

	block, err := aes.NewCipher(p.key)
	if err != nil {
		return err
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}

	ciphertext := aesGCM.Seal(nonce, nonce, plaintext, nil)
	return os.WriteFile(p.path, ciphertext, 0600)
}

// Interface compliance checks.
var (
	_ providers.SecretProvider = (*MemorySecretProvider)(nil)
	_ providers.SecretProvider = (*EnvSecretProvider)(nil)
	_ providers.SecretProvider = (*EncryptedFileSecretProvider)(nil)
)
