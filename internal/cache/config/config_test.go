package config_test

import (
	"errors"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/types"
)

func TestDefaultValidates(t *testing.T) {
	if _, err := config.Default().Validate(); err != nil {
		t.Fatalf("default config should validate: %v", err)
	}
}

func TestZeroValuesNormalizeToDefaults(t *testing.T) {
	c, err := config.Config{}.Validate()
	if err != nil {
		t.Fatal(err)
	}
	d := config.Default()
	if c.Redis.Addr != d.Redis.Addr {
		t.Fatalf("addr = %q", c.Redis.Addr)
	}
	if c.TTL.Default != d.TTL.Default {
		t.Fatalf("ttl default = %v", c.TTL.Default)
	}
	if c.Redis.KeyPrefix != d.Redis.KeyPrefix {
		t.Fatalf("key prefix = %q", c.Redis.KeyPrefix)
	}
}

func TestInvalidJitterRejected(t *testing.T) {
	c := config.Default()
	c.TTL.Jitter = 1.5
	_, err := c.Validate()
	if !errors.Is(err, types.ErrConfig) {
		t.Fatalf("expected ErrConfig, got %v", err)
	}
}

func TestMinIdleClampedToPoolSize(t *testing.T) {
	c := config.Default()
	c.Redis.PoolSize = 4
	c.Redis.MinIdleConns = 100
	got, err := c.Validate()
	if err != nil {
		t.Fatal(err)
	}
	if got.Redis.MinIdleConns != 4 {
		t.Fatalf("min idle = %d, want clamped to 4", got.Redis.MinIdleConns)
	}
}

func TestExplicitValuesPreserved(t *testing.T) {
	c := config.Default()
	c.TTL.Session = 48 * time.Hour
	c.Lock.DefaultLease = 42 * time.Second
	got, _ := c.Validate()
	if got.TTL.Session != 48*time.Hour || got.Lock.DefaultLease != 42*time.Second {
		t.Fatal("explicit values were overwritten")
	}
}
