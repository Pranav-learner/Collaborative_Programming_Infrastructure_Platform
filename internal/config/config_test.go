package config_test

import (
	"testing"
	"time"

	"cpip/internal/config"
)

func TestDefaultIsValid(t *testing.T) {
	if err := config.Default().Validate(); err != nil {
		t.Fatalf("default config is invalid: %v", err)
	}
}

func TestValidate_PongMustExceedHeartbeat(t *testing.T) {
	c := config.Default()
	c.HeartbeatInterval = 60 * time.Second
	c.PongTimeout = 60 * time.Second // equal -> invalid
	if err := c.Validate(); err == nil {
		t.Fatal("expected error when PongTimeout <= HeartbeatInterval")
	}
}

func TestValidate_RejectsNonPositive(t *testing.T) {
	cases := map[string]func(*config.Config){
		"send queue":      func(c *config.Config) { c.SendQueueSize = 0 },
		"max connections": func(c *config.Config) { c.MaxConnections = 0 },
		"max payload":     func(c *config.Config) { c.MaxPayloadBytes = 0 },
		"write timeout":   func(c *config.Config) { c.WriteTimeout = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := config.Default()
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatalf("expected error for %s", name)
			}
		})
	}
}

func TestValidate_RejectsEmptyOrigins(t *testing.T) {
	c := config.Default()
	c.AllowedOrigins = nil
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for empty AllowedOrigins")
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("CPIP_LISTEN_ADDR", ":9999")
	t.Setenv("CPIP_HEARTBEAT_INTERVAL", "5s")
	t.Setenv("CPIP_PONG_TIMEOUT", "12s")
	t.Setenv("CPIP_ALLOWED_ORIGINS", "https://a.com, https://b.com")

	c, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.ListenAddr != ":9999" {
		t.Fatalf("ListenAddr = %q", c.ListenAddr)
	}
	if c.HeartbeatInterval != 5*time.Second || c.PongTimeout != 12*time.Second {
		t.Fatalf("durations not overridden: hb=%s pong=%s", c.HeartbeatInterval, c.PongTimeout)
	}
	if len(c.AllowedOrigins) != 2 {
		t.Fatalf("AllowedOrigins = %v", c.AllowedOrigins)
	}
}

func TestLoad_InvalidDurationFails(t *testing.T) {
	t.Setenv("CPIP_PONG_TIMEOUT", "not-a-duration")
	if _, err := config.Load(); err == nil {
		t.Fatal("expected Load to fail on invalid duration")
	}
}
