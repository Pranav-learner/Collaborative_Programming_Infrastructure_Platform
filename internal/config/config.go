// Package config defines the typed, validated, immutable configuration for the
// CPIP WebSocket gateway. Configuration is loaded once at startup from the
// environment (12-factor) and never mutated at runtime.
//
// Loading fails fast: any invalid or contradictory value aborts boot with a
// descriptive error rather than silently falling back to a default that could
// weaken a limit or an isolation boundary.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the complete, validated configuration for a gateway node.
// It is safe to share by value; callers must treat it as read-only.
type Config struct {
	// HTTP server.
	ListenAddr        string        // address the HTTP/WS server binds to
	ReadHeaderTimeout time.Duration // bounds the request-header read (slowloris guard)
	ShutdownTimeout   time.Duration // max time to drain on graceful shutdown

	// WebSocket / connection tuning.
	HeartbeatInterval time.Duration // how often the server sends a ping
	PongTimeout       time.Duration // max time to await any read (incl. pong) before the conn is dead
	WriteTimeout      time.Duration // per-write deadline
	MaxPayloadBytes   int64         // maximum inbound message size
	SendQueueSize     int           // per-connection outbound buffer depth (backpressure bound)
	MaxConnections    int           // global cap on concurrent connections per node
	HandshakeTimeout  time.Duration // WebSocket upgrade handshake timeout
	ReadBufferSize    int           // upgrader read buffer
	WriteBufferSize   int           // upgrader write buffer

	// Security.
	AllowedOrigins     []string // permitted Origin values; ["*"] disables the check (dev only)
	AuthAllowAnonymous bool     // dummy auth: mint an anonymous identity when no user id is supplied

	// Observability.
	LogLevel  string // debug|info|warn|error
	LogFormat string // json|text
}

// Default returns a Config populated with production-sane defaults. Load starts
// from these and overlays environment overrides.
func Default() Config {
	return Config{
		ListenAddr:        ":8080",
		ReadHeaderTimeout: 10 * time.Second,
		ShutdownTimeout:   30 * time.Second,

		HeartbeatInterval: 30 * time.Second,
		PongTimeout:       60 * time.Second,
		WriteTimeout:      10 * time.Second,
		MaxPayloadBytes:   1 << 20, // 1 MiB
		SendQueueSize:     256,
		MaxConnections:    100_000,
		HandshakeTimeout:  10 * time.Second,
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,

		AllowedOrigins:     []string{"*"},
		AuthAllowAnonymous: true,

		LogLevel:  "info",
		LogFormat: "json",
	}
}

// Load builds a Config from Default() overlaid with CPIP_* environment
// variables, then validates it. A returned error means the process must not
// start.
func Load() (Config, error) {
	c := Default()

	c.ListenAddr = envString("CPIP_LISTEN_ADDR", c.ListenAddr)
	c.LogLevel = envString("CPIP_LOG_LEVEL", c.LogLevel)
	c.LogFormat = envString("CPIP_LOG_FORMAT", c.LogFormat)

	var err error
	if c.ReadHeaderTimeout, err = envDuration("CPIP_READ_HEADER_TIMEOUT", c.ReadHeaderTimeout); err != nil {
		return Config{}, err
	}
	if c.ShutdownTimeout, err = envDuration("CPIP_SHUTDOWN_TIMEOUT", c.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if c.HeartbeatInterval, err = envDuration("CPIP_HEARTBEAT_INTERVAL", c.HeartbeatInterval); err != nil {
		return Config{}, err
	}
	if c.PongTimeout, err = envDuration("CPIP_PONG_TIMEOUT", c.PongTimeout); err != nil {
		return Config{}, err
	}
	if c.WriteTimeout, err = envDuration("CPIP_WRITE_TIMEOUT", c.WriteTimeout); err != nil {
		return Config{}, err
	}
	if c.HandshakeTimeout, err = envDuration("CPIP_HANDSHAKE_TIMEOUT", c.HandshakeTimeout); err != nil {
		return Config{}, err
	}
	if c.MaxPayloadBytes, err = envInt64("CPIP_MAX_PAYLOAD_BYTES", c.MaxPayloadBytes); err != nil {
		return Config{}, err
	}
	if c.SendQueueSize, err = envInt("CPIP_SEND_QUEUE_SIZE", c.SendQueueSize); err != nil {
		return Config{}, err
	}
	if c.MaxConnections, err = envInt("CPIP_MAX_CONNECTIONS", c.MaxConnections); err != nil {
		return Config{}, err
	}
	if c.ReadBufferSize, err = envInt("CPIP_READ_BUFFER_SIZE", c.ReadBufferSize); err != nil {
		return Config{}, err
	}
	if c.WriteBufferSize, err = envInt("CPIP_WRITE_BUFFER_SIZE", c.WriteBufferSize); err != nil {
		return Config{}, err
	}
	if c.AuthAllowAnonymous, err = envBool("CPIP_AUTH_ALLOW_ANONYMOUS", c.AuthAllowAnonymous); err != nil {
		return Config{}, err
	}
	if v := os.Getenv("CPIP_ALLOWED_ORIGINS"); v != "" {
		c.AllowedOrigins = splitAndTrim(v)
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate enforces the invariants the runtime depends on. The relationships
// between timeouts are load-bearing (see PROTOCOL.md §13): getting them wrong
// causes heartbeat flapping or false disconnects.
func (c Config) Validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("config: ListenAddr must not be empty")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("config: HeartbeatInterval must be > 0")
	}
	if c.PongTimeout <= c.HeartbeatInterval {
		return fmt.Errorf("config: PongTimeout (%s) must be greater than HeartbeatInterval (%s)", c.PongTimeout, c.HeartbeatInterval)
	}
	if c.WriteTimeout <= 0 {
		return fmt.Errorf("config: WriteTimeout must be > 0")
	}
	if c.MaxPayloadBytes <= 0 {
		return fmt.Errorf("config: MaxPayloadBytes must be > 0")
	}
	if c.SendQueueSize <= 0 {
		return fmt.Errorf("config: SendQueueSize must be > 0")
	}
	if c.MaxConnections <= 0 {
		return fmt.Errorf("config: MaxConnections must be > 0")
	}
	if c.ReadBufferSize <= 0 || c.WriteBufferSize <= 0 {
		return fmt.Errorf("config: Read/WriteBufferSize must be > 0")
	}
	if len(c.AllowedOrigins) == 0 {
		return fmt.Errorf("config: AllowedOrigins must not be empty (use [\"*\"] to allow all in development)")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("config: LogLevel %q is invalid (want debug|info|warn|error)", c.LogLevel)
	}
	switch c.LogFormat {
	case "json", "text":
	default:
		return fmt.Errorf("config: LogFormat %q is invalid (want json|text)", c.LogFormat)
	}
	return nil
}

// --- env helpers ---

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid duration: %w", key, v, err)
	}
	return d, nil
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid integer: %w", key, v, err)
	}
	return n, nil
}

func envInt64(key string, def int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid integer: %w", key, v, err)
	}
	return n, nil
}

func envBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("config: %s=%q is not a valid bool: %w", key, v, err)
	}
	return b, nil
}

func splitAndTrim(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
