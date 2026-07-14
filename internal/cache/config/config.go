// Package config defines the configuration surface for the Redis distributed
// state & caching module. Configuration is injected at construction time; there
// is no global state. Every subsystem receives only the sub-struct it needs.
package config

import (
	"time"

	"cpip/internal/cache/types"
)

// Redis describes how to connect to the Redis backend and how the connection
// pool behaves.
type Redis struct {
	// Addr is the host:port of the Redis server.
	Addr string `json:"addr"`
	// Username / Password authenticate against Redis ACLs (Redis 6+).
	Username string `json:"username"`
	Password string `json:"password"`
	// DB selects the logical database index.
	DB int `json:"db"`

	// PoolSize bounds the number of socket connections.
	PoolSize int `json:"pool_size"`
	// MinIdleConns keeps a warm pool to avoid connection-storm latency.
	MinIdleConns int `json:"min_idle_conns"`
	// DialTimeout bounds connection establishment.
	DialTimeout time.Duration `json:"dial_timeout"`
	// ReadTimeout / WriteTimeout bound a single command's socket I/O.
	ReadTimeout  time.Duration `json:"read_timeout"`
	WriteTimeout time.Duration `json:"write_timeout"`
	// PoolTimeout bounds how long a caller waits for a free connection.
	PoolTimeout time.Duration `json:"pool_timeout"`

	// MaxRetries and backoff bounds govern per-command retry inside go-redis.
	MaxRetries      int           `json:"max_retries"`
	MinRetryBackoff time.Duration `json:"min_retry_backoff"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff"`

	// KeyPrefix namespaces every key this module writes, isolating it from other
	// tenants sharing the same Redis instance.
	KeyPrefix string `json:"key_prefix"`
}

// TTL groups default expiration behavior.
type TTL struct {
	// Default is applied when a Set provides no explicit TTL.
	Default time.Duration `json:"default"`
	// Session is the lifetime of an authenticated session.
	Session time.Duration `json:"session"`
	// Presence is the lifetime of a replicated presence record (heartbeat-refreshed).
	Presence time.Duration `json:"presence"`
	// Lock is the default lease of a distributed lock.
	Lock time.Duration `json:"lock"`
	// ReaperInterval is how often the TTL manager scans for expired callbacks.
	ReaperInterval time.Duration `json:"reaper_interval"`
	// Jitter (0..1) randomizes TTLs to prevent synchronized mass expiry (thundering herd).
	Jitter float64 `json:"jitter"`
}

// Policy configures the default caching strategy and its write-behind buffer.
type Policy struct {
	// Default is the strategy used by caches that don't override it.
	Default string `json:"default"` // see policies.Strategy
	// RefreshAheadRatio (0..1) triggers a background refresh once this fraction
	// of a value's TTL has elapsed.
	RefreshAheadRatio float64 `json:"refresh_ahead_ratio"`
	// WriteBehindBuffer bounds the async write-behind queue depth.
	WriteBehindBuffer int `json:"write_behind_buffer"`
	// WriteBehindFlushInterval batches write-behind flushes.
	WriteBehindFlushInterval time.Duration `json:"write_behind_flush_interval"`
	// WriteBehindWorkers is the number of async writer goroutines.
	WriteBehindWorkers int `json:"write_behind_workers"`
}

// Lock configures the distributed lock manager.
type Lock struct {
	// DefaultLease is how long a lock is held before it must be renewed.
	DefaultLease time.Duration `json:"default_lease"`
	// AcquireTimeout bounds how long Acquire blocks before giving up.
	AcquireTimeout time.Duration `json:"acquire_timeout"`
	// RetryInterval is the spin delay between acquisition attempts.
	RetryInterval time.Duration `json:"retry_interval"`
	// AutoRenewFraction (0..1) of the lease at which the watchdog renews.
	AutoRenewFraction float64 `json:"auto_renew_fraction"`
	// ClockDriftFactor pads lease validity to tolerate clock skew (Redlock).
	ClockDriftFactor float64 `json:"clock_drift_factor"`
}

// PubSub configures the pub/sub manager.
type PubSub struct {
	// SubscriberBuffer is the per-subscription channel depth before backpressure.
	SubscriberBuffer int `json:"subscriber_buffer"`
	// ReconnectInterval is the delay between reconnect attempts on disconnect.
	ReconnectInterval time.Duration `json:"reconnect_interval"`
	// MaxReconnectBackoff caps exponential reconnect backoff.
	MaxReconnectBackoff time.Duration `json:"max_reconnect_backoff"`
	// DropOnBackpressure, when true, drops messages for slow subscribers rather
	// than blocking the router. When false, a slow subscriber applies backpressure.
	DropOnBackpressure bool `json:"drop_on_backpressure"`
}

// Replication configures presence/state replication behavior.
type Replication struct {
	// ChannelPrefix namespaces replication pub/sub channels.
	ChannelPrefix string `json:"channel_prefix"`
	// NodeID identifies this CPIP instance in replication metadata. If empty a
	// random ID is generated at startup.
	NodeID string `json:"node_id"`
	// AntiEntropyInterval is how often a full state resync is broadcast to heal
	// missed pub/sub deltas (eventual consistency safety net).
	AntiEntropyInterval time.Duration `json:"anti_entropy_interval"`
	// HeartbeatInterval is how often local presence is re-published.
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
}

// Config is the composition root of module configuration.
type Config struct {
	Redis       Redis       `json:"redis"`
	TTL         TTL         `json:"ttl"`
	Policy      Policy      `json:"policy"`
	Lock        Lock        `json:"lock"`
	PubSub      PubSub      `json:"pubsub"`
	Replication Replication `json:"replication"`
}

// Default returns a production-sensible configuration suitable for booting
// against a local Redis. Callers typically start here and override selectively.
func Default() Config {
	return Config{
		Redis: Redis{
			Addr:            "localhost:6379",
			DB:              0,
			PoolSize:        64,
			MinIdleConns:    8,
			DialTimeout:     5 * time.Second,
			ReadTimeout:     3 * time.Second,
			WriteTimeout:    3 * time.Second,
			PoolTimeout:     4 * time.Second,
			MaxRetries:      3,
			MinRetryBackoff: 8 * time.Millisecond,
			MaxRetryBackoff: 512 * time.Millisecond,
			KeyPrefix:       "cpip",
		},
		TTL: TTL{
			Default:        10 * time.Minute,
			Session:        24 * time.Hour,
			Presence:       30 * time.Second,
			Lock:           15 * time.Second,
			ReaperInterval: 1 * time.Second,
			Jitter:         0.1,
		},
		Policy: Policy{
			Default:                  "cache_aside",
			RefreshAheadRatio:        0.75,
			WriteBehindBuffer:        4096,
			WriteBehindFlushInterval: 250 * time.Millisecond,
			WriteBehindWorkers:       4,
		},
		Lock: Lock{
			DefaultLease:      15 * time.Second,
			AcquireTimeout:    5 * time.Second,
			RetryInterval:     50 * time.Millisecond,
			AutoRenewFraction: 0.5,
			ClockDriftFactor:  0.01,
		},
		PubSub: PubSub{
			SubscriberBuffer:    1024,
			ReconnectInterval:   500 * time.Millisecond,
			MaxReconnectBackoff: 10 * time.Second,
			DropOnBackpressure:  true,
		},
		Replication: Replication{
			ChannelPrefix:       "cpip:repl",
			AntiEntropyInterval: 30 * time.Second,
			HeartbeatInterval:   10 * time.Second,
		},
	}
}

// Validate normalizes zero-valued fields to their defaults and rejects
// nonsensical values, returning a normalized copy. It mirrors the queue
// module's validation contract so misconfiguration fails fast at boot.
func (c Config) Validate() (Config, error) {
	d := Default()

	// --- Redis ---
	if c.Redis.Addr == "" {
		c.Redis.Addr = d.Redis.Addr
	}
	if c.Redis.PoolSize <= 0 {
		c.Redis.PoolSize = d.Redis.PoolSize
	}
	if c.Redis.MinIdleConns < 0 {
		c.Redis.MinIdleConns = d.Redis.MinIdleConns
	}
	if c.Redis.MinIdleConns > c.Redis.PoolSize {
		c.Redis.MinIdleConns = c.Redis.PoolSize
	}
	if c.Redis.DialTimeout <= 0 {
		c.Redis.DialTimeout = d.Redis.DialTimeout
	}
	if c.Redis.ReadTimeout <= 0 {
		c.Redis.ReadTimeout = d.Redis.ReadTimeout
	}
	if c.Redis.WriteTimeout <= 0 {
		c.Redis.WriteTimeout = d.Redis.WriteTimeout
	}
	if c.Redis.PoolTimeout <= 0 {
		c.Redis.PoolTimeout = d.Redis.PoolTimeout
	}
	if c.Redis.MaxRetries < 0 {
		c.Redis.MaxRetries = d.Redis.MaxRetries
	}
	if c.Redis.MinRetryBackoff <= 0 {
		c.Redis.MinRetryBackoff = d.Redis.MinRetryBackoff
	}
	if c.Redis.MaxRetryBackoff <= 0 {
		c.Redis.MaxRetryBackoff = d.Redis.MaxRetryBackoff
	}
	if c.Redis.KeyPrefix == "" {
		c.Redis.KeyPrefix = d.Redis.KeyPrefix
	}

	// --- TTL ---
	if c.TTL.Default <= 0 {
		c.TTL.Default = d.TTL.Default
	}
	if c.TTL.Session <= 0 {
		c.TTL.Session = d.TTL.Session
	}
	if c.TTL.Presence <= 0 {
		c.TTL.Presence = d.TTL.Presence
	}
	if c.TTL.Lock <= 0 {
		c.TTL.Lock = d.TTL.Lock
	}
	if c.TTL.ReaperInterval <= 0 {
		c.TTL.ReaperInterval = d.TTL.ReaperInterval
	}
	if c.TTL.Jitter < 0 || c.TTL.Jitter > 1 {
		return Config{}, wrap("ttl.jitter must be in [0,1]")
	}

	// --- Policy ---
	if c.Policy.Default == "" {
		c.Policy.Default = d.Policy.Default
	}
	if c.Policy.RefreshAheadRatio <= 0 || c.Policy.RefreshAheadRatio >= 1 {
		c.Policy.RefreshAheadRatio = d.Policy.RefreshAheadRatio
	}
	if c.Policy.WriteBehindBuffer <= 0 {
		c.Policy.WriteBehindBuffer = d.Policy.WriteBehindBuffer
	}
	if c.Policy.WriteBehindFlushInterval <= 0 {
		c.Policy.WriteBehindFlushInterval = d.Policy.WriteBehindFlushInterval
	}
	if c.Policy.WriteBehindWorkers <= 0 {
		c.Policy.WriteBehindWorkers = d.Policy.WriteBehindWorkers
	}

	// --- Lock ---
	if c.Lock.DefaultLease <= 0 {
		c.Lock.DefaultLease = d.Lock.DefaultLease
	}
	if c.Lock.AcquireTimeout <= 0 {
		c.Lock.AcquireTimeout = d.Lock.AcquireTimeout
	}
	if c.Lock.RetryInterval <= 0 {
		c.Lock.RetryInterval = d.Lock.RetryInterval
	}
	if c.Lock.AutoRenewFraction <= 0 || c.Lock.AutoRenewFraction >= 1 {
		c.Lock.AutoRenewFraction = d.Lock.AutoRenewFraction
	}
	if c.Lock.ClockDriftFactor < 0 || c.Lock.ClockDriftFactor > 1 {
		return Config{}, wrap("lock.clock_drift_factor must be in [0,1]")
	}
	if c.Lock.ClockDriftFactor == 0 {
		c.Lock.ClockDriftFactor = d.Lock.ClockDriftFactor
	}

	// --- PubSub ---
	if c.PubSub.SubscriberBuffer <= 0 {
		c.PubSub.SubscriberBuffer = d.PubSub.SubscriberBuffer
	}
	if c.PubSub.ReconnectInterval <= 0 {
		c.PubSub.ReconnectInterval = d.PubSub.ReconnectInterval
	}
	if c.PubSub.MaxReconnectBackoff <= 0 {
		c.PubSub.MaxReconnectBackoff = d.PubSub.MaxReconnectBackoff
	}

	// --- Replication ---
	if c.Replication.ChannelPrefix == "" {
		c.Replication.ChannelPrefix = d.Replication.ChannelPrefix
	}
	if c.Replication.AntiEntropyInterval <= 0 {
		c.Replication.AntiEntropyInterval = d.Replication.AntiEntropyInterval
	}
	if c.Replication.HeartbeatInterval <= 0 {
		c.Replication.HeartbeatInterval = d.Replication.HeartbeatInterval
	}

	return c, nil
}

func wrap(msg string) error { return &configError{msg: msg} }

type configError struct{ msg string }

func (e *configError) Error() string { return "cache/config: " + e.msg }

// Is lets callers match config errors against types.ErrConfig.
func (e *configError) Is(target error) bool { return target == types.ErrConfig }
