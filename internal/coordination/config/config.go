// Package config defines the configuration surface for the Distributed
// Coordination module. Configuration is injected at construction time; there is
// no global state. Each subsystem receives only the sub-struct it needs.
package config

import (
	"time"

	"cpip/internal/coordination/types"
)

// Heartbeat governs heartbeat publishing and expiry detection.
type Heartbeat struct {
	// Interval is how often a node publishes its heartbeat.
	Interval time.Duration `json:"interval"`
	// Timeout is how long without a heartbeat before a node is marked Suspect.
	Timeout time.Duration `json:"timeout"`
	// Expiry is how long without a heartbeat before a node is evicted (Dead).
	Expiry time.Duration `json:"expiry"`
	// MonitorInterval is how often the monitor scans for overdue nodes.
	MonitorInterval time.Duration `json:"monitor_interval"`
}

// Leader governs the leader election framework.
type Leader struct {
	// Lease is how long a leadership claim is valid without renewal.
	Lease time.Duration `json:"lease"`
	// RenewInterval is how often the leader refreshes its lease (< Lease).
	RenewInterval time.Duration `json:"renew_interval"`
	// RetryInterval is how often a follower re-campaigns after a lost race.
	RetryInterval time.Duration `json:"retry_interval"`
}

// Lock governs the distributed lock service.
type Lock struct {
	// DefaultLease is the default lock TTL.
	DefaultLease time.Duration `json:"default_lease"`
	// AcquireTimeout bounds how long Acquire blocks before giving up.
	AcquireTimeout time.Duration `json:"acquire_timeout"`
	// RetryInterval is the spin interval between acquisition attempts.
	RetryInterval time.Duration `json:"retry_interval"`
	// AutoRenewFraction is the fraction of the lease at which the watchdog renews.
	AutoRenewFraction float64 `json:"auto_renew_fraction"`
	// ClockDriftFactor discounts the lease validity to absorb clock skew (Redlock).
	ClockDriftFactor float64 `json:"clock_drift_factor"`
}

// Discovery governs service discovery caching/refresh.
type Discovery struct {
	// RefreshInterval is how often the discovery cache is refreshed from the registry.
	RefreshInterval time.Duration `json:"refresh_interval"`
	// CacheTTL bounds how stale a discovery result may be.
	CacheTTL time.Duration `json:"cache_ttl"`
}

// Replication governs the state replication framework.
type Replication struct {
	// SyncInterval is how often anti-entropy re-broadcasts local state.
	SyncInterval time.Duration `json:"sync_interval"`
	// SubscriberBuffer bounds the per-subscriber channel buffer.
	SubscriberBuffer int `json:"subscriber_buffer"`
}

// Config is the composition root of module configuration.
type Config struct {
	// ClusterID names the cluster; all nodes sharing it form one membership set.
	ClusterID string `json:"cluster_id"`
	// KeyPrefix namespaces all backend keys/channels (multi-tenant safe).
	KeyPrefix string `json:"key_prefix"`

	// Node is this process's identity/placement defaults (used when it joins).
	Node NodeIdentity `json:"node"`

	Heartbeat   Heartbeat   `json:"heartbeat"`
	Leader      Leader      `json:"leader"`
	Lock        Lock        `json:"lock"`
	Discovery   Discovery   `json:"discovery"`
	Replication Replication `json:"replication"`

	// Metadata is arbitrary cluster-level metadata surfaced in ClusterState.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// NodeIdentity is the local node's self-description used when it joins.
type NodeIdentity struct {
	ID             string
	Name           string
	Address        string
	Role           types.Role
	Region         string
	Zone           string
	Capabilities   []string
	RuntimeVersion string
	Metadata       map[string]string
}

// Default returns a production-sensible configuration for a modest cluster.
func Default() Config {
	return Config{
		ClusterID: "cpip",
		KeyPrefix: "cpip:coord",
		Node: NodeIdentity{
			Role: types.RoleGeneric,
		},
		Heartbeat: Heartbeat{
			Interval:        2 * time.Second,
			Timeout:         6 * time.Second,
			Expiry:          15 * time.Second,
			MonitorInterval: 2 * time.Second,
		},
		Leader: Leader{
			Lease:         10 * time.Second,
			RenewInterval: 3 * time.Second,
			RetryInterval: 2 * time.Second,
		},
		Lock: Lock{
			DefaultLease:      15 * time.Second,
			AcquireTimeout:    5 * time.Second,
			RetryInterval:     100 * time.Millisecond,
			AutoRenewFraction: 0.5,
			ClockDriftFactor:  0.02,
		},
		Discovery: Discovery{
			RefreshInterval: 3 * time.Second,
			CacheTTL:        5 * time.Second,
		},
		Replication: Replication{
			SyncInterval:     10 * time.Second,
			SubscriberBuffer: 256,
		},
	}
}

// Validate normalizes zero-valued fields to defaults and rejects nonsensical
// values, returning a normalized copy.
func (c Config) Validate() (Config, error) {
	d := Default()
	if c.ClusterID == "" {
		c.ClusterID = d.ClusterID
	}
	if c.KeyPrefix == "" {
		c.KeyPrefix = d.KeyPrefix
	}
	if c.Node.Role == "" {
		c.Node.Role = d.Node.Role
	}

	// Heartbeat.
	if c.Heartbeat.Interval <= 0 {
		c.Heartbeat.Interval = d.Heartbeat.Interval
	}
	if c.Heartbeat.Timeout <= 0 {
		c.Heartbeat.Timeout = d.Heartbeat.Timeout
	}
	if c.Heartbeat.Expiry <= 0 {
		c.Heartbeat.Expiry = d.Heartbeat.Expiry
	}
	if c.Heartbeat.MonitorInterval <= 0 {
		c.Heartbeat.MonitorInterval = d.Heartbeat.MonitorInterval
	}
	if c.Heartbeat.Timeout < c.Heartbeat.Interval {
		return Config{}, wrap("heartbeat.timeout must be >= heartbeat.interval")
	}
	if c.Heartbeat.Expiry < c.Heartbeat.Timeout {
		return Config{}, wrap("heartbeat.expiry must be >= heartbeat.timeout")
	}

	// Leader.
	if c.Leader.Lease <= 0 {
		c.Leader.Lease = d.Leader.Lease
	}
	if c.Leader.RenewInterval <= 0 {
		c.Leader.RenewInterval = d.Leader.RenewInterval
	}
	if c.Leader.RetryInterval <= 0 {
		c.Leader.RetryInterval = d.Leader.RetryInterval
	}
	if c.Leader.RenewInterval >= c.Leader.Lease {
		return Config{}, wrap("leader.renew_interval must be < leader.lease")
	}

	// Lock.
	if c.Lock.DefaultLease <= 0 {
		c.Lock.DefaultLease = d.Lock.DefaultLease
	}
	if c.Lock.AcquireTimeout < 0 {
		c.Lock.AcquireTimeout = d.Lock.AcquireTimeout
	}
	if c.Lock.RetryInterval <= 0 {
		c.Lock.RetryInterval = d.Lock.RetryInterval
	}
	if c.Lock.AutoRenewFraction <= 0 || c.Lock.AutoRenewFraction >= 1 {
		c.Lock.AutoRenewFraction = d.Lock.AutoRenewFraction
	}
	if c.Lock.ClockDriftFactor < 0 || c.Lock.ClockDriftFactor >= 1 {
		c.Lock.ClockDriftFactor = d.Lock.ClockDriftFactor
	}

	// Discovery.
	if c.Discovery.RefreshInterval <= 0 {
		c.Discovery.RefreshInterval = d.Discovery.RefreshInterval
	}
	if c.Discovery.CacheTTL <= 0 {
		c.Discovery.CacheTTL = d.Discovery.CacheTTL
	}

	// Replication.
	if c.Replication.SyncInterval <= 0 {
		c.Replication.SyncInterval = d.Replication.SyncInterval
	}
	if c.Replication.SubscriberBuffer <= 0 {
		c.Replication.SubscriberBuffer = d.Replication.SubscriberBuffer
	}
	return c, nil
}

func wrap(msg string) error { return &configError{msg: msg} }

type configError struct{ msg string }

func (e *configError) Error() string { return "coordination/config: " + e.msg }

// Is lets callers match config errors against types.ErrConfig.
func (e *configError) Is(target error) bool { return target == types.ErrConfig }
