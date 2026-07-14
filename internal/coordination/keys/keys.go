// Package keys centralizes coordination backend key and channel construction so
// every subsystem namespaces identically under a single configurable prefix.
// Keeping key layout in one place avoids drift between the writer and the scanner
// (e.g. the registry that writes node keys and the discovery sweep that scans
// them) and makes the whole keyspace multi-tenant safe.
package keys

import "strings"

// Builder builds namespaced keys/channels for one cluster under one prefix.
type Builder struct {
	prefix    string
	clusterID string
}

// New returns a Builder rooted at "<prefix>:<clusterID>".
func New(prefix, clusterID string) Builder {
	return Builder{prefix: strings.TrimRight(prefix, ":"), clusterID: clusterID}
}

func (b Builder) root(parts ...string) string {
	all := append([]string{b.prefix, b.clusterID}, parts...)
	return strings.Join(all, ":")
}

// NodeKey is the KV key holding a node's serialized record.
func (b Builder) NodeKey(nodeID string) string { return b.root("node", nodeID) }

// NodePrefix is the scan prefix for all node records.
func (b Builder) NodePrefix() string { return b.root("node") + ":" }

// MembersSet is the set key holding the roster of member node IDs.
func (b Builder) MembersSet() string { return b.root("members") }

// HeartbeatKey is the TTL key that proves a node is alive.
func (b Builder) HeartbeatKey(nodeID string) string { return b.root("hb", nodeID) }

// LeaderKey is the single key holding the current leader lease.
func (b Builder) LeaderKey(scope string) string { return b.root("leader", scope) }

// LockKey is the key backing a named distributed lock.
func (b Builder) LockKey(resource string) string { return b.root("lock", resource) }

// EventChannel is the pub/sub channel carrying cluster events.
func (b Builder) EventChannel() string { return b.root("events") }

// ReplicationChannel is the pub/sub channel for a state-replication domain.
func (b Builder) ReplicationChannel(domain string) string { return b.root("repl", domain) }

// Prefix returns the fully-qualified root prefix (for diagnostics).
func (b Builder) Prefix() string { return b.root() }
