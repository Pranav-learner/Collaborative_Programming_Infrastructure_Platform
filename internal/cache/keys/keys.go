// Package keys centralizes Redis key construction so that every subsystem
// namespaces consistently under a single configurable prefix. Keeping key
// layout in one place makes it auditable, collision-free, and trivial to
// re-shard if a future Redis Cluster hash-tag scheme is introduced.
package keys

import "strings"

// Builder produces namespaced keys and channels from a root prefix.
type Builder struct {
	prefix string
}

// New returns a Builder rooted at prefix (e.g. "cpip"). An empty prefix is
// tolerated and yields un-prefixed keys.
func New(prefix string) Builder { return Builder{prefix: prefix} }

// join assembles colon-delimited segments, skipping empties.
func (b Builder) join(parts ...string) string {
	segs := make([]string, 0, len(parts)+1)
	if b.prefix != "" {
		segs = append(segs, b.prefix)
	}
	for _, p := range parts {
		if p != "" {
			segs = append(segs, p)
		}
	}
	return strings.Join(segs, ":")
}

// Cache builds a key for a named cache entry: <prefix>:cache:<name>:<key>.
func (b Builder) Cache(name, key string) string { return b.join("cache", name, key) }

// CachePattern builds a glob matching all keys of a named cache.
func (b Builder) CachePattern(name string) string { return b.join("cache", name, "*") }

// Tag builds the key of the set that indexes members of a cache tag.
func (b Builder) Tag(tag string) string { return b.join("tag", tag) }

// Session builds the hash key for a session record.
func (b Builder) Session(id string) string { return b.join("session", id) }

// UserSessions builds the set key indexing a user's active session IDs (multi-device).
func (b Builder) UserSessions(userID string) string { return b.join("user", userID, "sessions") }

// Lock builds the key for a distributed lock on a resource.
func (b Builder) Lock(resource string) string { return b.join("lock", resource) }

// Presence builds the hash key for a user's presence record within a room.
func (b Builder) Presence(roomID, userID string) string {
	return b.join("presence", roomID, userID)
}

// PresenceRoom builds the set key indexing which users have presence in a room.
func (b Builder) PresenceRoom(roomID string) string { return b.join("presence", roomID, "members") }

// State builds a key for a distributed state datum in a namespace.
func (b Builder) State(namespace, id string) string { return b.join("state", namespace, id) }

// Channel builds a pub/sub channel name under the given prefix and topic.
func (b Builder) Channel(prefix, topic string) string {
	return strings.Join([]string{prefix, topic}, ":")
}

// Prefix returns the root prefix.
func (b Builder) Prefix() string { return b.prefix }
