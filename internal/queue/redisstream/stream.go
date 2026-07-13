// Package redisstream is the adapter over Redis Streams consumer groups. It
// defines the Client interface — the exact command surface the queue uses
// (XADD, XREADGROUP, XACK, XPENDING, XAUTOCLAIM, XLEN, XDEL, XGROUP CREATE) —
// and ships two implementations:
//
//   - Redis: the production adapter backed by github.com/redis/go-redis/v9.
//   - Emulator: an in-memory implementation with faithful consumer-group
//     semantics (per-group pending-entries list, delivery counts, idle-based
//     auto-claim, blocking reads) used for unit, integration, and stress tests
//     without requiring a live Redis server.
//
// The rest of the queue depends only on the Client interface, so the same logic
// runs unchanged against Redis in production and the emulator in tests.
package redisstream

import (
	"context"
	"time"
)

// Entry is a single stream entry: its ID and field map.
type Entry struct {
	ID     string
	Fields map[string]string
}

// ReadGroupArgs parameterizes a consumer-group read (XREADGROUP).
type ReadGroupArgs struct {
	Group    string
	Consumer string
	// Stream is the stream to read from. The special ID ">" delivers new,
	// never-delivered messages; this adapter supports ">" reads.
	Stream string
	Count  int
	Block  time.Duration
	NoAck  bool
}

// PendingArgs parameterizes an extended pending query (XPENDING ... IDLE).
type PendingArgs struct {
	Stream   string
	Group    string
	Idle     time.Duration
	Start    string // "-" for the beginning
	End      string // "+" for the end
	Count    int
	Consumer string // optional filter
}

// PendingEntry describes one message in a group's pending-entries list.
type PendingEntry struct {
	ID            string
	Consumer      string
	Idle          time.Duration
	DeliveryCount int64
}

// AutoClaimArgs parameterizes an idle-reclaim scan (XAUTOCLAIM).
type AutoClaimArgs struct {
	Stream   string
	Group    string
	Consumer string
	MinIdle  time.Duration
	Start    string // "0" to scan from the beginning
	Count    int
}

// Client is the Redis Streams command surface used by the queue. All methods
// take a context and must be safe for concurrent use.
type Client interface {
	// Add appends an entry to a stream (XADD) and returns its ID.
	Add(ctx context.Context, stream string, fields map[string]string) (string, error)
	// CreateGroup creates a consumer group (XGROUP CREATE ... MKSTREAM). start is
	// "0" (from the beginning) or "$" (only new messages). An existing group is
	// not an error.
	CreateGroup(ctx context.Context, stream, group, start string) error
	// ReadGroup reads new messages for a consumer (XREADGROUP ... >), moving them
	// into the group's pending-entries list.
	ReadGroup(ctx context.Context, args ReadGroupArgs) ([]Entry, error)
	// Ack acknowledges entries, removing them from the pending-entries list (XACK).
	Ack(ctx context.Context, stream, group string, ids ...string) (int64, error)
	// Pending returns pending entries matching the query (XPENDING ... IDLE).
	Pending(ctx context.Context, args PendingArgs) ([]PendingEntry, error)
	// AutoClaim reassigns idle pending entries to a consumer (XAUTOCLAIM),
	// returning the next scan cursor and the claimed entries.
	AutoClaim(ctx context.Context, args AutoClaimArgs) (nextCursor string, entries []Entry, err error)
	// Len returns the number of entries in a stream (XLEN).
	Len(ctx context.Context, stream string) (int64, error)
	// Delete removes entries from a stream (XDEL).
	Delete(ctx context.Context, stream string, ids ...string) (int64, error)
	// Close releases any resources held by the client.
	Close() error
}
