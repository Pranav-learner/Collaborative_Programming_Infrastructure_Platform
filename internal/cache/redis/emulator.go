package redis

import (
	"context"
	"strconv"
	"sync"
	"time"

	"cpip/internal/cache/types"
)

// kind discriminates the Redis value type stored under a key. A single key
// holds exactly one kind, and TTL is a property of the key regardless of kind —
// matching real Redis keyspace semantics.
type kind uint8

const (
	kindString kind = iota
	kindHash
	kindSet
)

type kv struct {
	kind     kind
	str      string
	hash     map[string]string
	set      map[string]struct{}
	expireAt time.Time // zero means no expiry
}

// Emulator is an in-memory Client with faithful Redis semantics for the subset
// this module uses: string/hash/set types, per-key TTL with lazy expiry,
// atomic compare-and-* operations, and glob-based pub/sub. It is safe for
// thousands of concurrent goroutines (single coarse mutex; operations are
// O(1)/O(matching) and never block while holding the lock except pub/sub fan-out
// which is non-blocking).
type Emulator struct {
	mu     sync.Mutex
	data   map[string]*kv
	subs   map[*emSub]struct{}
	closed bool

	// now is overridable for deterministic TTL tests.
	now func() time.Time
}

// NewEmulator constructs an empty in-memory client.
func NewEmulator() *Emulator {
	return &Emulator{
		data: make(map[string]*kv),
		subs: make(map[*emSub]struct{}),
		now:  time.Now,
	}
}

// SetClock overrides the emulator clock. Not safe to call concurrently with
// other operations; intended for single-threaded test setup.
func (e *Emulator) SetClock(now func() time.Time) { e.now = now }

// live returns the entry for key if present and not expired, lazily evicting
// expired keys. Caller must hold e.mu.
func (e *Emulator) live(key string) (*kv, bool) {
	v, ok := e.data[key]
	if !ok {
		return nil, false
	}
	if !v.expireAt.IsZero() && !v.expireAt.After(e.now()) {
		delete(e.data, key)
		return nil, false
	}
	return v, true
}

func (e *Emulator) expiry(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return e.now().Add(ttl)
}

// --- Strings ---

// Get implements Client.
func (e *Emulator) Get(_ context.Context, key string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return "", types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindString {
		return "", types.ErrNil
	}
	return v.str, nil
}

// Set implements Client.
func (e *Emulator) Set(_ context.Context, key, value string, ttl time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return types.ErrRedisUnavailable
	}
	exp := e.expiry(ttl)
	if ttl == KeepTTL {
		if cur, ok := e.live(key); ok {
			exp = cur.expireAt
		} else {
			exp = time.Time{}
		}
	}
	e.data[key] = &kv{kind: kindString, str: value, expireAt: exp}
	return nil
}

// SetNX implements Client.
func (e *Emulator) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false, types.ErrRedisUnavailable
	}
	if _, ok := e.live(key); ok {
		return false, nil
	}
	e.data[key] = &kv{kind: kindString, str: value, expireAt: e.expiry(ttl)}
	return true, nil
}

// Del implements Client.
func (e *Emulator) Del(_ context.Context, keys ...string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, types.ErrRedisUnavailable
	}
	var n int64
	for _, k := range keys {
		if _, ok := e.live(k); ok {
			delete(e.data, k)
			n++
		}
	}
	return n, nil
}

// Exists implements Client.
func (e *Emulator) Exists(_ context.Context, keys ...string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, types.ErrRedisUnavailable
	}
	var n int64
	for _, k := range keys {
		if _, ok := e.live(k); ok {
			n++
		}
	}
	return n, nil
}

// Expire implements Client.
func (e *Emulator) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok {
		return false, nil
	}
	v.expireAt = e.expiry(ttl)
	return true, nil
}

// TTL implements Client.
func (e *Emulator) TTL(_ context.Context, key string) (time.Duration, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok {
		return -2 * time.Second, nil
	}
	if v.expireAt.IsZero() {
		return -1 * time.Second, nil
	}
	return v.expireAt.Sub(e.now()), nil
}

// Persist implements Client.
func (e *Emulator) Persist(_ context.Context, key string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.expireAt.IsZero() {
		return false, nil
	}
	v.expireAt = time.Time{}
	return true, nil
}

// Incr implements Client.
func (e *Emulator) Incr(_ context.Context, key string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, types.ErrRedisUnavailable
	}
	var n int64
	if v, ok := e.live(key); ok && v.kind == kindString {
		n, _ = strconv.ParseInt(v.str, 10, 64)
	}
	n++
	exp := time.Time{}
	if v, ok := e.live(key); ok {
		exp = v.expireAt
	}
	e.data[key] = &kv{kind: kindString, str: strconv.FormatInt(n, 10), expireAt: exp}
	return n, nil
}

// --- Atomic compare-and-* ---

// CompareAndDelete implements Client.
func (e *Emulator) CompareAndDelete(_ context.Context, key, expected string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindString || v.str != expected {
		return false, nil
	}
	delete(e.data, key)
	return true, nil
}

// CompareAndExtend implements Client.
func (e *Emulator) CompareAndExtend(_ context.Context, key, expected string, ttl time.Duration) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindString || v.str != expected {
		return false, nil
	}
	v.expireAt = e.expiry(ttl)
	return true, nil
}

// CompareAndSet implements Client.
func (e *Emulator) CompareAndSet(_ context.Context, key, expected, newValue string, ttl time.Duration) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if expected == "" {
		if ok {
			return false, nil // caller expected absence but key exists
		}
	} else {
		if !ok || v.kind != kindString || v.str != expected {
			return false, nil
		}
	}
	e.data[key] = &kv{kind: kindString, str: newValue, expireAt: e.expiry(ttl)}
	return true, nil
}

// --- Bulk ---

// MGet implements Client.
func (e *Emulator) MGet(_ context.Context, keys ...string) ([]*string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, types.ErrRedisUnavailable
	}
	out := make([]*string, len(keys))
	for i, k := range keys {
		if v, ok := e.live(k); ok && v.kind == kindString {
			s := v.str
			out[i] = &s
		}
	}
	return out, nil
}

// SetMany implements Client.
func (e *Emulator) SetMany(_ context.Context, pairs map[string]string, ttl time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return types.ErrRedisUnavailable
	}
	exp := e.expiry(ttl)
	for k, val := range pairs {
		e.data[k] = &kv{kind: kindString, str: val, expireAt: exp}
	}
	return nil
}

// ScanKeys implements Client.
func (e *Emulator) ScanKeys(_ context.Context, match string, _ int64) ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, types.ErrRedisUnavailable
	}
	var out []string
	for k := range e.data {
		if _, ok := e.live(k); !ok {
			continue
		}
		if match == "" || match == "*" || MatchGlob(match, k) {
			out = append(out, k)
		}
	}
	return out, nil
}

// --- Hashes ---

// HSet implements Client.
func (e *Emulator) HSet(_ context.Context, key string, fields map[string]string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindHash {
		v = &kv{kind: kindHash, hash: make(map[string]string)}
		e.data[key] = v
	}
	for f, val := range fields {
		v.hash[f] = val
	}
	return nil
}

// HGet implements Client.
func (e *Emulator) HGet(_ context.Context, key, field string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return "", types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindHash {
		return "", types.ErrNil
	}
	val, ok := v.hash[field]
	if !ok {
		return "", types.ErrNil
	}
	return val, nil
}

// HGetAll implements Client.
func (e *Emulator) HGetAll(_ context.Context, key string) (map[string]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindHash {
		return map[string]string{}, nil
	}
	out := make(map[string]string, len(v.hash))
	for f, val := range v.hash {
		out[f] = val
	}
	return out, nil
}

// HDel implements Client.
func (e *Emulator) HDel(_ context.Context, key string, fields ...string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindHash {
		return 0, nil
	}
	var n int64
	for _, f := range fields {
		if _, ok := v.hash[f]; ok {
			delete(v.hash, f)
			n++
		}
	}
	if len(v.hash) == 0 {
		delete(e.data, key)
	}
	return n, nil
}

// --- Sets ---

// SAdd implements Client.
func (e *Emulator) SAdd(_ context.Context, key string, members ...string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindSet {
		v = &kv{kind: kindSet, set: make(map[string]struct{})}
		e.data[key] = v
	}
	var n int64
	for _, m := range members {
		if _, ok := v.set[m]; !ok {
			v.set[m] = struct{}{}
			n++
		}
	}
	return n, nil
}

// SRem implements Client.
func (e *Emulator) SRem(_ context.Context, key string, members ...string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindSet {
		return 0, nil
	}
	var n int64
	for _, m := range members {
		if _, ok := v.set[m]; ok {
			delete(v.set, m)
			n++
		}
	}
	if len(v.set) == 0 {
		delete(e.data, key)
	}
	return n, nil
}

// SMembers implements Client.
func (e *Emulator) SMembers(_ context.Context, key string) ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindSet {
		return nil, nil
	}
	out := make([]string, 0, len(v.set))
	for m := range v.set {
		out = append(out, m)
	}
	return out, nil
}

// SIsMember implements Client.
func (e *Emulator) SIsMember(_ context.Context, key, member string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return false, types.ErrRedisUnavailable
	}
	v, ok := e.live(key)
	if !ok || v.kind != kindSet {
		return false, nil
	}
	_, ok = v.set[member]
	return ok, nil
}

// --- Health / lifecycle ---

// Ping implements Client.
func (e *Emulator) Ping(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return types.ErrRedisUnavailable
	}
	return nil
}

// Close implements Client.
func (e *Emulator) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	for s := range e.subs {
		s.closeLocked()
	}
	e.subs = make(map[*emSub]struct{})
	return nil
}

// Flush clears all keys (test helper).
func (e *Emulator) Flush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data = make(map[string]*kv)
}

// KeyCount returns the number of live keys (test helper).
func (e *Emulator) KeyCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for k := range e.data {
		if _, ok := e.live(k); ok {
			n++
		}
	}
	return n
}

var _ Client = (*Emulator)(nil)
