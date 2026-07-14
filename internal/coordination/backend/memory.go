package backend

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Memory is a self-contained, thread-safe in-memory Backend. It faithfully
// implements TTL expiry, atomic compare-and-* semantics, sets, prefix scan, and
// in-process pub/sub, so it is both the reference backend for tests and a valid
// single-node deployment target. A single mutex serializes all operations, which
// is more than adequate for thousands of nodes on one process; the Redis backend
// is the horizontally-scalable path.
type Memory struct {
	mu     sync.Mutex
	kv     map[string]entry
	sets   map[string]map[string]struct{}
	subs   map[string]map[*memSub]struct{}
	closed bool
	now    func() time.Time
}

type entry struct {
	value    string
	expireAt time.Time // zero = no expiry
}

// NewMemory constructs an empty in-memory backend.
func NewMemory() *Memory {
	return &Memory{
		kv:   make(map[string]entry),
		sets: make(map[string]map[string]struct{}),
		subs: make(map[string]map[*memSub]struct{}),
		now:  time.Now,
	}
}

// liveLocked returns the entry if present and unexpired, lazily evicting on read.
func (m *Memory) liveLocked(key string) (entry, bool) {
	e, ok := m.kv[key]
	if !ok {
		return entry{}, false
	}
	if !e.expireAt.IsZero() && !m.now().Before(e.expireAt) {
		delete(m.kv, key)
		return entry{}, false
	}
	return e, true
}

func (m *Memory) expiry(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return m.now().Add(ttl)
}

func (m *Memory) Get(_ context.Context, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.liveLocked(key)
	return e.value, ok, nil
}

func (m *Memory) Set(_ context.Context, key, value string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = entry{value: value, expireAt: m.expiry(ttl)}
	return nil
}

func (m *Memory) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.liveLocked(key); ok {
		return false, nil
	}
	m.kv[key] = entry{value: value, expireAt: m.expiry(ttl)}
	return true, nil
}

func (m *Memory) Delete(_ context.Context, keys ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, k := range keys {
		if _, ok := m.liveLocked(k); ok {
			delete(m.kv, k)
			n++
		}
	}
	return n, nil
}

func (m *Memory) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.liveLocked(key)
	if !ok {
		return false, nil
	}
	e.expireAt = m.expiry(ttl)
	m.kv[key] = e
	return true, nil
}

func (m *Memory) TTL(_ context.Context, key string) (time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.liveLocked(key)
	if !ok {
		return -2 * time.Second, nil
	}
	if e.expireAt.IsZero() {
		return -1 * time.Second, nil
	}
	return time.Until(e.expireAt), nil
}

func (m *Memory) CompareAndSwap(_ context.Context, key, expected, newValue string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.liveLocked(key)
	if !ok {
		if expected != "" {
			return false, nil
		}
	} else if e.value != expected {
		return false, nil
	}
	m.kv[key] = entry{value: newValue, expireAt: m.expiry(ttl)}
	return true, nil
}

func (m *Memory) CompareAndDelete(_ context.Context, key, expected string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.liveLocked(key)
	if !ok || e.value != expected {
		return false, nil
	}
	delete(m.kv, key)
	return true, nil
}

func (m *Memory) CompareAndExpire(_ context.Context, key, expected string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.liveLocked(key)
	if !ok || e.value != expected {
		return false, nil
	}
	e.expireAt = m.expiry(ttl)
	m.kv[key] = e
	return true, nil
}

func (m *Memory) SAdd(_ context.Context, key string, members ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sets[key]
	if s == nil {
		s = make(map[string]struct{})
		m.sets[key] = s
	}
	var n int64
	for _, mem := range members {
		if _, ok := s[mem]; !ok {
			s[mem] = struct{}{}
			n++
		}
	}
	return n, nil
}

func (m *Memory) SRem(_ context.Context, key string, members ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sets[key]
	if s == nil {
		return 0, nil
	}
	var n int64
	for _, mem := range members {
		if _, ok := s[mem]; ok {
			delete(s, mem)
			n++
		}
	}
	if len(s) == 0 {
		delete(m.sets, key)
	}
	return n, nil
}

func (m *Memory) SMembers(_ context.Context, key string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sets[key]
	out := make([]string, 0, len(s))
	for mem := range s {
		out = append(out, mem)
	}
	return out, nil
}

func (m *Memory) Scan(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.kv {
		if _, ok := m.liveLocked(k); !ok {
			continue
		}
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (m *Memory) Publish(_ context.Context, channel, payload string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	targets := make([]*memSub, 0, len(m.subs[channel]))
	for s := range m.subs[channel] {
		targets = append(targets, s)
	}
	m.mu.Unlock()
	// Deliver outside the lock; drop on a full buffer so a slow subscriber can
	// never stall a publisher (best-effort, like Redis pub/sub).
	for _, s := range targets {
		select {
		case s.ch <- payload:
		default:
		}
	}
	return nil
}

func (m *Memory) Subscribe(_ context.Context, channel string) (Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, nil
	}
	sub := &memSub{ch: make(chan string, 256), backend: m, channel: channel}
	set := m.subs[channel]
	if set == nil {
		set = make(map[*memSub]struct{})
		m.subs[channel] = set
	}
	set[sub] = struct{}{}
	return sub, nil
}

func (m *Memory) removeSub(s *memSub) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if set := m.subs[s.channel]; set != nil {
		delete(set, s)
		if len(set) == 0 {
			delete(m.subs, s.channel)
		}
	}
}

func (m *Memory) Ping(_ context.Context) error { return nil }

func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	for _, set := range m.subs {
		for s := range set {
			s.closeChan()
		}
	}
	m.subs = make(map[string]map[*memSub]struct{})
	return nil
}

type memSub struct {
	ch      chan string
	backend *Memory
	channel string
	once    sync.Once
}

func (s *memSub) Messages() <-chan string { return s.ch }

func (s *memSub) Close() error {
	s.backend.removeSub(s)
	s.closeChan()
	return nil
}

func (s *memSub) closeChan() { s.once.Do(func() { close(s.ch) }) }

var _ Backend = (*Memory)(nil)
