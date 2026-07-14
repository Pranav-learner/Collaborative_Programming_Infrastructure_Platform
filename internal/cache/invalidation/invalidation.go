// Package invalidation implements every way a cached value can be removed:
// manual (single key), pattern (glob SCAN), tag (secondary-index sets), bulk
// (many keys at once), and event-driven cross-node broadcast. TTL-based
// invalidation is Redis' own job and needs no code here beyond the TTL manager.
//
// Because CPIP runs many nodes against a shared Redis, a delete is globally
// visible immediately. The pub/sub broadcast exists so that FUTURE in-process
// (L1) caches on every node can evict their local copy — the architecture is in
// place today via OnInvalidate hooks.
package invalidation

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"cpip/internal/cache/events"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/types"
)

// Mode classifies an invalidation for observability.
type Mode string

const (
	ModeManual  Mode = "manual"
	ModePattern Mode = "pattern"
	ModeTag     Mode = "tag"
	ModeBulk    Mode = "bulk"
	ModeCache   Mode = "cache"
	ModeRemote  Mode = "remote"
)

// broadcast is the wire form of a cross-node invalidation notification.
type broadcast struct {
	Mode   Mode     `json:"mode"`
	NodeID string   `json:"node_id"`
	Keys   []string `json:"keys,omitempty"`
	Tag    string   `json:"tag,omitempty"`
}

// LocalHook is invoked with the full keys evicted by an invalidation (local or
// remote). Future L1 caches register a hook to drop their in-process copies.
type LocalHook func(fullKeys []string)

// Manager performs invalidations against Redis and coordinates cross-node
// eviction over a dedicated pub/sub channel.
type Manager struct {
	client  redis.Client
	kb      keys.Builder
	channel string
	nodeID  string
	bus     *events.Bus
	rec     metrics.Recorder

	mu    sync.RWMutex
	hooks []LocalHook

	sub    redis.Subscription
	stopCh chan struct{}
	doneCh chan struct{}
	once   sync.Once
}

// Params configures a Manager.
type Params struct {
	Client  redis.Client
	Keys    keys.Builder
	Channel string // pub/sub channel for broadcasts
	NodeID  string
	Bus     *events.Bus
	Metrics metrics.Recorder
}

// New constructs an invalidation Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	ch := p.Channel
	if ch == "" {
		ch = p.Keys.Channel(p.Keys.Prefix(), "invalidation")
	}
	return &Manager{
		client:  p.Client,
		kb:      p.Keys,
		channel: ch,
		nodeID:  p.NodeID,
		bus:     p.Bus,
		rec:     rec,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// OnInvalidate registers a hook fired with the full keys of every eviction.
func (m *Manager) OnInvalidate(h LocalHook) {
	m.mu.Lock()
	m.hooks = append(m.hooks, h)
	m.mu.Unlock()
}

func (m *Manager) fireHooks(fullKeys []string) {
	m.mu.RLock()
	hooks := append([]LocalHook(nil), m.hooks...)
	m.mu.RUnlock()
	for _, h := range hooks {
		h(fullKeys)
	}
}

// IndexTags records fullKey as a member of each tag's secondary-index set so it
// can later be invalidated by tag. Called by the Cache Manager at Set time.
func (m *Manager) IndexTags(ctx context.Context, fullKey string, tags []string) error {
	for _, t := range tags {
		if _, err := m.client.SAdd(ctx, m.kb.Tag(t), fullKey); err != nil {
			return fmt.Errorf("index tag %q: %w", t, err)
		}
	}
	return nil
}

// InvalidateKey removes a single full key.
func (m *Manager) InvalidateKey(ctx context.Context, fullKey string) error {
	return m.deleteAndBroadcast(ctx, ModeManual, []string{fullKey}, "", true)
}

// InvalidateBulk removes many full keys in one round trip.
func (m *Manager) InvalidateBulk(ctx context.Context, fullKeys []string) error {
	if len(fullKeys) == 0 {
		return nil
	}
	return m.deleteAndBroadcast(ctx, ModeBulk, fullKeys, "", true)
}

// InvalidatePattern removes every key matching a glob pattern (SCAN + DEL).
func (m *Manager) InvalidatePattern(ctx context.Context, pattern string) (int, error) {
	matched, err := m.client.ScanKeys(ctx, pattern, 512)
	if err != nil {
		return 0, err
	}
	if len(matched) == 0 {
		return 0, nil
	}
	if err := m.deleteAndBroadcast(ctx, ModePattern, matched, "", true); err != nil {
		return 0, err
	}
	return len(matched), nil
}

// InvalidateTag removes every key indexed under a tag, then drops the tag set.
func (m *Manager) InvalidateTag(ctx context.Context, tag string) (int, error) {
	tagKey := m.kb.Tag(tag)
	members, err := m.client.SMembers(ctx, tagKey)
	if err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}
	if err := m.deleteAndBroadcast(ctx, ModeTag, members, tag, true); err != nil {
		return 0, err
	}
	if _, err := m.client.Del(ctx, tagKey); err != nil {
		return 0, err
	}
	return len(members), nil
}

// InvalidateCacheName removes all keys of a logical cache by its glob pattern.
func (m *Manager) InvalidateCacheName(ctx context.Context, cacheName string) (int, error) {
	return m.InvalidatePattern(ctx, m.kb.CachePattern(cacheName))
}

// deleteAndBroadcast performs the DEL, fires local hooks, emits an event, and —
// when broadcast is true — notifies other nodes.
func (m *Manager) deleteAndBroadcast(ctx context.Context, mode Mode, fullKeys []string, tag string, broadcastOut bool) error {
	if _, err := m.client.Del(ctx, fullKeys...); err != nil {
		m.rec.IncCounter(metrics.MetricCacheError, map[string]string{"op": "invalidate"})
		return err
	}
	m.rec.AddCounter(metrics.MetricCacheInvalidation, float64(len(fullKeys)), map[string]string{"mode": string(mode)})
	m.fireHooks(fullKeys)
	if m.bus != nil {
		m.bus.Emit(events.CacheInvalidated, "invalidation", func(e *events.Event) {
			e.Payload = map[string]any{"mode": mode, "count": len(fullKeys), "tag": tag}
		})
	}
	if broadcastOut {
		m.broadcast(ctx, broadcast{Mode: mode, NodeID: m.nodeID, Keys: fullKeys, Tag: tag})
	}
	return nil
}

func (m *Manager) broadcast(ctx context.Context, b broadcast) {
	data, err := json.Marshal(b)
	if err != nil {
		return
	}
	_, _ = m.client.Publish(ctx, m.channel, string(data))
}

// Start subscribes to the invalidation channel to apply remote evictions to
// local hooks. Idempotent.
func (m *Manager) Start(ctx context.Context) error {
	var startErr error
	m.once.Do(func() {
		sub, err := m.client.Subscribe(ctx, m.channel)
		if err != nil {
			startErr = err
			return
		}
		m.sub = sub
		go m.consume()
	})
	return startErr
}

func (m *Manager) consume() {
	defer close(m.doneCh)
	ch := m.sub.Channel()
	for {
		select {
		case <-m.stopCh:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var b broadcast
			if err := json.Unmarshal([]byte(msg.Payload), &b); err != nil {
				continue
			}
			if b.NodeID == m.nodeID {
				continue // ignore our own broadcasts
			}
			// The keys are already gone from shared Redis; fire local hooks so
			// any in-process caches on this node evict too.
			m.rec.AddCounter(metrics.MetricCacheInvalidation, float64(len(b.Keys)), map[string]string{"mode": string(ModeRemote)})
			m.fireHooks(b.Keys)
		}
	}
}

// Stop halts the remote-invalidation subscriber. Idempotent.
func (m *Manager) Stop() {
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
	if m.sub != nil {
		_ = m.sub.Close()
		select {
		case <-m.doneCh:
		case <-time.After(2 * time.Second):
		}
	}
}

var _ = types.ErrClosed
