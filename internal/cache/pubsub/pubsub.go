// Package pubsub is the module's Redis Pub/Sub manager. It layers logical topic
// registration, in-process fan-out, backpressure protection, and transparent
// reconnect on top of the raw redis.Client subscription primitive.
//
// One background router goroutine per active topic owns a single underlying
// Redis subscription and fans messages out to all local subscribers through
// bounded channels. When Redis drops the connection, the router reconnects with
// exponential backoff without the subscribers noticing. Slow subscribers are
// isolated: per the configured policy they either lose messages (drop) or apply
// backpressure to the router — never to Redis or to other subscribers.
//
// Redis Streams integration (durable, replayable topics) is a future extension:
// RegisterTopic already carries a Durable flag reserved for that path.
package pubsub

import (
	"context"
	"sync"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/events"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/logger"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/types"
)

// Message is a delivered pub/sub message.
type Message struct {
	Topic      string
	Channel    string
	Pattern    string
	Payload    string
	ReceivedAt time.Time
}

// TopicSpec declares a logical topic.
type TopicSpec struct {
	// Name is the logical topic identifier.
	Name string
	// Pattern, when true, treats Name as a glob and uses PSUBSCRIBE so a single
	// subscription receives many channels (e.g. "room:*").
	Pattern bool
	// Durable is reserved for a future Redis Streams-backed topic (replayable,
	// consumer groups). Ignored today.
	Durable bool
}

// Manager is the pub/sub hub.
type Manager struct {
	client redis.Client
	cfg    config.PubSub
	kb     keys.Builder
	bus    *events.Bus
	rec    metrics.Recorder
	log    *logger.Logger

	mu     sync.Mutex
	topics map[string]*topic
	nextID uint64
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
}

// Params configures a Manager.
type Params struct {
	Client  redis.Client
	Config  config.PubSub
	Keys    keys.Builder
	Bus     *events.Bus
	Metrics metrics.Recorder
	Logger  *logger.Logger
}

// New constructs a pub/sub Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	log := p.Logger
	if log == nil {
		log = logger.New(nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		client: p.Client,
		cfg:    p.Config,
		kb:     p.Keys,
		bus:    p.Bus,
		rec:    rec,
		log:    log,
		topics: make(map[string]*topic),
		ctx:    ctx,
		cancel: cancel,
	}
}

// RegisterTopic declares a topic. Registration is required before Publish or
// Subscribe so channel naming and pattern-vs-exact semantics are explicit.
func (m *Manager) RegisterTopic(spec TopicSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.topics[spec.Name]; ok {
		return
	}
	m.topics[spec.Name] = &topic{
		spec:    spec,
		channel: m.kb.Channel(m.kb.Prefix()+":topic", spec.Name),
		subs:    make(map[uint64]*subscription),
	}
}

func (m *Manager) topicFor(name string) (*topic, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.topics[name]
	return t, ok
}

// Publish sends payload to a topic. Returns the number of Redis subscribers that
// received it (across all nodes).
func (m *Manager) Publish(ctx context.Context, topicName, payload string) (int64, error) {
	t, ok := m.topicFor(topicName)
	if !ok {
		return 0, types.ErrTopicNotRegistered
	}
	n, err := m.client.Publish(ctx, t.channel, payload)
	if err != nil {
		m.rec.IncCounter(metrics.MetricRedisError, map[string]string{"op": "publish"})
		m.log.PubSub(ctx, "publish", t.channel, nil, err)
		return 0, err
	}
	m.rec.IncCounter(metrics.MetricPubSubPublished, map[string]string{"topic": topicName})
	if m.bus != nil {
		m.bus.Emit(events.MessagePublished, "pubsub", func(e *events.Event) {
			e.Key = topicName
			e.Payload = map[string]any{"subscribers": n}
		})
	}
	return n, nil
}

// Subscribe returns a Subscription streaming messages for a topic. The first
// subscriber for a topic starts its router; the last to Close stops it.
func (m *Manager) Subscribe(topicName string) (*Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, types.ErrPubSubClosed
	}
	t, ok := m.topics[topicName]
	if !ok {
		return nil, types.ErrTopicNotRegistered
	}
	m.nextID++
	id := m.nextID
	s := &subscription{
		id:    id,
		topic: topicName,
		mgr:   m,
		out:   make(chan Message, m.cfg.SubscriberBuffer),
	}
	t.mu.Lock()
	t.subs[id] = s
	needStart := !t.running
	if needStart {
		t.running = true
	}
	t.mu.Unlock()

	if needStart {
		go m.route(t)
	}
	m.rec.SetGauge(metrics.MetricPubSubSubscribers, float64(m.subscriberCount()), map[string]string{})
	return &Subscription{inner: s}, nil
}

func (m *Manager) subscriberCount() int {
	n := 0
	for _, t := range m.topics {
		t.mu.Lock()
		n += len(t.subs)
		t.mu.Unlock()
	}
	return n
}

// route owns a topic's underlying Redis subscription, reconnecting on failure,
// and fans messages out to local subscribers.
func (m *Manager) route(t *topic) {
	backoff := m.cfg.ReconnectInterval
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}
		// Stop if no subscribers remain.
		t.mu.Lock()
		if len(t.subs) == 0 {
			t.running = false
			t.mu.Unlock()
			return
		}
		t.mu.Unlock()

		var (
			sub redis.Subscription
			err error
		)
		if t.spec.Pattern {
			sub, err = m.client.PSubscribe(m.ctx, t.channel)
		} else {
			sub, err = m.client.Subscribe(m.ctx, t.channel)
		}
		if err != nil {
			m.log.PubSub(m.ctx, "subscribe_failed", t.channel, nil, err)
			if !m.sleepBackoff(&backoff) {
				return
			}
			continue
		}
		backoff = m.cfg.ReconnectInterval // reset on successful connect
		reconnect := m.pump(t, sub)
		_ = sub.Close()
		if !reconnect {
			return
		}
		m.rec.IncCounter(metrics.MetricPubSubReconnect, map[string]string{"topic": t.spec.Name})
		m.log.PubSub(m.ctx, "reconnect", t.channel, nil, nil)
		if !m.sleepBackoff(&backoff) {
			return
		}
	}
}

// pump forwards from the underlying subscription to local subscribers. It
// returns true if the loop ended because the connection dropped (reconnect
// wanted), false if it ended because the manager is shutting down or the topic
// has no subscribers left.
func (m *Manager) pump(t *topic, sub redis.Subscription) bool {
	in := sub.Channel()
	for {
		select {
		case <-m.ctx.Done():
			return false
		case msg, ok := <-in:
			if !ok {
				return true // connection dropped → reconnect
			}
			m.fanout(t, msg)
		}
	}
}

func (m *Manager) fanout(t *topic, msg redis.Message) {
	out := Message{
		Topic:      t.spec.Name,
		Channel:    msg.Channel,
		Pattern:    msg.Pattern,
		Payload:    msg.Payload,
		ReceivedAt: time.Now(),
	}
	t.mu.Lock()
	subs := make([]*subscription, 0, len(t.subs))
	for _, s := range t.subs {
		subs = append(subs, s)
	}
	t.mu.Unlock()

	for _, s := range subs {
		delivered := s.send(out, m.cfg.DropOnBackpressure, m.ctx.Done())
		if delivered {
			m.rec.IncCounter(metrics.MetricPubSubReceived, map[string]string{"topic": t.spec.Name})
		} else {
			m.rec.IncCounter(metrics.MetricPubSubDropped, map[string]string{"topic": t.spec.Name})
		}
	}
	if m.bus != nil {
		m.bus.Emit(events.MessageReceived, "pubsub", func(e *events.Event) { e.Key = t.spec.Name })
	}
}

// sleepBackoff sleeps for the current backoff and doubles it up to the cap.
// Returns false if the manager is shutting down.
func (m *Manager) sleepBackoff(backoff *time.Duration) bool {
	d := *backoff
	if d <= 0 {
		d = 500 * time.Millisecond
	}
	next := d * 2
	if next > m.cfg.MaxReconnectBackoff {
		next = m.cfg.MaxReconnectBackoff
	}
	*backoff = next
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-m.ctx.Done():
		return false
	}
}

// unsubscribe removes a subscriber from its topic. Called by Subscription.Close.
func (m *Manager) unsubscribe(s *subscription) {
	t, ok := m.topicFor(s.topic)
	if !ok {
		return
	}
	t.mu.Lock()
	if _, ok := t.subs[s.id]; ok {
		delete(t.subs, s.id)
		s.markClosed()
	}
	t.mu.Unlock()
	m.mu.Lock()
	m.rec.SetGauge(metrics.MetricPubSubSubscribers, float64(m.subscriberCount()), map[string]string{})
	m.mu.Unlock()
}

// Close shuts down all routers and subscriptions. Idempotent.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	topics := make([]*topic, 0, len(m.topics))
	for _, t := range m.topics {
		topics = append(topics, t)
	}
	m.mu.Unlock()

	m.cancel()
	for _, t := range topics {
		t.mu.Lock()
		for _, s := range t.subs {
			s.markClosed()
		}
		t.subs = make(map[uint64]*subscription)
		t.mu.Unlock()
	}
	return nil
}

// topic is the internal state for one logical topic.
type topic struct {
	spec    TopicSpec
	channel string

	mu      sync.Mutex
	subs    map[uint64]*subscription
	running bool
}

// subscription is the internal per-subscriber state. A per-subscription mutex
// serializes send against close so the router can never send on a closed
// channel. The lock is held only for the duration of one subscriber's delivery,
// so a slow subscriber isolates itself without stalling the topic or its peers.
type subscription struct {
	id    uint64
	topic string
	mgr   *Manager
	out   chan Message

	mu     sync.Mutex
	closed bool
}

func (s *subscription) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// send delivers msg respecting the backpressure policy. Returns true if the
// message was enqueued. Safe against concurrent close.
func (s *subscription) send(msg Message, drop bool, done <-chan struct{}) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if drop {
		select {
		case s.out <- msg:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case s.out <- msg:
		return true
	case <-timer.C:
		return false
	case <-done:
		return false
	}
}

func (s *subscription) markClosed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.out)
}

// Subscription is the public handle returned to callers. It wraps the internal
// subscription so Close can be made idempotent and safe.
type Subscription struct {
	inner *subscription
}

// Messages returns the receive-only stream of messages for this subscription.
func (s *Subscription) Messages() <-chan Message { return s.inner.out }

// Topic returns the logical topic name.
func (s *Subscription) Topic() string { return s.inner.topic }

// Close unsubscribes and releases resources. Idempotent.
func (s *Subscription) Close() {
	s.inner.mgr.unsubscribe(s.inner)
}
