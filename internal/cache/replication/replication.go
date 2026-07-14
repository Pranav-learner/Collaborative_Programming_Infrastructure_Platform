// Package replication is the transport for eventually-consistent shared state.
// A node BROADCASTS a versioned update on a per-namespace Redis pub/sub channel;
// every other node APPLIES it to its local materialized view after a
// last-writer-wins (LWW) check that discards stale or out-of-order deltas.
//
// This is the substrate beneath presence replication and the distributed state
// manager. It deliberately provides no coordination or consensus — updates are
// commutative under LWW, converging without locks. A periodic anti-entropy
// resync (driven by the owner of the authoritative state) heals any deltas lost
// to a transient pub/sub disconnect.
package replication

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"cpip/internal/cache/events"
	"cpip/internal/cache/logger"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/redis"
)

// Update is a single replicated state change.
type Update struct {
	Namespace string            `json:"ns"`
	ID        string            `json:"id"`
	Fields    map[string]string `json:"fields,omitempty"`
	Payload   string            `json:"payload,omitempty"`
	// Version is the LWW clock. Higher wins. Callers use wall-clock UnixNano,
	// which is monotonic enough for a single writer per (namespace,id).
	Version   int64  `json:"ver"`
	NodeID    string `json:"node"`
	Deleted   bool   `json:"del,omitempty"`
	EmittedAt int64  `json:"ts"` // UnixMilli, for lag metrics
}

// ApplyFunc is invoked with an update that survived the LWW check.
type ApplyFunc func(u Update)

// Replicator broadcasts and applies updates for any number of namespaces.
type Replicator struct {
	client        redis.Client
	channelPrefix string
	nodeID        string
	bus           *events.Bus
	rec           metrics.Recorder
	log           *logger.Logger

	mu       sync.Mutex
	appliers map[string][]ApplyFunc // namespace -> handlers
	subs     map[string]redis.Subscription
	// versions dedupes/LWW-orders updates per (namespace|id).
	versions map[string]int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

// Params configures a Replicator.
type Params struct {
	Client        redis.Client
	ChannelPrefix string
	NodeID        string
	Bus           *events.Bus
	Metrics       metrics.Recorder
	Logger        *logger.Logger
}

// New constructs a Replicator.
func New(p Params) *Replicator {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	log := p.Logger
	if log == nil {
		log = logger.New(nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Replicator{
		client:        p.Client,
		channelPrefix: p.ChannelPrefix,
		nodeID:        p.NodeID,
		bus:           p.Bus,
		rec:           rec,
		log:           log,
		appliers:      make(map[string][]ApplyFunc),
		subs:          make(map[string]redis.Subscription),
		versions:      make(map[string]int64),
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (r *Replicator) channel(namespace string) string {
	return r.channelPrefix + ":" + namespace
}

// NextVersion returns a monotonically increasing LWW clock value. Callers stamp
// their updates with it so later writes win.
func (r *Replicator) NextVersion() int64 { return time.Now().UnixNano() }

// Broadcast publishes an update to its namespace channel. It also records the
// local version so the node ignores the echo of its own update.
func (r *Replicator) Broadcast(ctx context.Context, u Update) error {
	if u.NodeID == "" {
		u.NodeID = r.nodeID
	}
	if u.Version == 0 {
		u.Version = r.NextVersion()
	}
	u.EmittedAt = time.Now().UnixMilli()

	r.recordVersion(u.Namespace, u.ID, u.Version)

	data, err := json.Marshal(u)
	if err != nil {
		return err
	}
	if _, err := r.client.Publish(ctx, r.channel(u.Namespace), string(data)); err != nil {
		r.rec.IncCounter(metrics.MetricRedisError, map[string]string{"op": "replicate"})
		return err
	}
	r.rec.IncCounter(metrics.MetricPresenceReplicated, map[string]string{"ns": u.Namespace})
	if r.bus != nil {
		r.bus.Emit(events.PresenceReplicated, "replication", func(e *events.Event) {
			e.Key = u.Namespace + ":" + u.ID
		})
	}
	return nil
}

// Subscribe registers an applier for a namespace and, on the first applier for
// that namespace, starts the listener goroutine.
func (r *Replicator) Subscribe(namespace string, apply ApplyFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.appliers[namespace] = append(r.appliers[namespace], apply)
	if _, ok := r.subs[namespace]; ok {
		return nil // listener already running
	}
	sub, err := r.client.Subscribe(r.ctx, r.channel(namespace))
	if err != nil {
		return err
	}
	r.subs[namespace] = sub
	r.wg.Add(1)
	go r.listen(namespace, sub)
	return nil
}

func (r *Replicator) listen(namespace string, sub redis.Subscription) {
	defer r.wg.Done()
	ch := sub.Channel()
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var u Update
			if err := json.Unmarshal([]byte(msg.Payload), &u); err != nil {
				continue
			}
			r.apply(namespace, u)
		}
	}
}

// apply runs an incoming update through the LWW gate before dispatching to
// handlers. Updates from this node, or with a version <= the last seen version
// for the same (namespace,id), are dropped.
func (r *Replicator) apply(namespace string, u Update) {
	if u.NodeID == r.nodeID {
		return // ignore our own echo
	}
	if !r.acceptVersion(u.Namespace, u.ID, u.Version) {
		r.rec.IncCounter(metrics.MetricPresenceStale, map[string]string{"ns": u.Namespace})
		return
	}
	if u.EmittedAt > 0 {
		lag := time.Now().UnixMilli() - u.EmittedAt
		if lag >= 0 {
			r.rec.ObserveHistogram(metrics.MetricReplicationLagMs, float64(lag), map[string]string{"ns": u.Namespace})
		}
	}
	r.mu.Lock()
	handlers := append([]ApplyFunc(nil), r.appliers[namespace]...)
	r.mu.Unlock()

	for _, h := range handlers {
		h(u)
	}
	r.rec.IncCounter(metrics.MetricPresenceApplied, map[string]string{"ns": u.Namespace})
	if r.bus != nil {
		r.bus.Emit(events.PresenceApplied, "replication", func(e *events.Event) {
			e.Key = u.Namespace + ":" + u.ID
		})
	}
}

func versionKey(namespace, id string) string { return namespace + "\x00" + id }

func (r *Replicator) recordVersion(namespace, id string, v int64) {
	r.mu.Lock()
	if v > r.versions[versionKey(namespace, id)] {
		r.versions[versionKey(namespace, id)] = v
	}
	r.mu.Unlock()
}

// acceptVersion returns true and records v if it is newer than the last seen
// version for (namespace,id). LWW: equal or older versions are rejected.
func (r *Replicator) acceptVersion(namespace, id string, v int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := versionKey(namespace, id)
	if v <= r.versions[k] {
		return false
	}
	r.versions[k] = v
	return true
}

// Close stops all listeners. Idempotent.
func (r *Replicator) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	subs := r.subs
	r.subs = make(map[string]redis.Subscription)
	r.mu.Unlock()

	r.cancel()
	for _, s := range subs {
		_ = s.Close()
	}
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	return nil
}
