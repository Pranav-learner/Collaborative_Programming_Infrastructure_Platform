// Package replication implements the State Replication Framework: it fans local
// state changes out to every node and applies remote changes locally, over the
// coordination backend's pub/sub. It carries the ephemeral, cluster-shared state
// that is NOT the durable system-of-record — presence, room metadata, execution
// status, worker metadata, cluster metadata, node metadata — so every node
// converges on the same view.
//
// Replication is domain-partitioned: each domain has its own channel, handler
// set, and (future) merge strategy. Today the conflict rule is last-write-wins by
// version/timestamp; the Merger interface is the seam a future CRDT/state-sync
// engine plugs into WITHOUT changing publishers or subscribers — the module's
// explicit forward-compatibility requirement.
package replication

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/keys"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/metrics"
	"cpip/internal/coordination/types"
)

// Domain names a replicated state partition.
type Domain string

const (
	DomainPresence  Domain = "presence"
	DomainRoom      Domain = "room"
	DomainExecution Domain = "execution"
	DomainWorker    Domain = "worker"
	DomainCluster   Domain = "cluster"
	DomainNode      Domain = "node"
)

// Update is one replicated state change.
type Update struct {
	Domain    Domain          `json:"domain"`
	Key       string          `json:"key"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Origin    string          `json:"origin"`
	Version   uint64          `json:"version"`
	Deleted   bool            `json:"deleted,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

// Handler is invoked for each remote update in a domain.
type Handler func(Update)

// Merger is the pluggable conflict-resolution seam. Given the current locally
// known update and an incoming one for the same key, it returns the winning
// update and whether it changed local state. The default merger is last-write-
// wins; a future CRDT engine implements richer convergence here without any
// change to callers. This is the module's CRDT/state-sync forward hook.
type Merger interface {
	Merge(domain Domain, current, incoming Update) (winner Update, changed bool)
}

// StateProvider supplies a domain's full local state for anti-entropy re-broadcast.
type StateProvider func() []Update

// LWWMerger resolves conflicts by highest version, then latest timestamp.
type LWWMerger struct{}

// Merge implements Merger with last-write-wins semantics.
func (LWWMerger) Merge(_ Domain, current, incoming Update) (Update, bool) {
	if incoming.Version != current.Version {
		if incoming.Version > current.Version {
			return incoming, true
		}
		return current, false
	}
	if incoming.Timestamp.After(current.Timestamp) {
		return incoming, true
	}
	return current, false
}

// Replicator is the State Replication Framework.
type Replicator struct {
	backend backend.Backend
	kb      keys.Builder
	cfg     config.Replication
	nodeID  string
	merger  Merger
	bus     *events.Bus
	rec     metrics.Recorder
	log     *logger.Logger
	now     func() time.Time

	mu        sync.Mutex
	domains   map[Domain]*domainState
	cancel    context.CancelFunc
	baseCtx   context.Context
	providers map[Domain]StateProvider
	syncDone  chan struct{}
	closed    bool
}

type domainState struct {
	handlers []Handler
	sub      backend.Subscription
	done     chan struct{}
}

// Params configures a Replicator.
type Params struct {
	Backend backend.Backend
	Keys    keys.Builder
	Config  config.Replication
	NodeID  string
	Merger  Merger
	Events  *events.Bus
	Metrics metrics.Recorder
	Logger  *logger.Logger
}

// New constructs a Replicator.
func New(p Params) *Replicator {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	merger := p.Merger
	if merger == nil {
		merger = LWWMerger{}
	}
	return &Replicator{
		backend:   p.Backend,
		kb:        p.Keys,
		cfg:       p.Config,
		nodeID:    p.NodeID,
		merger:    merger,
		bus:       p.Events,
		rec:       rec,
		log:       p.Logger.With("subsystem", "replication"),
		now:       time.Now,
		domains:   make(map[Domain]*domainState),
		providers: make(map[Domain]StateProvider),
	}
}

// Start records the base context used for subscription receive loops and launches
// the anti-entropy loop. Call once before registering domains.
func (r *Replicator) Start(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.baseCtx != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	r.baseCtx = loopCtx
	r.cancel = cancel
	r.syncDone = make(chan struct{})
	go r.antiEntropyLoop(loopCtx)
}

// Register subscribes to a domain's channel and starts applying remote updates.
// Idempotent per domain. Registering also happens implicitly on OnUpdate.
func (r *Replicator) Register(ctx context.Context, domain Domain) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.registerLocked(ctx, domain)
}

func (r *Replicator) registerLocked(ctx context.Context, domain Domain) error {
	if r.closed {
		return types.ErrClosed
	}
	if _, ok := r.domains[domain]; ok {
		return nil
	}
	sub, err := r.backend.Subscribe(ctx, r.kb.ReplicationChannel(string(domain)))
	if err != nil {
		return err
	}
	ds := &domainState{sub: sub, done: make(chan struct{})}
	r.domains[domain] = ds
	go r.receiveLoop(domain, ds)
	return nil
}

// OnUpdate registers a handler for a domain (registering the domain if needed).
func (r *Replicator) OnUpdate(ctx context.Context, domain Domain, h Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.registerLocked(ctx, domain); err != nil {
		return err
	}
	r.domains[domain].handlers = append(r.domains[domain].handlers, h)
	return nil
}

// RegisterProvider registers an anti-entropy state provider for a domain, whose
// updates are periodically re-broadcast so a node that missed a live message
// eventually converges.
func (r *Replicator) RegisterProvider(domain Domain, p StateProvider) {
	r.mu.Lock()
	r.providers[domain] = p
	r.mu.Unlock()
}

// Broadcast publishes a state change to a domain. Origin and Timestamp are
// stamped by the replicator; the caller supplies Domain, Key, Payload, Version.
func (r *Replicator) Broadcast(ctx context.Context, u Update) error {
	u.Origin = r.nodeID
	if u.Timestamp.IsZero() {
		u.Timestamp = r.now().UTC()
	}
	data, err := json.Marshal(u)
	if err != nil {
		r.rec.IncCounter(metrics.MetricReplicationFailed, nil)
		return types.ErrReplicationFailed
	}
	if err := r.backend.Publish(ctx, r.kb.ReplicationChannel(string(u.Domain)), string(data)); err != nil {
		r.rec.IncCounter(metrics.MetricReplicationFailed, nil)
		return err
	}
	r.rec.IncCounter(metrics.MetricReplicationPublished, map[string]string{"domain": string(u.Domain)})
	return nil
}

func (r *Replicator) receiveLoop(domain Domain, ds *domainState) {
	defer close(ds.done)
	for payload := range ds.sub.Messages() {
		var u Update
		if err := json.Unmarshal([]byte(payload), &u); err != nil {
			r.rec.IncCounter(metrics.MetricReplicationDropped, nil)
			continue
		}
		// Dedup: ignore our own echoes.
		if u.Origin == r.nodeID {
			continue
		}
		r.mu.Lock()
		handlers := append([]Handler(nil), ds.handlers...)
		r.mu.Unlock()
		for _, h := range handlers {
			h(u)
		}
		r.rec.IncCounter(metrics.MetricReplicationApplied, map[string]string{"domain": string(domain)})
		r.log.Replication(context.Background(), "applied", string(domain), true, nil)
		r.bus.Emit(events.StateReplicated, "replication", func(e *events.Event) {
			e.Origin = u.Origin
			e.Payload = map[string]any{"domain": string(domain), "key": u.Key}
		})
	}
}

func (r *Replicator) antiEntropyLoop(ctx context.Context) {
	defer close(r.syncDone)
	ticker := time.NewTicker(r.cfg.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.mu.Lock()
			providers := make(map[Domain]StateProvider, len(r.providers))
			for d, p := range r.providers {
				providers[d] = p
			}
			r.mu.Unlock()
			for _, p := range providers {
				for _, u := range p() {
					_ = r.Broadcast(ctx, u)
				}
			}
		}
	}
}

// Domains returns the set of currently-registered domains.
func (r *Replicator) Domains() []Domain {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Domain, 0, len(r.domains))
	for d := range r.domains {
		out = append(out, d)
	}
	return out
}

// Close stops all receive loops and the anti-entropy loop.
func (r *Replicator) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	cancel := r.cancel
	domains := r.domains
	r.domains = make(map[Domain]*domainState)
	syncDone := r.syncDone
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, ds := range domains {
		_ = ds.sub.Close()
		<-ds.done
	}
	if syncDone != nil {
		<-syncDone
	}
	return nil
}
