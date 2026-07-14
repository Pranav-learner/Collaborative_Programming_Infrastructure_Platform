// Package state provides the Distributed State Manager: the single abstraction
// through which every CPIP node manages ephemeral shared state. It is the
// composition root of the module — it constructs and owns the Redis adapter,
// cache manager, session store, presence replicator, lock manager, pub/sub hub,
// and replication engine, and exposes them behind one cohesive facade.
//
// Business services depend on THIS package (or the interfaces it hands out),
// never on Redis. The design realizes the module's layering:
//
//	Business Services → Cache/State facade → Distributed State Manager → Redis Adapter → Redis
//
// Beyond wiring, it offers a generic namespaced state API (put/get/CAS/delete/
// watch) used for room membership, execution status, worker state, and
// temporary metadata — anything that is shared and ephemeral but not part of the
// durable system of record (PostgreSQL).
package state

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/events"
	"cpip/internal/cache/invalidation"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/locks"
	"cpip/internal/cache/logger"
	cachemgr "cpip/internal/cache/manager"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/presence"
	"cpip/internal/cache/pubsub"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/replication"
	"cpip/internal/cache/sessions"
	"cpip/internal/cache/types"
)

// Namespaces for generic distributed state. Each is an isolated keyspace with
// its own replication channel.
const (
	NamespaceRoomMembership  = "membership"
	NamespaceExecutionStatus = "exec_status"
	NamespaceWorkerState     = "worker"
	NamespaceSessionState    = "session_state"
	NamespaceMetadata        = "metadata"
	NamespaceClusterState    = "cluster" // reserved for a future coordination layer
)

// Manager is the Distributed State Manager.
type Manager struct {
	cfg        config.Config
	client     redis.Client
	ownsClient bool
	kb         keys.Builder
	codec      types.Codec
	nodeID     string

	cache    *cachemgr.Manager
	sessions *sessions.Store
	presence *presence.Manager
	locks    *locks.Manager
	pubsub   *pubsub.Manager
	repl     *replication.Replicator
	inval    *invalidation.Manager

	bus *events.Bus
	rec metrics.Recorder
	log *logger.Logger
}

// Params configures a Manager. Only Config is required. If Client is nil a
// production go-redis client is constructed from Config.Redis.
type Params struct {
	Config  config.Config
	Client  redis.Client // optional; nil → build from Config.Redis
	Codec   types.Codec
	Bus     *events.Bus
	Metrics metrics.Recorder
	Logger  *slog.Logger
}

// New constructs and wires the entire module. The returned Manager is ready to
// use after Start is called.
func New(p Params) (*Manager, error) {
	cfg, err := p.Config.Validate()
	if err != nil {
		return nil, err
	}
	nodeID := cfg.Replication.NodeID
	if nodeID == "" {
		nodeID = randomNodeID()
		cfg.Replication.NodeID = nodeID
	}

	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	bus := p.Bus
	if bus == nil {
		bus = events.NewBus()
	}
	log := logger.New(p.Logger).With("node_id", nodeID)
	codec := p.Codec
	if codec == nil {
		codec = types.JSONCodec{}
	}
	kb := keys.New(cfg.Redis.KeyPrefix)

	client := p.Client
	ownsClient := false
	if client == nil {
		rc, err := redis.NewRedis(cfg.Redis)
		if err != nil {
			return nil, fmt.Errorf("connect redis: %w", err)
		}
		client = rc
		ownsClient = true
	}

	// Shared subsystems.
	inval := invalidation.New(invalidation.Params{
		Client: client, Keys: kb, NodeID: nodeID, Bus: bus, Metrics: rec,
	})
	repl := replication.New(replication.Params{
		Client: client, ChannelPrefix: cfg.Replication.ChannelPrefix, NodeID: nodeID,
		Bus: bus, Metrics: rec, Logger: log,
	})

	cache, err := cachemgr.New(cachemgr.Params{
		Config: cfg, Client: client, Codec: codec, Bus: bus, Metrics: rec, Logger: log,
		Invalidation: inval,
	})
	if err != nil {
		return nil, err
	}

	sess := sessions.New(sessions.Params{
		Client: client, Keys: kb, Config: cfg.TTL, Bus: bus, Metrics: rec, Logger: log,
	})
	pres := presence.New(presence.Params{
		Client: client, Replicator: repl, Keys: kb, Config: cfg.Replication,
		PresenceTTL: cfg.TTL.Presence, NodeID: nodeID, Metrics: rec, Logger: log,
	})
	lockMgr := locks.New(locks.Params{
		Client: client, Config: cfg.Lock, Keys: kb, NodeID: nodeID, Bus: bus, Metrics: rec, Logger: log,
	})
	ps := pubsub.New(pubsub.Params{
		Client: client, Config: cfg.PubSub, Keys: kb, Bus: bus, Metrics: rec, Logger: log,
	})

	return &Manager{
		cfg: cfg, client: client, ownsClient: ownsClient, kb: kb, codec: codec, nodeID: nodeID,
		cache: cache, sessions: sess, presence: pres, locks: lockMgr, pubsub: ps,
		repl: repl, inval: inval, bus: bus, rec: rec, log: log,
	}, nil
}

func randomNodeID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "node_" + hex.EncodeToString(b)
}

// --- Subsystem accessors (the public seams for business services) ---

// Cache returns the cache facade.
func (m *Manager) Cache() cachemgr.Cache { return m.cache }

// CacheManager returns the concrete cache manager (for cache registration).
func (m *Manager) CacheManager() *cachemgr.Manager { return m.cache }

// Sessions returns the distributed session store.
func (m *Manager) Sessions() *sessions.Store { return m.sessions }

// Presence returns the presence replicator.
func (m *Manager) Presence() *presence.Manager { return m.presence }

// Locks returns the distributed lock manager.
func (m *Manager) Locks() *locks.Manager { return m.locks }

// PubSub returns the pub/sub hub.
func (m *Manager) PubSub() *pubsub.Manager { return m.pubsub }

// Replication returns the low-level replication engine.
func (m *Manager) Replication() *replication.Replicator { return m.repl }

// Invalidation returns the invalidation manager.
func (m *Manager) Invalidation() *invalidation.Manager { return m.inval }

// Events returns the module event bus (future modules subscribe here).
func (m *Manager) Events() *events.Bus { return m.bus }

// NodeID returns this node's replication identity.
func (m *Manager) NodeID() string { return m.nodeID }

// --- Generic distributed state API ---

// PutState stores a value under (namespace,id) with ttl and broadcasts the
// change so subscribers on other nodes converge. ttl <= 0 means no expiry.
func (m *Manager) PutState(ctx context.Context, namespace, id string, value any, ttl time.Duration) error {
	enc, err := m.codec.Encode(value)
	if err != nil {
		return err
	}
	if err := m.client.Set(ctx, m.kb.State(namespace, id), enc, ttl); err != nil {
		return err
	}
	m.bus.Emit(events.StateUpdated, "state", func(e *events.Event) { e.Key = namespace + ":" + id })
	return m.repl.Broadcast(ctx, replication.Update{
		Namespace: "state:" + namespace, ID: id, Payload: enc, NodeID: m.nodeID,
	})
}

// GetState decodes the value stored under (namespace,id) into dst.
func (m *Manager) GetState(ctx context.Context, namespace, id string, dst any) (bool, error) {
	raw, err := m.client.Get(ctx, m.kb.State(namespace, id))
	if err != nil {
		if errors.Is(err, types.ErrNil) {
			return false, nil
		}
		return false, err
	}
	if dst != nil {
		if err := m.codec.Decode(raw, dst); err != nil {
			return true, err
		}
	}
	return true, nil
}

// CompareAndSwapState atomically replaces the value under (namespace,id) only if
// its current encoded form equals expected's — the primitive for cross-node
// state synchronization without a lock. Pass a nil expected to require absence.
func (m *Manager) CompareAndSwapState(ctx context.Context, namespace, id string, expected, newValue any, ttl time.Duration) (bool, error) {
	var expEnc string
	if expected != nil {
		e, err := m.codec.Encode(expected)
		if err != nil {
			return false, err
		}
		expEnc = e
	}
	newEnc, err := m.codec.Encode(newValue)
	if err != nil {
		return false, err
	}
	ok, err := m.client.CompareAndSet(ctx, m.kb.State(namespace, id), expEnc, newEnc, ttl)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	_ = m.repl.Broadcast(ctx, replication.Update{Namespace: "state:" + namespace, ID: id, Payload: newEnc, NodeID: m.nodeID})
	return true, nil
}

// DeleteState removes (namespace,id) and broadcasts a tombstone.
func (m *Manager) DeleteState(ctx context.Context, namespace, id string) error {
	if _, err := m.client.Del(ctx, m.kb.State(namespace, id)); err != nil {
		return err
	}
	return m.repl.Broadcast(ctx, replication.Update{Namespace: "state:" + namespace, ID: id, NodeID: m.nodeID, Deleted: true})
}

// WatchState subscribes to replicated changes for a namespace. The handler
// receives the raw replicated update (Payload holds the encoded value).
func (m *Manager) WatchState(namespace string, handler func(replication.Update)) error {
	return m.repl.Subscribe("state:"+namespace, handler)
}

// --- Room membership (set-backed distributed state) ---

// AddRoomMember records that userID is present in roomID across the cluster.
func (m *Manager) AddRoomMember(ctx context.Context, roomID, userID string) error {
	if _, err := m.client.SAdd(ctx, m.kb.State(NamespaceRoomMembership, roomID), userID); err != nil {
		return err
	}
	return m.repl.Broadcast(ctx, replication.Update{Namespace: "membership", ID: roomID, Fields: map[string]string{"add": userID}, NodeID: m.nodeID})
}

// RemoveRoomMember removes userID from roomID's membership.
func (m *Manager) RemoveRoomMember(ctx context.Context, roomID, userID string) error {
	if _, err := m.client.SRem(ctx, m.kb.State(NamespaceRoomMembership, roomID), userID); err != nil {
		return err
	}
	return m.repl.Broadcast(ctx, replication.Update{Namespace: "membership", ID: roomID, Fields: map[string]string{"remove": userID}, NodeID: m.nodeID})
}

// RoomMembers returns the current membership of a room.
func (m *Manager) RoomMembers(ctx context.Context, roomID string) ([]string, error) {
	return m.client.SMembers(ctx, m.kb.State(NamespaceRoomMembership, roomID))
}

// IsRoomMember reports whether userID is in roomID.
func (m *Manager) IsRoomMember(ctx context.Context, roomID, userID string) (bool, error) {
	return m.client.SIsMember(ctx, m.kb.State(NamespaceRoomMembership, roomID), userID)
}

// --- Lifecycle ---

// Start launches all background workers (TTL reaper, invalidation subscriber,
// presence anti-entropy). Call once after New.
func (m *Manager) Start(ctx context.Context) error {
	if err := m.cache.Start(ctx); err != nil {
		return err
	}
	m.presence.Start()
	m.log.Redis(ctx, "state_manager_started", nil)
	return nil
}

// Ping verifies backend connectivity.
func (m *Manager) Ping(ctx context.Context) error { return m.client.Ping(ctx) }

// Health returns the module's health based on Redis connectivity.
func (m *Manager) Health(ctx context.Context) types.Health {
	if err := m.client.Ping(ctx); err != nil {
		return types.HealthDown
	}
	return types.HealthUp
}

// Close gracefully shuts down every subsystem, flushing async work. It closes
// the Redis client only if this Manager created it.
func (m *Manager) Close(ctx context.Context) error {
	_ = m.cache.Close(ctx)
	_ = m.presence.Close()
	_ = m.pubsub.Close()
	_ = m.repl.Close()
	m.inval.Stop()
	m.bus.Close()
	if m.ownsClient {
		return m.client.Close()
	}
	return nil
}
