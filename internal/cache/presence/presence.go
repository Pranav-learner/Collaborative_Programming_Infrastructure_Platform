// Package presence replicates ephemeral collaboration state — who is in a room,
// their cursor, typing indicators, heartbeat liveness, and arbitrary presence
// metadata — across all CPIP nodes with eventual consistency.
//
// Two layers cooperate:
//
//	Authoritative:  each presence record is a Redis hash with a short,
//	                heartbeat-refreshed TTL, plus a per-room member set. Any node
//	                can read the current truth directly from Redis.
//	Real-time:      every mutation is also broadcast via the replication engine
//	                (LWW ordered) so subscribers on other nodes update their
//	                in-memory view instantly and fan out to WebSocket clients
//	                without polling Redis.
//
// A record whose owner stops heart-beating simply expires — presence is
// self-healing and needs no explicit "user crashed" detection.
package presence

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/logger"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/replication"
	"cpip/internal/cache/types"
)

const namespace = "presence"

// Presence is a user's ephemeral state within a room.
type Presence struct {
	RoomID      string            `json:"room_id"`
	UserID      string            `json:"user_id"`
	State       string            `json:"state"` // e.g. online, idle, away, offline
	Cursor      string            `json:"cursor,omitempty"`
	Typing      bool              `json:"typing"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	HeartbeatAt time.Time         `json:"heartbeat_at"`
	Version     int64             `json:"version"`
	NodeID      string            `json:"node_id"`
	Left        bool              `json:"left,omitempty"`
}

// Handler receives live presence updates for a subscribed room.
type Handler func(Presence)

// Manager coordinates presence storage and replication.
type Manager struct {
	client redis.Client
	repl   *replication.Replicator
	kb     keys.Builder
	cfg    config.Replication
	ttl    time.Duration
	nodeID string
	rec    metrics.Recorder
	log    *logger.Logger

	mu       sync.RWMutex
	handlers map[string][]Handler // roomID -> handlers
	subbed   map[string]bool      // roomID -> replication subscribed
	// local materialized view for fast reads and anti-entropy resync.
	view map[string]map[string]Presence // roomID -> userID -> presence

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	now    func() time.Time
}

// Params configures a Manager.
type Params struct {
	Client      redis.Client
	Replicator  *replication.Replicator
	Keys        keys.Builder
	Config      config.Replication
	PresenceTTL time.Duration
	NodeID      string
	Metrics     metrics.Recorder
	Logger      *logger.Logger
}

// New constructs a presence Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	log := p.Logger
	if log == nil {
		log = logger.New(nil)
	}
	ttl := p.PresenceTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		client:   p.Client,
		repl:     p.Replicator,
		kb:       p.Keys,
		cfg:      p.Config,
		ttl:      ttl,
		nodeID:   p.NodeID,
		rec:      rec,
		log:      log,
		handlers: make(map[string][]Handler),
		subbed:   make(map[string]bool),
		view:     make(map[string]map[string]Presence),
		ctx:      ctx,
		cancel:   cancel,
		now:      time.Now,
	}
}

// --- encoding between Presence and hash fields ---

func (p Presence) toFields() map[string]string {
	meta, _ := json.Marshal(p.Metadata)
	return map[string]string{
		"state":        p.State,
		"cursor":       p.Cursor,
		"typing":       strconv.FormatBool(p.Typing),
		"metadata":     string(meta),
		"heartbeat_at": strconv.FormatInt(p.HeartbeatAt.UnixMilli(), 10),
		"version":      strconv.FormatInt(p.Version, 10),
		"node_id":      p.NodeID,
	}
}

func presenceFromFields(roomID, userID string, f map[string]string) Presence {
	typing, _ := strconv.ParseBool(f["typing"])
	hb, _ := strconv.ParseInt(f["heartbeat_at"], 10, 64)
	ver, _ := strconv.ParseInt(f["version"], 10, 64)
	var meta map[string]string
	if f["metadata"] != "" {
		_ = json.Unmarshal([]byte(f["metadata"]), &meta)
	}
	return Presence{
		RoomID:      roomID,
		UserID:      userID,
		State:       f["state"],
		Cursor:      f["cursor"],
		Typing:      typing,
		Metadata:    meta,
		HeartbeatAt: time.UnixMilli(hb),
		Version:     ver,
		NodeID:      f["node_id"],
	}
}

// Announce publishes or refreshes a user's presence in a room. It writes the
// authoritative Redis record (with TTL), indexes room membership, updates the
// local view, and broadcasts the change to other nodes.
func (m *Manager) Announce(ctx context.Context, p Presence) error {
	if p.NodeID == "" {
		p.NodeID = m.nodeID
	}
	now := m.now()
	if p.HeartbeatAt.IsZero() {
		p.HeartbeatAt = now
	}
	p.Version = m.repl.NextVersion()

	key := m.kb.Presence(p.RoomID, p.UserID)
	if err := m.client.HSet(ctx, key, p.toFields()); err != nil {
		return err
	}
	if _, err := m.client.Expire(ctx, key, m.ttl); err != nil {
		return err
	}
	if _, err := m.client.SAdd(ctx, m.kb.PresenceRoom(p.RoomID), p.UserID); err != nil {
		return err
	}
	m.updateView(p)
	return m.broadcast(ctx, p)
}

// UpdateCursor is a partial update of a user's cursor position.
func (m *Manager) UpdateCursor(ctx context.Context, roomID, userID, cursor string) error {
	return m.mutate(ctx, roomID, userID, func(p *Presence) { p.Cursor = cursor })
}

// SetTyping updates a user's typing indicator.
func (m *Manager) SetTyping(ctx context.Context, roomID, userID string, typing bool) error {
	return m.mutate(ctx, roomID, userID, func(p *Presence) { p.Typing = typing })
}

// SetState updates a user's presence state (online/idle/away/…).
func (m *Manager) SetState(ctx context.Context, roomID, userID, state string) error {
	return m.mutate(ctx, roomID, userID, func(p *Presence) { p.State = state })
}

// Heartbeat refreshes a user's liveness, extending the record's TTL.
func (m *Manager) Heartbeat(ctx context.Context, roomID, userID string) error {
	return m.mutate(ctx, roomID, userID, func(p *Presence) { p.HeartbeatAt = m.now() })
}

// mutate loads the current presence (from local view or Redis), applies fn, and
// re-announces. Presence is small and single-writer-per-user, so a full
// re-announce with LWW versioning is simpler and safer than field-level CAS.
func (m *Manager) mutate(ctx context.Context, roomID, userID string, fn func(*Presence)) error {
	cur, ok := m.viewGet(roomID, userID)
	if !ok {
		got, err := m.getRecord(ctx, roomID, userID)
		if err != nil {
			if errors.Is(err, types.ErrNil) {
				cur = Presence{RoomID: roomID, UserID: userID, State: "online"}
			} else {
				return err
			}
		} else {
			cur = got
		}
	}
	cur.HeartbeatAt = m.now()
	fn(&cur)
	return m.Announce(ctx, cur)
}

// Leave removes a user's presence from a room and broadcasts a tombstone so
// other nodes drop it from their views immediately.
func (m *Manager) Leave(ctx context.Context, roomID, userID string) error {
	key := m.kb.Presence(roomID, userID)
	if _, err := m.client.Del(ctx, key); err != nil {
		return err
	}
	if _, err := m.client.SRem(ctx, m.kb.PresenceRoom(roomID), userID); err != nil {
		return err
	}
	m.removeFromView(roomID, userID)
	tomb := Presence{RoomID: roomID, UserID: userID, NodeID: m.nodeID, Left: true, Version: m.repl.NextVersion()}
	return m.broadcast(ctx, tomb)
}

func (m *Manager) broadcast(ctx context.Context, p Presence) error {
	payload, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return m.repl.Broadcast(ctx, replication.Update{
		Namespace: namespace,
		ID:        p.RoomID + "|" + p.UserID,
		Payload:   string(payload),
		Version:   p.Version,
		NodeID:    p.NodeID,
		Deleted:   p.Left,
	})
}

// getRecord reads a single authoritative presence record from Redis.
func (m *Manager) getRecord(ctx context.Context, roomID, userID string) (Presence, error) {
	f, err := m.client.HGetAll(ctx, m.kb.Presence(roomID, userID))
	if err != nil {
		return Presence{}, err
	}
	if len(f) == 0 {
		return Presence{}, types.ErrNil
	}
	return presenceFromFields(roomID, userID, f), nil
}

// GetRoom returns the authoritative presence of every user in a room, read from
// Redis. Stale membership entries (whose records expired) are pruned.
func (m *Manager) GetRoom(ctx context.Context, roomID string) ([]Presence, error) {
	users, err := m.client.SMembers(ctx, m.kb.PresenceRoom(roomID))
	if err != nil {
		return nil, err
	}
	out := make([]Presence, 0, len(users))
	for _, u := range users {
		p, err := m.getRecord(ctx, roomID, u)
		if err != nil {
			if errors.Is(err, types.ErrNil) {
				_, _ = m.client.SRem(ctx, m.kb.PresenceRoom(roomID), u)
				m.removeFromView(roomID, u)
				continue
			}
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// GetRoomLocal returns the room's presence from the in-memory materialized view
// (no Redis round trip) — useful for high-frequency fan-out on the hot path.
func (m *Manager) GetRoomLocal(roomID string) []Presence {
	m.mu.RLock()
	defer m.mu.RUnlock()
	room := m.view[roomID]
	out := make([]Presence, 0, len(room))
	for _, p := range room {
		out = append(out, p)
	}
	return out
}

// Subscribe registers a handler for live presence updates in a room and ensures
// the replication listener for the presence namespace is running.
func (m *Manager) Subscribe(roomID string, h Handler) error {
	m.mu.Lock()
	m.handlers[roomID] = append(m.handlers[roomID], h)
	needSub := !m.subbed[roomID]
	m.subbed[roomID] = true
	firstEver := len(m.subbed) == 1
	m.mu.Unlock()

	if firstEver {
		// One replication subscription for the whole presence namespace; the
		// apply handler routes by room.
		if err := m.repl.Subscribe(namespace, m.onReplicated); err != nil {
			return err
		}
	}
	_ = needSub
	return nil
}

// onReplicated is the replication apply callback: it decodes the presence delta,
// updates the local view, and dispatches to room handlers.
func (m *Manager) onReplicated(u replication.Update) {
	var p Presence
	if err := json.Unmarshal([]byte(u.Payload), &p); err != nil {
		return
	}
	if p.Left {
		m.removeFromView(p.RoomID, p.UserID)
	} else {
		m.updateView(p)
	}
	m.mu.RLock()
	handlers := append([]Handler(nil), m.handlers[p.RoomID]...)
	m.mu.RUnlock()
	for _, h := range handlers {
		h(p)
	}
	m.log.Presence(m.ctx, "applied", p.RoomID, p.UserID, true)
}

// --- local view helpers ---

func (m *Manager) updateView(p Presence) {
	m.mu.Lock()
	room := m.view[p.RoomID]
	if room == nil {
		room = make(map[string]Presence)
		m.view[p.RoomID] = room
	}
	// LWW at the view level too, so a late Redis read cannot overwrite a newer
	// replicated delta.
	if cur, ok := room[p.UserID]; ok && cur.Version > p.Version {
		m.mu.Unlock()
		return
	}
	room[p.UserID] = p
	m.mu.Unlock()
}

func (m *Manager) removeFromView(roomID, userID string) {
	m.mu.Lock()
	if room := m.view[roomID]; room != nil {
		delete(room, userID)
		if len(room) == 0 {
			delete(m.view, roomID)
		}
	}
	m.mu.Unlock()
}

func (m *Manager) viewGet(roomID, userID string) (Presence, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if room := m.view[roomID]; room != nil {
		p, ok := room[userID]
		return p, ok
	}
	return Presence{}, false
}

// Start launches the anti-entropy resync loop, which periodically re-reads each
// locally-tracked room from Redis to heal any deltas missed during a pub/sub
// disconnect (the eventual-consistency safety net).
func (m *Manager) Start() {
	m.wg.Add(1)
	go m.antiEntropyLoop()
}

func (m *Manager) antiEntropyLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.AntiEntropyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.RLock()
			rooms := make([]string, 0, len(m.subbed))
			for r := range m.subbed {
				rooms = append(rooms, r)
			}
			m.mu.RUnlock()
			for _, r := range rooms {
				if _, err := m.GetRoom(m.ctx, r); err != nil {
					m.log.Presence(m.ctx, "anti_entropy_error", r, "", false)
				}
			}
		}
	}
}

// Close stops background loops. It does not close the shared replicator/client.
func (m *Manager) Close() error {
	m.cancel()
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	return nil
}
