// Package presence is the orchestrator of the Presence & Awareness System.
package presence

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"cpip/internal/presence/activity"
	"cpip/internal/presence/awareness"
	"cpip/internal/presence/config"
	"cpip/internal/presence/cursor"
	"cpip/internal/presence/events"
	"cpip/internal/presence/heartbeat"
	"cpip/internal/presence/metrics"
	"cpip/internal/presence/registry"
	"cpip/internal/presence/selection"
	"cpip/internal/presence/types"
	"cpip/internal/presence/typing"
)

// Transport abstracts connection delivery to clients.
type Transport interface {
	SendText(connID string, payload []byte) error
}

// Manager orchestrates all presence tracking and awareness broadcasting.
type Manager struct {
	cfg          config.Config
	reg          *registry.Registry
	bus          *events.Bus
	metrics      metrics.Recorder
	transport    Transport
	log          *slog.Logger

	cursorMgr    *cursor.Manager
	selectionMgr *selection.Manager
	typingMgr    *typing.Manager
	heartbeatMon *heartbeat.Monitor
	awarenessMgr *awareness.Manager

	trackersMu   sync.RWMutex
	trackers     map[string]*activity.Tracker // connID -> tracker

	dirtyMu      sync.Mutex
	dirty        map[string]map[string]struct{} // roomID -> connIDs

	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// Params configures the Manager.
type Params struct {
	Config    config.Config
	Metrics   metrics.Recorder
	Transport Transport
	Logger    *slog.Logger
}

// NewManager builds a presence Manager.
func NewManager(p Params) *Manager {
	if p.Metrics == nil {
		p.Metrics = metrics.NewNoop()
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}

	bus := events.New(events.Options{
		OnDrop: func() {
			p.Metrics.EventDropped()
		},
		OnPublish: func() {
			p.Metrics.EventPublished()
		},
	})

	reg := registry.New()
	cursorMgr := cursor.New()
	selectionMgr := selection.New()
	typingMgr := typing.New(2*time.Second, p.Config.TypingTimeout)
	heartbeatMon := heartbeat.NewMonitor(p.Config.HeartbeatInterval*2, p.Config.RecoveryTimeout)
	awarenessMgr := awareness.New()

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		cfg:          p.Config,
		reg:          reg,
		bus:          bus,
		metrics:      p.Metrics,
		transport:    p.Transport,
		log:          p.Logger,
		cursorMgr:    cursorMgr,
		selectionMgr: selectionMgr,
		typingMgr:    typingMgr,
		heartbeatMon: heartbeatMon,
		awarenessMgr: awarenessMgr,
		trackers:     make(map[string]*activity.Tracker),
		dirty:        make(map[string]map[string]struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start runs the background tasks (sweeper loop and broadcast flusher loop).
func (m *Manager) Start() {
	m.wg.Add(2)
	go m.sweepLoop()
	go m.broadcastLoop()
}

// Stop halts all background loops and closes the event bus.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.bus.Close()
}

// Register signs online a new connection presence.
func (m *Manager) Register(connID, userID, roomID, sessionID, reconnectToken string, meta map[string]any) error {
	// Size limit check on metadata
	if meta != nil {
		bytes, err := json.Marshal(meta)
		if err == nil && len(bytes) > m.cfg.MaxMetadataSize {
			return ErrMetadataTooLarge
		}
	}

	now := time.Now()
	p := types.Presence{
		UserID:         userID,
		ConnID:         connID,
		SessionID:      sessionID,
		RoomID:         roomID,
		State:          types.StateOnline,
		JoinTime:       now,
		LastHeartbeat:  now,
		LastActivity:   now,
		ReconnectToken: reconnectToken,
		Metadata:       meta,
	}

	if err := m.reg.Register(p); err != nil {
		if errors.Is(err, registry.ErrSessionConflict) {
			m.metrics.PresenceConflict()
			return err
		}
		return err
	}

	// Create activity tracker
	m.trackersMu.Lock()
	tr := activity.NewTracker()
	tr.Record(activity.ActionJoin, now)
	m.trackers[connID] = tr
	m.trackersMu.Unlock()

	m.metrics.ActiveConnections(m.reg.Len())

	// Publish Event
	m.bus.Publish(events.Event{
		Type:      events.PresenceOnline,
		UserID:    userID,
		RoomID:    roomID,
		SessionID: sessionID,
		ConnID:    connID,
		At:        now,
	})

	// Late join synchronization: send full sync of the room to this new connection immediately
	m.sendFullSync(connID, roomID)

	// Mark new participant dirty so other room members receive their online presence in the next tick
	m.markDirty(roomID, connID)
	return nil
}

// Deregister disconnects a presence, moving it to offline or initiating recovery window if applicable.
func (m *Manager) Deregister(connID string, reason string) {
	now := time.Now()
	p, err := m.reg.Get(connID)
	if !err {
		return
	}

	// If we are deregistering but there is a recovery window, move state to Disconnected first
	if p.ReconnectToken != "" && m.cfg.RecoveryTimeout > 0 && reason != "explicit_leave" {
		_, updateErr := m.reg.UpdateState(connID, types.StateDisconnected)
		if updateErr == nil {
			m.bus.Publish(events.Event{
				Type:      events.PresenceOffline,
				UserID:    p.UserID,
				RoomID:    p.RoomID,
				SessionID: p.SessionID,
				ConnID:    connID,
				At:        now,
				Payload:   "disconnected",
			})
			m.markDirty(p.RoomID, connID)
			return
		}
	}

	// Normal offline teardown
	_, _ = m.reg.Deregister(connID)

	m.trackersMu.Lock()
	delete(m.trackers, connID)
	m.trackersMu.Unlock()

	m.metrics.ActiveConnections(m.reg.Len())

	m.bus.Publish(events.Event{
		Type:      events.PresenceOffline,
		UserID:    p.UserID,
		RoomID:    p.RoomID,
		SessionID: p.SessionID,
		ConnID:    connID,
		At:        now,
		Payload:   reason,
	})

	m.markDirty(p.RoomID, connID)
}

// Heartbeat refreshes the last heartbeat and resets inactivity states.
func (m *Manager) Heartbeat(connID string) error {
	now := time.Now()
	_, err := m.reg.Mutate(connID, func(p *types.Presence) error {
		p.LastHeartbeat = now

		// Update activity tracker
		m.trackersMu.RLock()
		tr, exists := m.trackers[connID]
		m.trackersMu.RUnlock()
		if exists {
			tr.Record(activity.ActionHeartbeat, now)
		}

		// Rescue from Idle/Away if they checked in and were active
		if p.State == types.StateIdle || p.State == types.StateAway {
			p.State = types.StateOnline
			p.LastActivity = now
			m.bus.Publish(events.Event{
				Type:      events.PresenceOnline,
				UserID:    p.UserID,
				RoomID:    p.RoomID,
				SessionID: p.SessionID,
				ConnID:    p.ConnID,
				At:        now,
			})
			m.markDirty(p.RoomID, p.ConnID)
		}
		return nil
	})

	if err != nil {
		return err
	}

	m.metrics.HeartbeatReceived()
	return nil
}

// UpdateCursor mutates cursor coordinates with throttling/broadcast coordination.
func (m *Manager) UpdateCursor(connID string, line, ch int, color string, filePath string, visible bool) error {
	now := time.Now()
	var roomID string

	_, err := m.reg.Mutate(connID, func(p *types.Presence) error {
		roomID = p.RoomID
		p.LastActivity = now

		if p.State == types.StateIdle || p.State == types.StateAway {
			p.State = types.StateOnline
		}

		if err := m.cursorMgr.Update(&p.Cursor, line, ch, color, filePath, visible); err != nil {
			return err
		}

		m.trackersMu.RLock()
		tr, exists := m.trackers[connID]
		m.trackersMu.RUnlock()
		if exists {
			tr.Record(activity.ActionCursorMove, now)
		}

		return nil
	})

	if err != nil {
		return err
	}

	m.bus.Publish(events.Event{
		Type:      events.CursorMoved,
		RoomID:    roomID,
		ConnID:    connID,
		At:        now,
	})

	m.markDirty(roomID, connID)
	return nil
}

// UpdateSelection mutates cursor selections.
func (m *Manager) UpdateSelection(connID string, anchorLine, anchorCh, focusLine, focusCh int) error {
	now := time.Now()
	var roomID string

	_, err := m.reg.Mutate(connID, func(p *types.Presence) error {
		roomID = p.RoomID
		p.LastActivity = now

		if p.State == types.StateIdle || p.State == types.StateAway {
			p.State = types.StateOnline
		}

		if err := m.selectionMgr.Update(&p.Selection, anchorLine, anchorCh, focusLine, focusCh); err != nil {
			return err
		}

		m.trackersMu.RLock()
		tr, exists := m.trackers[connID]
		m.trackersMu.RUnlock()
		if exists {
			tr.Record(activity.ActionSelectionChange, now)
		}

		return nil
	})

	if err != nil {
		return err
	}

	m.bus.Publish(events.Event{
		Type:      events.SelectionChanged,
		RoomID:    roomID,
		ConnID:    connID,
		At:        now,
	})

	m.markDirty(roomID, connID)
	return nil
}

// UpdateTyping mutates the typing indicator state.
func (m *Manager) UpdateTyping(connID string, isTyping bool) error {
	now := time.Now()
	var roomID string
	var changed bool

	_, err := m.reg.Mutate(connID, func(p *types.Presence) error {
		roomID = p.RoomID
		p.LastActivity = now

		if p.State == types.StateIdle || p.State == types.StateAway {
			p.State = types.StateOnline
		}

		if m.typingMgr.ShouldPublish(isTyping, p.IsTyping, p.LastActivity, now) {
			changed = true
		}

		p.IsTyping = isTyping

		m.trackersMu.RLock()
		tr, exists := m.trackers[connID]
		m.trackersMu.RUnlock()
		if exists {
			tr.Record(activity.ActionTyping, now)
		}

		return nil
	})

	if err != nil {
		return err
	}

	if changed {
		evType := events.TypingStarted
		if !isTyping {
			evType = events.TypingStopped
		}
		m.bus.Publish(events.Event{
			Type:   evType,
			RoomID: roomID,
			ConnID: connID,
			At:     now,
		})
		m.markDirty(roomID, connID)
	} else {
		m.metrics.TypingFloodPrevented()
	}

	return nil
}

// RecoverSession rescues a disconnected session within the recovery window.
func (m *Manager) RecoverSession(userID, token, newConnID string) (types.Presence, error) {
	now := time.Now()
	presList := m.reg.ListByUser(userID)

	var target types.Presence
	var found bool
	for _, p := range presList {
		if p.ReconnectToken == token {
			target = p
			found = true
			break
		}
	}

	if !found {
		return types.Presence{}, heartbeat.ErrTokenMismatch
	}

	if err := m.heartbeatMon.ValidateRecovery(target, token, now); err != nil {
		if errors.Is(err, heartbeat.ErrSessionExpired) {
			m.metrics.ReconnectExpired()
		}
		return types.Presence{}, err
	}

	// Evict the old connection registration
	_, _ = m.reg.Deregister(target.ConnID)

	m.trackersMu.Lock()
	delete(m.trackers, target.ConnID)
	m.trackers[newConnID] = activity.NewTracker()
	m.trackers[newConnID].Record(activity.ActionJoin, now)
	m.trackersMu.Unlock()

	// Register under the new connection ID
	recovered := target
	recovered.ConnID = newConnID
	recovered.State = types.StateOnline
	recovered.LastHeartbeat = now
	recovered.LastActivity = now

	if err := m.reg.Register(recovered); err != nil {
		return recovered, err
	}

	m.bus.Publish(events.Event{
		Type:      events.PresenceRecovered,
		UserID:    recovered.UserID,
		RoomID:    recovered.RoomID,
		SessionID: recovered.SessionID,
		ConnID:    newConnID,
		At:        now,
	})

	m.sendFullSync(newConnID, recovered.RoomID)
	m.markDirty(recovered.RoomID, newConnID)

	return recovered, nil
}

// GetRoomPresence returns the presence list for a room.
func (m *Manager) GetRoomPresence(roomID string) []types.Presence {
	return m.reg.ListByRoom(roomID)
}

// GetTrackerStats returns the activity stats of a connection.
func (m *Manager) GetTrackerStats(connID string) (activity.Stats, bool) {
	m.trackersMu.RLock()
	tr, ok := m.trackers[connID]
	m.trackersMu.RUnlock()
	if !ok {
		return activity.Stats{}, false
	}
	return tr.Stats(), true
}

// Registry returns the underlying registry.
func (m *Manager) Registry() *registry.Registry {
	return m.reg
}

// Events returns the internal event bus.
func (m *Manager) Events() *events.Bus {
	return m.bus
}

func (m *Manager) markDirty(roomID, connID string) {
	if roomID == "" || connID == "" {
		return
	}
	m.dirtyMu.Lock()
	defer m.dirtyMu.Unlock()
	if _, ok := m.dirty[roomID]; !ok {
		m.dirty[roomID] = make(map[string]struct{})
	}
	m.dirty[roomID][connID] = struct{}{}
}

func (m *Manager) sendFullSync(connID string, roomID string) {
	if m.transport == nil {
		return
	}
	presences := m.reg.ListByRoom(roomID)
	frame := m.awarenessMgr.BuildFullSync(roomID, presences)
	payload, err := json.Marshal(frame)
	if err == nil {
		_ = m.transport.SendText(connID, payload)
	}
}

func (m *Manager) sweepLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.runSweep()
		}
	}
}

func (m *Manager) runSweep() {
	now := time.Now()
	presences := m.reg.ListByState(types.StateOnline)
	presences = append(presences, m.reg.ListByState(types.StateIdle)...)
	presences = append(presences, m.reg.ListByState(types.StateAway)...)

	// 1. Heartbeat Timeout / Idle / Away Sweeps
	for _, p := range presences {
		// A. Check missed heartbeat
		if m.heartbeatMon.IsDead(p.LastHeartbeat, now) {
			_, err := m.reg.UpdateState(p.ConnID, types.StateDisconnected)
			if err == nil {
				m.metrics.HeartbeatTimeout()
				m.bus.Publish(events.Event{
					Type:      events.HeartbeatTimeout,
					UserID:    p.UserID,
					RoomID:    p.RoomID,
					SessionID: p.SessionID,
					ConnID:    p.ConnID,
					At:        now,
				})
				m.bus.Publish(events.Event{
					Type:      events.PresenceOffline,
					UserID:    p.UserID,
					RoomID:    p.RoomID,
					SessionID: p.SessionID,
					ConnID:    p.ConnID,
					At:        now,
					Payload:   "heartbeat_timeout",
				})
				m.markDirty(p.RoomID, p.ConnID)
			}
			continue
		}

		// B. Idle/Away/Typing Expiry Sweeps
		var changed bool
		_, _ = m.reg.Mutate(p.ConnID, func(pr *types.Presence) error {
			// Idle/Away
			if pr.State == types.StateOnline {
				if now.Sub(pr.LastActivity) >= m.cfg.AwayTimeout {
					pr.State = types.StateAway
					changed = true
					m.bus.Publish(events.Event{
						Type:      events.UserAway,
						UserID:    pr.UserID,
						RoomID:    pr.RoomID,
						SessionID: pr.SessionID,
						ConnID:    pr.ConnID,
						At:        now,
					})
				} else if now.Sub(pr.LastActivity) >= m.cfg.IdleTimeout {
					pr.State = types.StateIdle
					changed = true
					m.bus.Publish(events.Event{
						Type:      events.UserIdle,
						UserID:    pr.UserID,
						RoomID:    pr.RoomID,
						SessionID: pr.SessionID,
						ConnID:    pr.ConnID,
						At:        now,
					})
				}
			} else if pr.State == types.StateIdle {
				if now.Sub(pr.LastActivity) >= m.cfg.AwayTimeout {
					pr.State = types.StateAway
					changed = true
					m.bus.Publish(events.Event{
						Type:      events.UserAway,
						UserID:    pr.UserID,
						RoomID:    pr.RoomID,
						SessionID: pr.SessionID,
						ConnID:    pr.ConnID,
						At:        now,
					})
				}
			}

			// Typing expiration
			if pr.IsTyping && m.typingMgr.IsExpired(pr.LastActivity, now) {
				pr.IsTyping = false
				changed = true
				m.bus.Publish(events.Event{
					Type:      events.TypingStopped,
					RoomID:    pr.RoomID,
					ConnID:    pr.ConnID,
					At:        now,
				})
			}

			return nil
		})

		if changed {
			m.markDirty(p.RoomID, p.ConnID)
		}
	}

	// 2. Recovery Timeout Eviction
	disconnected := m.reg.ListByState(types.StateDisconnected)
	for _, p := range disconnected {
		lastSeen := p.LastActivity
		if p.LastHeartbeat.After(lastSeen) {
			lastSeen = p.LastHeartbeat
		}
		if now.Sub(lastSeen) >= m.cfg.RecoveryTimeout {
			_, _ = m.reg.Deregister(p.ConnID)

			m.trackersMu.Lock()
			delete(m.trackers, p.ConnID)
			m.trackersMu.Unlock()

			m.bus.Publish(events.Event{
				Type:      events.PresenceOffline,
				UserID:    p.UserID,
				RoomID:    p.RoomID,
				SessionID: p.SessionID,
				ConnID:    p.ConnID,
				At:        now,
				Payload:   "recovery_expired",
			})
			m.markDirty(p.RoomID, p.ConnID)
		}
	}
}

func (m *Manager) broadcastLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.BroadcastInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.flushDirtyBroadcasts()
		}
	}
}

func (m *Manager) flushDirtyBroadcasts() {
	m.dirtyMu.Lock()
	if len(m.dirty) == 0 {
		m.dirtyMu.Unlock()
		return
	}
	work := m.dirty
	m.dirty = make(map[string]map[string]struct{})
	m.dirtyMu.Unlock()

	for roomID, conns := range work {
		updates := make([]types.Presence, 0, len(conns))
		for connID := range conns {
			p, ok := m.reg.Get(connID)
			if ok {
				updates = append(updates, p)
			} else {
				// Participant left completely, represent as Offline state in incremental sync
				updates = append(updates, types.Presence{
					ConnID: connID,
					RoomID: roomID,
					State:  types.StateOffline,
				})
			}
		}

		if len(updates) == 0 {
			continue
		}

		// Build incremental sync Frame
		frame := m.awarenessMgr.BuildIncrementalSync(roomID, updates)
		payload, err := json.Marshal(frame)
		if err != nil {
			continue
		}

		// Broadcast to all active connection IDs currently in the room
		roomConns := m.reg.ListByRoom(roomID)
		for _, rc := range roomConns {
			if rc.State == types.StateOnline || rc.State == types.StateIdle || rc.State == types.StateAway {
				_ = m.transport.SendText(rc.ConnID, payload)
			}
		}
	}
}
