// Package rooms is the top-level orchestrator of the Room Management System.
package rooms

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cpip/internal/rooms/config"
	"cpip/internal/rooms/events"
	"cpip/internal/rooms/lifecycle"
	"cpip/internal/rooms/membership"
	"cpip/internal/rooms/metrics"
	"cpip/internal/rooms/permissions"
	"cpip/internal/rooms/recovery"
	"cpip/internal/rooms/registry"
	"cpip/internal/rooms/room"
	"cpip/internal/rooms/storage"
)

// Manager coordinates all room subsystems: registry, membership, events,
// recovery, storage, metrics, and background cleanup loops.
type Manager struct {
	cfg      config.Config
	reg      *registry.Registry
	mem      *membership.Manager
	bus      *events.Bus
	metrics  metrics.Recorder
	repo     storage.Repository
	tracker  *recovery.Tracker
	log      *slog.Logger

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// Params configure the rooms.Manager.
type Params struct {
	Config  config.Config
	Metrics metrics.Recorder
	Repo    storage.Repository
	Logger  *slog.Logger
}

// NewManager constructs a rooms.Manager.
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

	reg := registry.New(p.Metrics)
	mem := membership.New(reg, bus, p.Metrics, p.Repo, p.Logger)
	tracker := recovery.NewTracker()

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		cfg:     p.Config,
		reg:     reg,
		mem:     mem,
		bus:     bus,
		metrics: p.Metrics,
		repo:    p.Repo,
		tracker: tracker,
		log:     p.Logger,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start spawns the background cleanup janitor.
func (m *Manager) Start() {
	m.wg.Add(1)
	go m.janitorLoop()
}

// Stop stops the janitor and shuts down the event bus.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.bus.Close()
}

// Registry returns the room registry.
func (m *Manager) Registry() *registry.Registry {
	return m.reg
}

// Membership returns the membership manager.
func (m *Manager) Membership() *membership.Manager {
	return m.mem
}

// Events returns the event bus.
func (m *Manager) Events() *events.Bus {
	return m.bus
}

// CreateRoom initializes and registers a new room.
func (m *Manager) CreateRoom(ctx context.Context, id, name, ownerID string, customCfg *room.Config, meta map[string]any) (*room.Room, error) {
	cfg := room.Config{
		MaxParticipants: m.cfg.DefaultMaxParticipants,
		IdleTimeout:     m.cfg.DefaultIdleTimeout,
		ExpireTimeout:   m.cfg.DefaultExpireTimeout,
		RecoveryTimeout: m.cfg.DefaultRecoveryTimeout,
		Visibility:      room.VisibilityPrivate,
	}
	if customCfg != nil {
		cfg = *customCfg
	}

	now := time.Now()
	r := room.New(room.Params{
		ID:             id,
		Name:           name,
		OwnerID:        ownerID,
		Config:         cfg,
		Policy:         permissions.Default(),
		Metadata:       meta,
		Now:            now,
		OwnerSessionID: "",
		OwnerConnID:    "",
	})

	// Initial transition. Since no one is connected yet, transition to Waiting.
	if err := r.Transition(lifecycle.StateWaiting, now); err != nil {
		return nil, fmt.Errorf("initial state transition failed: %w", err)
	}

	if err := m.reg.Register(r); err != nil {
		return nil, err
	}

	m.metrics.RoomCreated()
	m.metrics.StateTransition(lifecycle.StateWaiting.String())

	m.bus.Publish(events.Event{
		Type:    events.RoomCreated,
		RoomID:  id,
		ActorID: ownerID,
		Role:    permissions.RoleOwner,
		At:      now,
	})

	m.saveRoomSnapshot(ctx, r)
	return r, nil
}

// SetConnected sets a participant's connection status.
func (m *Manager) SetConnected(ctx context.Context, roomID, userID string, connected bool, connID, sessionID string) error {
	r, ok := m.reg.Get(roomID)
	if !ok {
		return registry.ErrRoomNotFound
	}

	now := time.Now()
	p, ok, nowEmpty := r.SetConnected(userID, connected, connID, sessionID, now)
	if !ok {
		return room.ErrParticipantNotFound
	}

	m.bus.Publish(events.Event{
		Type:    events.MembershipUpdated,
		RoomID:  roomID,
		ActorID: userID,
		Role:    p.Role,
		At:      now,
	})

	if !connected {
		m.metrics.RecoveryStarted()
		m.log.Info("participant disconnected, entered recovery window", "room_id", roomID, "user_id", userID)
	}

	// If the room just became empty of connected participants, transition to Waiting.
	if nowEmpty {
		if r.State() == lifecycle.StateActive {
			prev := r.State()
			if err := r.Transition(lifecycle.StateWaiting, now); err == nil {
				m.metrics.StateTransition(lifecycle.StateWaiting.String())
				m.bus.Publish(events.Event{
					Type:   events.StateChanged,
					RoomID: roomID,
					From:   prev,
					To:     lifecycle.StateWaiting,
					At:     now,
				})
			}
		}
	} else if connected {
		// A connection was made; if the room was Waiting, Idle, or Expiring, pull it back to Active.
		if r.State() != lifecycle.StateActive {
			prev := r.State()
			if r.Touch(now) {
				m.metrics.StateTransition(lifecycle.StateActive.String())
				m.bus.Publish(events.Event{
					Type:   events.RoomRecovered,
					RoomID: roomID,
					At:     now,
				})
				m.bus.Publish(events.Event{
					Type:   events.StateChanged,
					RoomID: roomID,
					From:   prev,
					To:     lifecycle.StateActive,
					At:     now,
				})
			}
		}
	}

	m.saveRoomSnapshot(ctx, r)
	return nil
}

// CloseRoom explicitly terminates a room.
func (m *Manager) CloseRoom(ctx context.Context, roomID, actorID string) error {
	r, ok := m.reg.Get(roomID)
	if !ok {
		return registry.ErrRoomNotFound
	}

	if err := r.Authorize(actorID, permissions.ActionCloseRoom); err != nil {
		return err
	}

	now := time.Now()
	prev := r.State()
	if err := r.Transition(lifecycle.StateClosed, now); err != nil {
		return err
	}

	m.metrics.RoomClosed("explicit")
	m.metrics.StateTransition(lifecycle.StateClosed.String())

	m.bus.Publish(events.Event{
		Type:    events.RoomClosed,
		RoomID:  roomID,
		ActorID: actorID,
		From:    prev,
		To:      lifecycle.StateClosed,
		Reason:  "explicit",
		At:      now,
	})

	m.saveRoomSnapshot(ctx, r)
	return nil
}

func (m *Manager) janitorLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.runCleanup()
		}
	}
}

func (m *Manager) runCleanup() {
	now := time.Now()
	rooms := m.reg.List()

	for _, r := range rooms {
		m.cleanupRoom(r, now)
	}
}

func (m *Manager) cleanupRoom(r *room.Room, now time.Time) {
	// 1. Evict expired recovery sessions.
	cfg := r.Config()
	for _, p := range r.Participants() {
		if !p.Connected {
			if m.tracker.IsExpired(p.LastSeen, cfg.RecoveryTimeout, now) {
				_, err := r.Leave(p.UserID, now)
				if err == nil {
					m.metrics.RecoveryExpired()
					m.metrics.ParticipantLeft("recovery_expired")
					m.bus.Publish(events.Event{
						Type:    events.UserLeft,
						RoomID:  r.ID(),
						ActorID: p.UserID,
						Role:    p.Role,
						Reason:  "recovery_expired",
						At:      now,
					})
					m.log.Info("evicted participant: recovery window expired", "room_id", r.ID(), "user_id", p.UserID)
				}
			}
		}
	}

	// 2. Room state transitions.
	state := r.State()
	lastAct := r.LastActivity()
	connCount := r.ConnectedCount()

	switch state {
	case lifecycle.StateActive:
		if connCount == 0 || now.Sub(lastAct) > cfg.IdleTimeout {
			prev := state
			target := lifecycle.StateIdle
			if connCount == 0 {
				target = lifecycle.StateWaiting
			}
			if err := r.Transition(target, now); err == nil {
				m.metrics.StateTransition(target.String())
				m.bus.Publish(events.Event{
					Type:   events.StateChanged,
					RoomID: r.ID(),
					From:   prev,
					To:     target,
					At:     now,
				})
				m.saveRoomSnapshot(m.ctx, r)
			}
		}

	case lifecycle.StateWaiting:
		if now.Sub(lastAct) > cfg.ExpireTimeout {
			prev := state
			if err := r.Transition(lifecycle.StateExpiring, now); err == nil {
				m.metrics.StateTransition(lifecycle.StateExpiring.String())
				m.bus.Publish(events.Event{
					Type:   events.RoomExpired,
					RoomID: r.ID(),
					From:   prev,
					To:     lifecycle.StateExpiring,
					At:     now,
				})
				m.bus.Publish(events.Event{
					Type:   events.StateChanged,
					RoomID: r.ID(),
					From:   prev,
					To:     lifecycle.StateExpiring,
					At:     now,
				})
				m.saveRoomSnapshot(m.ctx, r)
			}
		}

	case lifecycle.StateIdle:
		if connCount == 0 {
			prev := state
			if err := r.Transition(lifecycle.StateWaiting, now); err == nil {
				m.metrics.StateTransition(lifecycle.StateWaiting.String())
				m.bus.Publish(events.Event{
					Type:   events.StateChanged,
					RoomID: r.ID(),
					From:   prev,
					To:     lifecycle.StateWaiting,
					At:     now,
				})
				m.saveRoomSnapshot(m.ctx, r)
			}
		} else if now.Sub(lastAct) > cfg.ExpireTimeout {
			prev := state
			if err := r.Transition(lifecycle.StateExpiring, now); err == nil {
				m.metrics.StateTransition(lifecycle.StateExpiring.String())
				m.bus.Publish(events.Event{
					Type:   events.RoomExpired,
					RoomID: r.ID(),
					From:   prev,
					To:     lifecycle.StateExpiring,
					At:     now,
				})
				m.bus.Publish(events.Event{
					Type:   events.StateChanged,
					RoomID: r.ID(),
					From:   prev,
					To:     lifecycle.StateExpiring,
					At:     now,
				})
				m.saveRoomSnapshot(m.ctx, r)
			}
		}

	case lifecycle.StateExpiring:
		if now.Sub(lastAct) > cfg.ExpireTimeout {
			prev := state
			if err := r.Transition(lifecycle.StateClosed, now); err == nil {
				m.metrics.RoomClosed("idle_timeout")
				m.metrics.StateTransition(lifecycle.StateClosed.String())
				m.bus.Publish(events.Event{
					Type:   events.RoomClosed,
					RoomID: r.ID(),
					From:   prev,
					To:     lifecycle.StateClosed,
					Reason: "idle_timeout",
					At:     now,
				})
				m.saveRoomSnapshot(m.ctx, r)
			}
		}

	case lifecycle.StateClosed:
		retention := m.cfg.DefaultRecoveryTimeout
		if retention > 1*time.Minute {
			retention = 1 * time.Minute
		} else if retention < 5*time.Millisecond {
			retention = 5 * time.Millisecond
		}
		if now.Sub(lastAct) > retention {
			prev := state
			if err := r.Transition(lifecycle.StateDestroyed, now); err == nil {
				m.metrics.RoomDestroyed()
				m.metrics.StateTransition(lifecycle.StateDestroyed.String())
				m.bus.Publish(events.Event{
					Type:   events.RoomDestroyed,
					RoomID: r.ID(),
					From:   prev,
					To:     lifecycle.StateDestroyed,
					At:     now,
				})

				_, _ = m.reg.Deregister(r.ID())

				if m.repo != nil {
					go func(rid string) {
						delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := m.repo.Delete(delCtx, rid); err != nil && !errors.Is(err, storage.ErrNotFound) {
							m.metrics.PersistenceError()
							m.log.Error("failed to delete destroyed room from persistence", "room_id", rid, "err", err)
						}
					}(r.ID())
				}
				m.log.Info("destroyed room and purged resources", "room_id", r.ID())
			}
		}
	}
}

func (m *Manager) saveRoomSnapshot(ctx context.Context, r *room.Room) {
	if m.repo == nil {
		return
	}
	view := r.View()
	snap := storage.Snapshot{
		ID:              view.ID,
		Name:            view.Name,
		OwnerID:         view.OwnerID,
		State:           view.State,
		CreatedAt:       view.CreatedAt,
		LastActivity:    view.LastActivity,
		MaxParticipants: view.Config.MaxParticipants,
		Visibility:      uint8(view.Config.Visibility),
		Metadata:        view.Metadata,
	}
	for _, p := range view.Participants {
		snap.Participants = append(snap.Participants, storage.ParticipantSnapshot{
			UserID:    p.UserID,
			Role:      p.Role,
			JoinedAt:  p.JoinedAt,
			LastSeen:  p.LastSeen,
			Connected: p.Connected,
			Metadata:  p.Metadata,
		})
	}
	go func() {
		saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.repo.Save(saveCtx, snap); err != nil {
			m.metrics.PersistenceError()
			m.log.Error("failed to persist room snapshot", "room_id", view.ID, "err", err)
		}
	}()
}
