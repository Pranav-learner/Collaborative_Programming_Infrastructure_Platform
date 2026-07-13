// Package membership handles joining, leaving, kicking, and transferring ownership.
package membership

import (
	"context"
	"log/slog"
	"time"

	"cpip/internal/rooms/events"
	"cpip/internal/rooms/metrics"
	"cpip/internal/rooms/registry"
	"cpip/internal/rooms/room"
	"cpip/internal/rooms/storage"
)

// Manager coordinates membership mutations and orchestrates cross-cutting side effects
// like event publishing, metrics recording, and write-through persistence.
type Manager struct {
	reg     *registry.Registry
	bus     *events.Bus
	metrics metrics.Recorder
	repo    storage.Repository
	log     *slog.Logger
}

// New builds a Manager.
func New(reg *registry.Registry, bus *events.Bus, m metrics.Recorder, repo storage.Repository, log *slog.Logger) *Manager {
	return &Manager{
		reg:     reg,
		bus:     bus,
		metrics: m,
		repo:    repo,
		log:     log,
	}
}

// Join admits a participant to a room, handling reconnection session recovery,
// event publishing, and persistent write-through.
func (m *Manager) Join(ctx context.Context, roomID string, req room.JoinRequest) (room.JoinResult, error) {
	r, ok := m.reg.Get(roomID)
	if !ok {
		return room.JoinResult{}, registry.ErrRoomNotFound
	}

	now := time.Now()
	res, err := r.Join(req, now)
	if err != nil {
		return room.JoinResult{}, err
	}

	if res.Reconnected {
		m.metrics.RecoveryCompleted()
		m.bus.Publish(events.Event{
			Type:    events.RoomRecovered,
			RoomID:  roomID,
			ActorID: req.UserID,
			Role:    res.Participant.Role,
			At:      now,
		})
	} else {
		m.metrics.ParticipantJoined()
		m.bus.Publish(events.Event{
			Type:    events.UserJoined,
			RoomID:  roomID,
			ActorID: req.UserID,
			Role:    res.Participant.Role,
			At:      now,
		})
	}

	if res.StateChanged {
		m.metrics.StateTransition(res.NewState.String())
		m.bus.Publish(events.Event{
			Type:    events.StateChanged,
			RoomID:  roomID,
			ActorID: req.UserID,
			From:    res.PreviousState,
			To:      res.NewState,
			At:      now,
		})
	}

	m.saveRoomSnapshot(ctx, r)
	return res, nil
}

// Leave handles voluntary exit of a participant.
func (m *Manager) Leave(ctx context.Context, roomID string, userID string) (room.LeaveResult, error) {
	r, ok := m.reg.Get(roomID)
	if !ok {
		return room.LeaveResult{}, registry.ErrRoomNotFound
	}

	now := time.Now()
	res, err := r.Leave(userID, now)
	if err != nil {
		return room.LeaveResult{}, err
	}

	m.metrics.ParticipantLeft("voluntary")
	m.bus.Publish(events.Event{
		Type:    events.UserLeft,
		RoomID:  roomID,
		ActorID: userID,
		Role:    res.Participant.Role,
		Reason:  "voluntary",
		At:      now,
	})

	if res.StateChanged {
		m.metrics.StateTransition(res.NewState.String())
		m.bus.Publish(events.Event{
			Type:    events.StateChanged,
			RoomID:  roomID,
			ActorID: userID,
			From:    res.PreviousState,
			To:      res.NewState,
			At:      now,
		})
	}

	m.saveRoomSnapshot(ctx, r)
	return res, nil
}

// Kick forcibly evicts a participant from a room on behalf of another user.
func (m *Manager) Kick(ctx context.Context, roomID string, actorID, targetID string) (room.LeaveResult, error) {
	r, ok := m.reg.Get(roomID)
	if !ok {
		return room.LeaveResult{}, registry.ErrRoomNotFound
	}

	now := time.Now()
	res, err := r.Remove(actorID, targetID, now)
	if err != nil {
		return room.LeaveResult{}, err
	}

	m.metrics.ParticipantLeft("kicked")
	m.bus.Publish(events.Event{
		Type:     events.UserLeft,
		RoomID:   roomID,
		ActorID:  actorID,
		TargetID: targetID,
		Role:     res.Participant.Role,
		Reason:   "kicked",
		At:       now,
	})

	if res.StateChanged {
		m.metrics.StateTransition(res.NewState.String())
		m.bus.Publish(events.Event{
			Type:    events.StateChanged,
			RoomID:  roomID,
			ActorID: actorID,
			From:    res.PreviousState,
			To:      res.NewState,
			At:      now,
		})
	}

	m.saveRoomSnapshot(ctx, r)
	return res, nil
}

// TransferOwnership transfers room ownership to another participant.
func (m *Manager) TransferOwnership(ctx context.Context, roomID string, actorID, newOwnerID string) (room.TransferResult, error) {
	r, ok := m.reg.Get(roomID)
	if !ok {
		return room.TransferResult{}, registry.ErrRoomNotFound
	}

	now := time.Now()
	res, err := r.TransferOwnership(actorID, newOwnerID, now)
	if err != nil {
		return room.TransferResult{}, err
	}

	m.bus.Publish(events.Event{
		Type:     events.OwnerChanged,
		RoomID:   roomID,
		ActorID:  actorID,
		TargetID: newOwnerID,
		At:       now,
	})

	m.saveRoomSnapshot(ctx, r)
	return res, nil
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
