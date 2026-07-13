// Package awareness constructs full and incremental presence synchronization frames.
package awareness

import (
	"cpip/internal/presence/types"
)

// SyncType defines if the frame is a full dump or an incremental update.
type SyncType string

const (
	SyncFull        SyncType = "sync_full"
	SyncIncremental SyncType = "sync_incremental"
)

// Frame carries the awareness payload to be serialized and broadcast to the room.
type Frame struct {
	Type      SyncType         `json:"type"`
	RoomID    string           `json:"room_id"`
	Presences []types.Presence `json:"presences"`
}

// Manager builds awareness frames for distribution to clients.
type Manager struct{}

// New builds an awareness Manager.
func New() *Manager {
	return &Manager{}
}

// BuildFullSync creates a Frame containing all registered presences in a room.
func (m *Manager) BuildFullSync(roomID string, list []types.Presence) Frame {
	return Frame{
		Type:      SyncFull,
		RoomID:    roomID,
		Presences: list,
	}
}

// BuildIncrementalSync creates a Frame containing only modified/updated presences.
func (m *Manager) BuildIncrementalSync(roomID string, updates []types.Presence) Frame {
	return Frame{
		Type:      SyncIncremental,
		RoomID:    roomID,
		Presences: updates,
	}
}
