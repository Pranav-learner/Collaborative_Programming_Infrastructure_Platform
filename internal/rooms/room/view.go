package room

import (
	"time"

	"cpip/internal/rooms/lifecycle"
)

// View is an immutable, self-contained snapshot of a room at a moment in time.
// It carries only value types (its Participants and Metadata are independent
// copies), so it is safe to hand to any consumer — the public API, the
// persistence layer, presence seeding — without exposing the live room's
// internals or its lock.
type View struct {
	ID           string
	Name         string
	OwnerID      string
	State        lifecycle.State
	CreatedAt    time.Time
	LastActivity time.Time
	Participants []Participant
	Config       Config
	Metadata     map[string]any
}

// ConnectedCount reports how many participants in the snapshot were connected.
func (v View) ConnectedCount() int {
	n := 0
	for _, p := range v.Participants {
		if p.Connected {
			n++
		}
	}
	return n
}
