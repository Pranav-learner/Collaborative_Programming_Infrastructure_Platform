package room

import (
	"time"

	"cpip/internal/rooms/permissions"
)

// Participant is a single principal's membership within a room. It is a value
// type: Room stores participants internally and hands out cloned copies through
// its read APIs, so callers can never mutate a room's state by holding a
// Participant.
//
// A Participant models "who is a member", which is distinct from "who has a live
// socket": a member may be Connected == false during the recovery window after a
// disconnect, still holding their role and place in the room until the window
// elapses. This separation is what makes graceful reconnection possible.
type Participant struct {
	// UserID is the authenticated principal identifier (matches auth.Identity).
	UserID string
	// Role is the participant's authority within this room.
	Role permissions.Role
	// SessionID and ConnID bind the participant to their current transport
	// session/connection. They are updated on (re)connect and are informational
	// for higher layers (presence, targeted delivery). Empty while disconnected.
	SessionID string
	ConnID    string
	// JoinedAt is when the participant first joined; preserved across reconnects.
	JoinedAt time.Time
	// LastSeen is the time of the most recent (re)connect or activity.
	LastSeen time.Time
	// Connected reports whether the participant currently has a live connection.
	// False means they are within (or awaiting the end of) their recovery window.
	Connected bool
	// Metadata carries opaque per-participant attributes for higher layers
	// (display name, color, client info). The room treats it as an opaque bag.
	Metadata map[string]any
}

// clone returns a deep-enough copy of p: the Metadata map is duplicated so the
// caller cannot mutate the room's internal copy through the returned value.
func (p Participant) clone() Participant {
	cp := p
	if p.Metadata != nil {
		m := make(map[string]any, len(p.Metadata))
		for k, v := range p.Metadata {
			m[k] = v
		}
		cp.Metadata = m
	}
	return cp
}
