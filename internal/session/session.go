// Package session models a single participation window of an authenticated user
// on the platform. A session is created when a connection is established and is
// distinct from the connection itself so that future modules (reconnect/resume,
// room membership, presence) can reason about "who is here" independently of the
// underlying socket.
package session

import (
	"time"

	"cpip/internal/id"
)

// Session is the metadata for one user-participation window. In this module it
// is a thin value carried by a connection; later modules attach room membership
// and presence state keyed by SessionID.
type Session struct {
	// ID uniquely identifies this session for its lifetime.
	ID string
	// UserID is the authenticated principal this session belongs to.
	UserID string
	// CreatedAt is when the session began.
	CreatedAt time.Time
}

// New creates a Session for userID stamped at now.
func New(userID string, now time.Time) *Session {
	return &Session{
		ID:        id.NewWithPrefix("s"),
		UserID:    userID,
		CreatedAt: now,
	}
}
