// Package auth defines the pluggable authentication boundary for the gateway.
//
// Authentication happens at the edge, before the WebSocket upgrade, so an
// unauthenticated request never gets an established socket. The gateway depends
// only on the Authenticator interface; the concrete strategy (dummy today; JWT,
// OAuth/OIDC, or API keys tomorrow) is injected. There is deliberately no
// hard-coded auth logic in the gateway itself.
package auth

import (
	"errors"
	"net/http"
)

// ErrUnauthorized is returned by an Authenticator when a request carries no
// acceptable credential.
var ErrUnauthorized = errors.New("auth: unauthorized")

// Identity is the authenticated principal derived from a request. It is opaque
// to the transport layer and carried inward on the connection.
type Identity struct {
	// UserID is the stable principal identifier. It scopes presence, room
	// membership, and per-user connection lookup.
	UserID string
	// Claims carries additional verified attributes (roles, scopes, token id).
	// It is nil for the dummy authenticator and populated by JWT/OAuth later.
	Claims map[string]any
}

// Authenticator verifies an inbound HTTP request (the WebSocket handshake) and
// returns the authenticated Identity, or an error to reject the connection.
//
// Implementations MUST NOT mutate the request and MUST be safe for concurrent
// use.
type Authenticator interface {
	Authenticate(r *http.Request) (Identity, error)
}

// AuthenticatorFunc adapts a function to the Authenticator interface.
type AuthenticatorFunc func(r *http.Request) (Identity, error)

// Authenticate implements Authenticator.
func (f AuthenticatorFunc) Authenticate(r *http.Request) (Identity, error) { return f(r) }
