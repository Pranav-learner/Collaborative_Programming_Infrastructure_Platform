package auth

import (
	"net/http"

	"cpip/internal/id"
)

// DummyAuthenticator is the development-stage Authenticator. It trusts a
// client-supplied user identifier taken from the "X-User-ID" header or the
// "user_id" query parameter.
//
// It exists only so the gateway has a working, injectable auth boundary before
// the real identity module lands. It performs NO credential verification and
// MUST NOT be used in production. The production path swaps in a JWT/OAuth
// implementation of Authenticator with zero gateway changes.
type DummyAuthenticator struct {
	// AllowAnonymous mints a random anonymous identity when the request supplies
	// no user id. When false, such requests are rejected with ErrUnauthorized.
	AllowAnonymous bool
}

const (
	headerUserID = "X-User-ID"
	queryUserID  = "user_id"
)

// Authenticate implements Authenticator.
func (d DummyAuthenticator) Authenticate(r *http.Request) (Identity, error) {
	uid := r.Header.Get(headerUserID)
	if uid == "" {
		uid = r.URL.Query().Get(queryUserID)
	}
	if uid == "" {
		if !d.AllowAnonymous {
			return Identity{}, ErrUnauthorized
		}
		uid = "anon-" + id.New()[:12]
	}
	return Identity{UserID: uid}, nil
}

// Compile-time assurance that DummyAuthenticator satisfies Authenticator.
var _ Authenticator = DummyAuthenticator{}
