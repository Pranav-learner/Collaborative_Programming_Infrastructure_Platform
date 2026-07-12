package auth_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"cpip/internal/auth"
)

func TestDummyAuth_HeaderIdentity(t *testing.T) {
	a := auth.DummyAuthenticator{AllowAnonymous: false}
	r := httptest.NewRequest(http.MethodGet, "/ws", nil)
	r.Header.Set("X-User-ID", "alice")

	id, err := a.Authenticate(r)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.UserID != "alice" {
		t.Fatalf("UserID = %q, want alice", id.UserID)
	}
}

func TestDummyAuth_QueryIdentity(t *testing.T) {
	a := auth.DummyAuthenticator{AllowAnonymous: false}
	r := httptest.NewRequest(http.MethodGet, "/ws?user_id=bob", nil)

	id, err := a.Authenticate(r)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.UserID != "bob" {
		t.Fatalf("UserID = %q, want bob", id.UserID)
	}
}

func TestDummyAuth_AnonymousAllowed(t *testing.T) {
	a := auth.DummyAuthenticator{AllowAnonymous: true}
	r := httptest.NewRequest(http.MethodGet, "/ws", nil)

	id, err := a.Authenticate(r)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.UserID == "" {
		t.Fatal("expected a minted anonymous user id")
	}
}

func TestDummyAuth_AnonymousRejected(t *testing.T) {
	a := auth.DummyAuthenticator{AllowAnonymous: false}
	r := httptest.NewRequest(http.MethodGet, "/ws", nil)

	_, err := a.Authenticate(r)
	if !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func TestAuthenticatorFunc(t *testing.T) {
	var a auth.Authenticator = auth.AuthenticatorFunc(func(r *http.Request) (auth.Identity, error) {
		return auth.Identity{UserID: "fn"}, nil
	})
	id, err := a.Authenticate(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != nil || id.UserID != "fn" {
		t.Fatalf("AuthenticatorFunc failed: id=%v err=%v", id, err)
	}
}
