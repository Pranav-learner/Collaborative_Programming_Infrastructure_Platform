// Package id generates cryptographically-random, collision-resistant
// identifiers used for connections, sessions, and correlation IDs.
//
// IDs are 128 bits of CSPRNG entropy rendered as lowercase hex. This is
// deliberately dependency-free so every other package can import it without
// pulling in transitive dependencies.
package id

import (
	"crypto/rand"
	"encoding/hex"
)

// New returns a new 128-bit random identifier as a 32-character hex string.
//
// It reads from crypto/rand. On the supported platforms crypto/rand.Read does
// not fail; if the OS entropy source is genuinely unavailable the process
// cannot operate securely, so we fail loudly rather than return a weak ID.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("id: system entropy source unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// NewWithPrefix returns New() prefixed with p and a hyphen (e.g. "c-9f3a...").
// The prefix aids log readability and lets operators tell IDs apart at a glance.
func NewWithPrefix(p string) string {
	return p + "-" + New()
}
