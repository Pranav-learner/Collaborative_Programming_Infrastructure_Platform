// Package security holds transport-level protections applied at the edge of the
// gateway. This module implements Origin validation for the WebSocket handshake;
// payload/connection limits and rate limiting live with the components that
// enforce them (connection, manager, ratelimit).
package security

import (
	"log/slog"
	"net/http"
	"net/url"
	"strings"
)

// OriginChecker returns a func suitable for the WebSocket upgrader's CheckOrigin
// hook. It enforces a strict allow-list to prevent cross-site WebSocket
// hijacking (browsers do not apply the same-origin policy to WebSockets, so the
// server must validate Origin itself).
//
// Rules:
//   - allowed == ["*"]  -> every origin is accepted (development only).
//   - a request with no Origin header is treated as a non-browser client
//     (curl, service, native app) and accepted; browsers always send Origin, so
//     a cross-site page cannot suppress it.
//   - otherwise the Origin's host[:port] must appear in the allow-list.
func OriginChecker(allowed []string, log *slog.Logger) func(r *http.Request) bool {
	allowAll := false
	set := make(map[string]struct{}, len(allowed))
	for _, a := range allowed {
		a = strings.TrimSpace(a)
		if a == "*" {
			allowAll = true
			continue
		}
		set[strings.ToLower(a)] = struct{}{}
	}

	return func(r *http.Request) bool {
		if allowAll {
			return true
		}
		origin := r.Header.Get("Origin")
		if origin == "" {
			// Non-browser client: no Origin to validate.
			return true
		}
		u, err := url.Parse(origin)
		if err != nil {
			if log != nil {
				log.Warn("rejected websocket: unparseable Origin", "origin", origin)
			}
			return false
		}
		host := strings.ToLower(u.Host) // host[:port]
		if _, ok := set[host]; ok {
			return true
		}
		// Also accept a bare-host match (allow-list entry without port).
		if _, ok := set[strings.ToLower(u.Hostname())]; ok {
			return true
		}
		if log != nil {
			log.Warn("rejected websocket: origin not allowed", "origin", origin, "host", host)
		}
		return false
	}
}
