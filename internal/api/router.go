// Package api assembles the HTTP surface: middleware chain plus routes for the
// WebSocket endpoint, health probes, and a metrics placeholder. It is the single
// place where the transport wiring is declared, so later modules add routes here
// without touching the gateway.
package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"cpip/internal/gateway"
	"cpip/internal/health"
	"cpip/internal/middleware"
)

// Deps are the dependencies the router wires together.
type Deps struct {
	Gateway *gateway.Gateway
	Health  *health.Checker
	Logger  *slog.Logger
}

// NewRouter builds the top-level HTTP handler.
func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()

	// Order matters: RequestID first (so downstream logs/handlers see the id),
	// then Recoverer (so panics are caught and logged with the id), then access
	// logging.
	r.Use(middleware.RequestID())
	r.Use(middleware.Recoverer(d.Logger))
	r.Use(middleware.AccessLog(d.Logger))

	// Health probes.
	r.Get("/healthz", d.Health.LivenessHandler())
	r.Get("/readyz", d.Health.ReadinessHandler())

	// WebSocket endpoint. A WebSocket handshake is an HTTP GET with upgrade
	// headers; chi routes it like any GET.
	r.Get("/ws", d.Gateway.HandleWS)

	// Metrics placeholder. The observability module replaces this with the
	// Prometheus handler; exposing the path now keeps the contract stable.
	r.Get("/metrics", metricsPlaceholder())

	return r
}

func metricsPlaceholder() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("# metrics endpoint reserved; Prometheus exporter wired in a later module\n"))
	}
}
