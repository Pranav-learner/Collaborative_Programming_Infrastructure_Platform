// Package gateway is the WebSocket entry point. For each inbound handshake it
// runs the admission pipeline and, on success, constructs and starts a
// Connection:
//
//	rate-limit -> authenticate -> capacity check -> upgrade -> build session
//	-> build connection -> register -> go Serve
//
// The HTTP handler returns immediately after starting Serve; the connection's
// lifecycle is independent of the handler (the socket is hijacked). This is why
// graceful shutdown is coordinated through the connection manager rather than
// http.Server.Shutdown alone.
package gateway

import (
	"context"
	"errors"
	"net/http"
	"time"

	"cpip/internal/auth"
	"cpip/internal/connection"
	"cpip/internal/id"
	"cpip/internal/manager"
	"cpip/internal/metrics"
	"cpip/internal/middleware"
	"cpip/internal/ratelimit"
	"cpip/internal/session"
	"cpip/internal/websocket"

	"log/slog"
)

// Params configure a Gateway.
type Params struct {
	Upgrader   websocket.Upgrader
	Auth       auth.Authenticator
	Manager    *manager.Manager
	Handler    connection.Handler
	Limiter    ratelimit.Limiter
	ConnConfig connection.Config
	Logger     *slog.Logger
	Metrics    metrics.Recorder
	// BaseContext is the parent context for every connection. It is cancelled on
	// shutdown, cascading cancellation to all live connections as a safety net
	// alongside the manager's coordinated close.
	BaseContext context.Context
}

// Gateway upgrades HTTP requests to managed WebSocket connections.
type Gateway struct {
	upgrader websocket.Upgrader
	auth     auth.Authenticator
	manager  *manager.Manager
	handler  connection.Handler
	limiter  ratelimit.Limiter
	connCfg  connection.Config
	log      *slog.Logger
	metrics  metrics.Recorder
	baseCtx  context.Context
}

// New builds a Gateway.
func New(p Params) *Gateway {
	base := p.BaseContext
	if base == nil {
		base = context.Background()
	}
	lim := p.Limiter
	if lim == nil {
		lim = ratelimit.NoopLimiter{}
	}
	return &Gateway{
		upgrader: p.Upgrader,
		auth:     p.Auth,
		manager:  p.Manager,
		handler:  p.Handler,
		limiter:  lim,
		connCfg:  p.ConnConfig,
		log:      p.Logger,
		metrics:  p.Metrics,
		baseCtx:  base,
	}
}

// HandleWS is the http.HandlerFunc for the WebSocket endpoint.
func (g *Gateway) HandleWS(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	ip := middleware.ClientIP(r)

	// 1. Rate-limit connection admission (hook; NoopLimiter by default).
	if !g.limiter.Allow(ip) {
		g.metrics.ConnectionRejected("rate_limited")
		g.log.Warn("connection rejected: rate limited", "remote", ip, "request_id", reqID)
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}

	// 2. Authenticate BEFORE upgrading — an unauthenticated request never gets a
	//    socket.
	identity, err := g.auth.Authenticate(r)
	if err != nil {
		g.metrics.ConnectionRejected("unauthorized")
		g.log.Warn("connection rejected: unauthorized", "remote", ip, "request_id", reqID, "err", err.Error())
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// 3. Soft capacity check before spending resources on the upgrade. Register
	//    re-checks authoritatively (this is a TOCTOU-tolerant fast path).
	if g.manager.AtCapacity() {
		g.metrics.ConnectionRejected("at_capacity")
		g.log.Warn("connection rejected: at capacity", "remote", ip, "request_id", reqID)
		http.Error(w, "server at capacity", http.StatusServiceUnavailable)
		return
	}

	// 4. Upgrade. On failure gorilla has already written an HTTP error response.
	wsConn, err := g.upgrader.Upgrade(w, r, nil)
	if err != nil {
		g.metrics.ConnectionRejected("upgrade_failed")
		g.log.Warn("websocket upgrade failed", "remote", ip, "request_id", reqID, "err", err.Error())
		return
	}

	// 5. Build the session and connection.
	now := time.Now()
	sess := session.New(identity.UserID, now)
	connID := id.NewWithPrefix("c")
	c := connection.New(connection.Params{
		ID:        connID,
		Identity:  identity,
		Session:   sess,
		Conn:      wsConn,
		Config:    g.connCfg,
		Logger:    g.log,
		Metrics:   g.metrics,
		Handler:   g.handler,
		Parent:    g.baseCtx,
		RequestID: reqID,
		OnClose:   g.manager.Unregister,
	})

	// 6. Register (authoritative capacity/closed check). On failure, close the
	//    freshly-upgraded socket cleanly without starting pumps.
	if err := g.manager.Register(c); err != nil {
		g.metrics.ConnectionRejected(registerReason(err))
		g.log.Warn("connection registration failed", "conn_id", connID, "request_id", reqID, "err", err.Error())
		c.RejectAndClose(websocket.CloseTryAgainLater, "server unable to accept connection")
		return
	}

	g.log.Info("connection established",
		"conn_id", connID,
		"user_id", identity.UserID,
		"session_id", sess.ID,
		"remote", wsConn.RemoteAddr().String(),
		"request_id", reqID,
	)

	// 7. Run the connection on its own goroutine; the HTTP handler returns now.
	go c.Serve()
}

func registerReason(err error) string {
	switch {
	case errors.Is(err, manager.ErrConnectionLimit):
		return "at_capacity"
	case errors.Is(err, manager.ErrManagerClosed):
		return "shutting_down"
	case errors.Is(err, manager.ErrDuplicate):
		return "duplicate_id"
	default:
		return "register_failed"
	}
}
