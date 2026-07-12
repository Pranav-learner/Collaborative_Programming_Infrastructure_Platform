// Command cpip is the CPIP WebSocket gateway node.
//
// It is the composition root: it loads and validates configuration, constructs
// every subsystem, injects dependencies, starts the HTTP/WebSocket server, and
// coordinates graceful shutdown. There is no global state; everything is wired
// explicitly here so the dependency graph is visible in one place.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cpip/internal/api"
	"cpip/internal/auth"
	"cpip/internal/config"
	"cpip/internal/connection"
	"cpip/internal/gateway"
	"cpip/internal/health"
	"cpip/internal/logger"
	"cpip/internal/manager"
	"cpip/internal/metrics"
	"cpip/internal/ratelimit"
	"cpip/internal/security"
	"cpip/internal/websocket"
)

func main() {
	if err := run(); err != nil {
		// Config/boot errors happen before the logger exists; write to stderr.
		os.Stderr.WriteString("fatal: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	// 1. Configuration (fail-fast).
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// 2. Cross-cutting singletons (injected everywhere).
	log := logger.New(cfg.LogLevel, cfg.LogFormat, os.Stdout)
	rec := metrics.NewNoop() // replaced by the Prometheus recorder in a later module

	log.Info("starting cpip gateway",
		"listen", cfg.ListenAddr,
		"max_connections", cfg.MaxConnections,
		"heartbeat_interval", cfg.HeartbeatInterval.String(),
		"pong_timeout", cfg.PongTimeout.String(),
	)

	// 3. Base context for connections; cancelled last during shutdown.
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	// 4. Subsystems.
	mgr := manager.New(manager.Params{
		Logger:         log,
		Metrics:        rec,
		MaxConnections: cfg.MaxConnections,
	})

	authn := auth.DummyAuthenticator{AllowAnonymous: cfg.AuthAllowAnonymous}

	// The default message handler logs at debug and does nothing else. Later
	// modules (rooms, presence, CRDT relay, execution result streaming) provide
	// a real connection.Handler and inject it here with no other changes.
	handler := connection.NoopHandler{Log: log}

	upgrader := websocket.NewGorillaUpgrader(websocket.UpgraderConfig{
		ReadBufferSize:   cfg.ReadBufferSize,
		WriteBufferSize:  cfg.WriteBufferSize,
		HandshakeTimeout: cfg.HandshakeTimeout,
		CheckOrigin:      security.OriginChecker(cfg.AllowedOrigins, log),
	})

	gw := gateway.New(gateway.Params{
		Upgrader: upgrader,
		Auth:     authn,
		Manager:  mgr,
		Handler:  handler,
		Limiter:  ratelimit.NoopLimiter{},
		ConnConfig: connection.Config{
			HeartbeatInterval: cfg.HeartbeatInterval,
			PongTimeout:       cfg.PongTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			MaxPayloadBytes:   cfg.MaxPayloadBytes,
			SendQueueSize:     cfg.SendQueueSize,
		},
		Logger:      log,
		Metrics:     rec,
		BaseContext: baseCtx,
	})

	healthChecker := health.New(2 * time.Second)

	router := api.NewRouter(api.Deps{
		Gateway: gw,
		Health:  healthChecker,
		Logger:  log,
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           router,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	// 5. Serve. ListenAndServe runs until shutdown; errors other than the
	//    expected ErrServerClosed abort the process.
	serveErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	// 6. Wait for a termination signal or a fatal serve error.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serveErr:
		if err != nil {
			return err
		}
		return nil
	case sig := <-stop:
		log.Info("shutdown signal received", "signal", sig.String())
	}

	// 7. Graceful shutdown.
	//    a) Mark not-ready so the load balancer drains us.
	healthChecker.SetDraining(true)

	shCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	//    b) Stop accepting new HTTP connections/handshakes. The WS handler
	//       returns immediately after starting Serve, so this is quick.
	if err := srv.Shutdown(shCtx); err != nil {
		log.Warn("http server shutdown returned error", "err", err.Error())
	}

	//    c) Gracefully close all live connections and wait for them to drain.
	mgr.Shutdown(shCtx)

	//    d) Cancel the base context as a final safety net.
	cancelBase()

	log.Info("shutdown complete")
	return nil
}
