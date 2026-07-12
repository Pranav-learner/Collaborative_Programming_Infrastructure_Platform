package gateway_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"

	"cpip/internal/api"
	"cpip/internal/auth"
	"cpip/internal/connection"
	"cpip/internal/gateway"
	"cpip/internal/health"
	"cpip/internal/logger"
	"cpip/internal/manager"
	"cpip/internal/metrics"
	"cpip/internal/security"
	"cpip/internal/websocket"
)

// echoHandler echoes every inbound frame back to the sender.
type echoHandler struct{}

func (echoHandler) OnConnect(*connection.Connection) {}
func (echoHandler) OnMessage(c *connection.Connection, msg connection.Inbound) {
	_ = c.Send(msg.Type, msg.Payload)
}
func (echoHandler) OnDisconnect(*connection.Connection, error) {}

type serverOpts struct {
	allowAnon bool
	origins   []string
	maxConns  int
	handler   connection.Handler
}

func newServer(t *testing.T, o serverOpts) (*httptest.Server, *manager.Manager) {
	t.Helper()
	if o.origins == nil {
		o.origins = []string{"*"}
	}
	if o.maxConns == 0 {
		o.maxConns = 100
	}
	if o.handler == nil {
		o.handler = echoHandler{}
	}
	log := logger.Nop()
	mgr := manager.New(manager.Params{Logger: log, Metrics: metrics.NewNoop(), MaxConnections: o.maxConns})
	up := websocket.NewGorillaUpgrader(websocket.UpgraderConfig{
		ReadBufferSize:   4096,
		WriteBufferSize:  4096,
		HandshakeTimeout: 5 * time.Second,
		CheckOrigin:      security.OriginChecker(o.origins, log),
	})
	gw := gateway.New(gateway.Params{
		Upgrader: up,
		Auth:     auth.DummyAuthenticator{AllowAnonymous: o.allowAnon},
		Manager:  mgr,
		Handler:  o.handler,
		ConnConfig: connection.Config{
			HeartbeatInterval: time.Second,
			PongTimeout:       5 * time.Second,
			WriteTimeout:      2 * time.Second,
			MaxPayloadBytes:   1 << 16,
			SendQueueSize:     16,
		},
		Logger:      log,
		Metrics:     metrics.NewNoop(),
		BaseContext: context.Background(),
	})
	router := api.NewRouter(api.Deps{Gateway: gw, Health: health.New(time.Second), Logger: log})
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)
	return ts, mgr
}

func wsURL(ts *httptest.Server, path string) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http") + path
}

func TestGateway_UpgradeAndEcho(t *testing.T) {
	ts, mgr := newServer(t, serverOpts{allowAnon: true})

	c, resp, err := gws.DefaultDialer.Dial(wsURL(ts, "/ws?user_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial: %v (status %v)", err, statusOf(resp))
	}
	defer c.Close()

	if err := c.WriteMessage(gws.TextMessage, []byte("ping-payload")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if mt != gws.TextMessage || string(data) != "ping-payload" {
		t.Fatalf("echo mismatch: mt=%d data=%q", mt, data)
	}

	// The connection should be registered.
	waitCount(t, mgr, 1, 2*time.Second)
}

func TestGateway_AuthRejected(t *testing.T) {
	ts, _ := newServer(t, serverOpts{allowAnon: false})

	_, resp, err := gws.DefaultDialer.Dial(wsURL(ts, "/ws"), nil) // no user id
	if err == nil {
		t.Fatal("expected handshake to fail without credentials")
	}
	if statusOf(resp) != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", statusOf(resp))
	}
}

func TestGateway_OriginRejected(t *testing.T) {
	ts, _ := newServer(t, serverOpts{allowAnon: true, origins: []string{"good.com"}})

	h := http.Header{}
	h.Set("Origin", "http://evil.com")
	_, resp, err := gws.DefaultDialer.Dial(wsURL(ts, "/ws?user_id=x"), h)
	if err == nil {
		t.Fatal("expected handshake to fail for disallowed origin")
	}
	if statusOf(resp) != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", statusOf(resp))
	}
}

func TestGateway_OriginAllowed(t *testing.T) {
	ts, _ := newServer(t, serverOpts{allowAnon: true, origins: []string{"good.com"}})

	h := http.Header{}
	h.Set("Origin", "http://good.com")
	c, resp, err := gws.DefaultDialer.Dial(wsURL(ts, "/ws?user_id=x"), h)
	if err != nil {
		t.Fatalf("dial with allowed origin failed: %v (status %v)", err, statusOf(resp))
	}
	c.Close()
}

func TestGateway_CapacityRejected(t *testing.T) {
	ts, _ := newServer(t, serverOpts{allowAnon: true, maxConns: 1})

	c1, _, err := gws.DefaultDialer.Dial(wsURL(ts, "/ws?user_id=a"), nil)
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	defer c1.Close()

	_, resp, err := gws.DefaultDialer.Dial(wsURL(ts, "/ws?user_id=b"), nil)
	if err == nil {
		t.Fatal("expected second dial to be rejected at capacity")
	}
	if statusOf(resp) != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", statusOf(resp))
	}
}

func TestGateway_GracefulShutdownClosesClient(t *testing.T) {
	ts, mgr := newServer(t, serverOpts{allowAnon: true})

	c, _, err := gws.DefaultDialer.Dial(wsURL(ts, "/ws?user_id=a"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	waitCount(t, mgr, 1, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go mgr.Shutdown(ctx)

	// The client should observe the connection closing.
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("expected read error after server-initiated shutdown")
	}
	waitCount(t, mgr, 0, 3*time.Second)
}

func TestGateway_HealthEndpoints(t *testing.T) {
	ts, _ := newServer(t, serverOpts{allowAnon: true})

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
	}
}

func statusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func waitCount(t *testing.T, m *manager.Manager, want int, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		if m.Count() == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("connection count = %d, want %d", m.Count(), want)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
