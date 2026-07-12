// Package middleware provides HTTP middleware for the gateway's request/response
// surface: correlation-id injection, panic recovery, and structured access
// logging. These wrap the HTTP handshake (and health endpoints); once a request
// is upgraded to a WebSocket the connection package takes over.
//
// The access-log ResponseWriter wrapper deliberately forwards http.Hijacker and
// http.Flusher so the WebSocket upgrade (which hijacks the connection) still
// works when the logging middleware is in the chain.
package middleware

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"time"

	"cpip/internal/id"
)

// RequestIDHeader is the canonical header carrying the correlation id.
const RequestIDHeader = "X-Request-ID"

type ctxKey int

const requestIDKey ctxKey = iota

// RequestID ensures every request carries a correlation id: it reuses an
// inbound X-Request-ID if present, otherwise mints one. The id is stored in the
// request context and echoed in the response header so it can be correlated with
// the per-connection logs downstream.
func RequestID() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rid := r.Header.Get(RequestIDHeader)
			if rid == "" {
				rid = id.NewWithPrefix("req")
			}
			w.Header().Set(RequestIDHeader, rid)
			ctx := context.WithValue(r.Context(), requestIDKey, rid)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequestIDFromContext returns the correlation id stored by RequestID, or "".
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// Recoverer converts a panic in an HTTP handler into a 500 response and a
// logged error, so a single bad request can never crash the process. (The
// connection pumps have their own panic recovery for the post-upgrade path.)
func Recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					// http.ErrAbortHandler is the sanctioned way to abort; re-panic it.
					if rec == http.ErrAbortHandler {
						panic(rec)
					}
					log.Error("http handler panic recovered",
						"panic", rec,
						"path", r.URL.Path,
						"request_id", RequestIDFromContext(r.Context()),
						"stack", string(debug.Stack()),
					)
					w.WriteHeader(http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// AccessLog logs one structured line per HTTP request with method, path, status,
// byte count, duration, and correlation id.
func AccessLog(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			log.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.status,
				"bytes", rw.written,
				"duration", time.Since(start).String(),
				"remote", ClientIP(r),
				"request_id", RequestIDFromContext(r.Context()),
			)
		})
	}
}

// responseRecorder captures status and byte count while transparently forwarding
// the Hijacker and Flusher interfaces required by WebSocket upgrades and
// streaming responses.
type responseRecorder struct {
	http.ResponseWriter
	status  int
	written int
	wrote   bool
}

func (r *responseRecorder) WriteHeader(code int) {
	if !r.wrote {
		r.status = code
		r.wrote = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.wrote = true
	n, err := r.ResponseWriter.Write(b)
	r.written += n
	return n, err
}

// Hijack forwards to the underlying ResponseWriter so gorilla's WebSocket
// upgrade works through this middleware. Without this, the upgrade fails.
func (r *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("middleware: underlying ResponseWriter is not an http.Hijacker")
	}
	return h.Hijack()
}

// Flush forwards to the underlying ResponseWriter if it supports flushing.
func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ClientIP extracts a best-effort client IP, honouring X-Forwarded-For when
// present (the reverse proxy sets it) and falling back to RemoteAddr. It is used
// both for access logging and as the rate-limit key at the gateway.
func ClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// First entry is the original client.
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return trimSpace(xff[:i])
			}
		}
		return trimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
