// Package middleware provides HTTP integration for the observability platform:
// per-request correlation propagation, distributed-trace server spans, structured
// access logging, request metrics, and ready-made /metrics and health handlers.
// It is the glue between an inbound request and the Telemetry SDK, so a service
// gets end-to-end observability by wrapping its handler once.
//
// It depends only on the vendor-neutral sdk.Telemetry facade and the neutral
// framework types — never on an exporter — so the same middleware works whatever
// the platform exports to.
package middleware

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"cpip/internal/observability/correlation"
	"cpip/internal/observability/health"
	"cpip/internal/observability/logging"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/sdk"
	"cpip/internal/observability/tracing"
)

// Middleware instruments HTTP handlers with the Telemetry SDK.
type Middleware struct {
	tel      sdk.Telemetry
	promText func() string
	health   *health.Registry
	log      logging.Logger

	reqTotal    metrics.Counter
	reqDuration metrics.Histogram
	inFlight    metrics.Gauge
}

// Params configures a Middleware.
type Params struct {
	Telemetry sdk.Telemetry
	// PrometheusText renders the metrics exposition for MetricsHandler.
	PrometheusText func() string
	// Health backs the liveness/readiness handlers.
	Health *health.Registry
}

// New constructs a Middleware and registers its request metrics.
func New(p Params) *Middleware {
	m := &Middleware{
		tel: p.Telemetry, promText: p.PrometheusText, health: p.Health,
		log: p.Telemetry.Logger().WithComponent("http"),
	}
	meter := p.Telemetry.Meter()
	m.reqTotal = meter.Counter(metrics.Def{
		Name: "http.requests.total", Help: "HTTP requests", Labels: []string{"method", "route", "status"},
	})
	m.reqDuration = meter.Histogram(metrics.Def{
		Name: "http.request.duration_seconds", Help: "HTTP request duration", Labels: []string{"method", "route"},
	})
	m.inFlight = meter.Gauge(metrics.Def{Name: "http.requests.in_flight", Help: "in-flight HTTP requests"})
	return m
}

// Instrument wraps next with correlation, tracing, logging, and metrics. route is
// a low-cardinality label (a route template like "/jobs/:id", not the raw path)
// to keep metric series bounded.
func (m *Middleware) Instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := r.Context()

		// Extract upstream correlation/trace context, then ensure our own ids.
		incoming := correlation.Extract(headerMap(r.Header))
		ctx = correlation.Update(ctx, incoming)
		ctx, _ = correlation.EnsureCorrelationID(ctx)
		if correlation.From(ctx).RequestID == "" {
			ctx = correlation.Update(ctx, correlation.IDs{RequestID: correlation.NewRequestID()})
		}

		// Server span for the request.
		ctx, span := m.tel.StartSpan(ctx, "HTTP "+r.Method+" "+route, tracing.WithKind(tracing.KindServer),
			tracing.WithAttributes(map[string]any{"http.method": r.Method, "http.route": route, "http.target": r.URL.Path}))
		defer span.End()

		// Propagate correlation back to the client.
		respHeaders := map[string]string{}
		correlation.Inject(correlation.From(ctx), respHeaders)
		for k, v := range respHeaders {
			w.Header().Set(k, v)
		}

		m.inFlight.Inc()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))
		m.inFlight.Dec()

		dur := time.Since(start)
		status := strconv.Itoa(sw.status)
		m.reqTotal.With(metrics.Labels{"method": r.Method, "route": route, "status": status}).Inc()
		m.reqDuration.With(metrics.Labels{"method": r.Method, "route": route}).Observe(dur.Seconds())
		span.SetAttribute("http.status_code", sw.status)
		if sw.status >= 500 {
			span.SetStatus(tracing.StatusError, http.StatusText(sw.status))
		}

		lvl := logging.LevelInfo
		if sw.status >= 500 {
			lvl = logging.LevelError
		} else if sw.status >= 400 {
			lvl = logging.LevelWarn
		}
		m.log.Log(ctx, lvl, "http_request",
			logging.String("method", r.Method), logging.String("route", route),
			logging.Int("status", sw.status), logging.Int64("bytes", sw.written),
			logging.Duration("duration", dur))
	})
}

// MetricsHandler serves the Prometheus exposition format at /metrics.
func (m *Middleware) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if m.promText != nil {
			_, _ = w.Write([]byte(m.promText()))
		}
	})
}

// LivenessHandler serves the liveness probe (200 if up, 503 otherwise).
func (m *Middleware) LivenessHandler() http.Handler {
	return m.healthHandler(func(r *http.Request) health.Snapshot { return m.health.Liveness(r.Context()) })
}

// ReadinessHandler serves the readiness probe (200 if ready, 503 otherwise).
func (m *Middleware) ReadinessHandler() http.Handler {
	return m.healthHandler(func(r *http.Request) health.Snapshot { return m.health.Readiness(r.Context()) })
}

// HealthHandler serves the full component health snapshot.
func (m *Middleware) HealthHandler() http.Handler {
	return m.healthHandler(func(r *http.Request) health.Snapshot { return m.health.CheckAll(r.Context()) })
}

func (m *Middleware) healthHandler(snap func(*http.Request) health.Snapshot) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := snap(r)
		w.Header().Set("Content-Type", "application/json")
		if s.Status == health.StatusUp {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(s)
	})
}

// statusWriter captures the response status code and byte count.
type statusWriter struct {
	http.ResponseWriter
	status  int
	written int64
	wrote   bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.wrote = true
	}
	n, err := w.ResponseWriter.Write(b)
	w.written += int64(n)
	return n, err
}

func headerMap(h http.Header) map[string]string {
	m := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}
	return m
}
