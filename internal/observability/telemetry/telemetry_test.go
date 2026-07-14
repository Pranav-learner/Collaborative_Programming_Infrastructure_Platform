package telemetry_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"cpip/internal/observability/alerts"
	"cpip/internal/observability/config"
	"cpip/internal/observability/correlation"
	"cpip/internal/observability/health"
	"cpip/internal/observability/logging"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/sdk"
	"cpip/internal/observability/telemetry"
)

func newProvider(t *testing.T) *telemetry.Provider {
	t.Helper()
	cfg := config.Default()
	cfg.Logging.StdoutSink = false // keep tests quiet
	cfg.Exporters.Enabled = []string{"prometheus", "otlp"}
	cfg.Health.Interval = time.Hour // drive health on demand in tests
	p, err := telemetry.New(telemetry.Params{Config: cfg, OTLPWriter: &bytes.Buffer{}})
	if err != nil {
		t.Fatalf("telemetry.New: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func TestProviderImplementsSDK(t *testing.T) {
	var _ sdk.Telemetry = newProvider(t)
}

func TestEndToEndSignals(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()

	// Correlation flows into logs and spans.
	ctx, ids := p.Correlate(ctx)
	if ids.CorrelationID == "" {
		t.Fatal("correlate should mint a correlation id")
	}

	ctx, span := p.StartSpan(ctx, "operation")
	if correlation.From(ctx).TraceID != span.TraceID() {
		t.Fatal("span trace id should be on the context for log correlation")
	}
	p.RecordEvent(ctx, "checkpoint", map[string]any{"step": 1})
	p.RecordError(ctx, errors.New("bad"), nil)
	p.EndSpan(span)

	p.EmitLog(ctx, logging.LevelInfo, "did work", logging.String("k", "v"))
	p.EmitMetric(ctx, sdk.MetricSample{Name: "app.things.total", Kind: metrics.KindCounter, Value: 3, Labels: metrics.Labels{"kind": "a"}})

	// The metric shows up in the Prometheus exposition.
	text := p.PrometheusText()
	if !strings.Contains(text, "app_things_total") {
		t.Fatalf("emitted metric not in prometheus text:\n%s", text)
	}
	// Self-observability: the platform recorded its own log/span throughput.
	if !strings.Contains(text, "obs_logs_emitted_total") || !strings.Contains(text, "obs_spans_finished_total") {
		t.Fatalf("self-observability metrics missing:\n%s", text)
	}
}

func TestHealthAndDashboard(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	p.RegisterHealthCheck(health.NewCheck("db", func(context.Context) health.Result { return health.Up("ok") }), health.Options{Critical: true})
	p.RegisterHealthCheck(health.NewCheck("cache", func(context.Context) health.Result { return health.Degraded("slow") }), health.Options{Critical: true})

	if p.Health().CheckAll(ctx).Status != health.StatusDegraded {
		t.Fatal("aggregate health should be degraded")
	}
	// Dashboard aggregates metrics into subsystem sections.
	p.EmitMetric(ctx, sdk.MetricSample{Name: "storage.upload.bytes", Kind: metrics.KindCounter, Value: 10})
	d := p.Dashboard(ctx)
	found := false
	for _, s := range d.Sections {
		if s.Key == "storage" {
			found = true
		}
	}
	if !found {
		t.Fatalf("storage metric not classified into a dashboard section: %+v", d.Sections)
	}
}

func TestAlertFires(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	p.Meter().Gauge(metrics.Def{Name: "app.queue.depth"}).Set(500)
	p.AddAlertRule(alerts.Rule{
		Name: "queue_backlog", Kind: alerts.Threshold, Metric: "app.queue.depth",
		Comparator: alerts.GT, Threshold: 100, Severity: alerts.SeverityWarning,
	})
	var fired int
	var mu sync.Mutex
	p.Alerts().AddNotifier(alerts.NotifierFunc(func(_ context.Context, a alerts.Alert) {
		if a.State == alerts.StateFiring {
			mu.Lock()
			fired++
			mu.Unlock()
		}
	}))
	p.Alerts().EvaluateOnce(ctx)
	mu.Lock()
	defer mu.Unlock()
	if fired != 1 {
		t.Fatalf("expected the threshold alert to fire once, got %d", fired)
	}
	if len(p.Alerts().Active()) != 1 {
		t.Fatal("expected one active alert")
	}
}

func TestConcurrentTelemetryStress(t *testing.T) {
	p := newProvider(t)
	counter := p.Meter().Counter(metrics.Def{Name: "app.ops.total", Labels: []string{"worker"}})

	const workers, ops = 40, 500
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx, _ := p.Correlate(context.Background())
			label := metrics.Labels{"worker": fmt.Sprintf("w%d", w%4)}
			for i := 0; i < ops; i++ {
				ctx2, span := p.StartSpan(ctx, "unit")
				p.EmitLog(ctx2, logging.LevelInfo, "tick", logging.Int("i", i))
				counter.With(label).Inc()
				p.EmitMetric(ctx2, sdk.MetricSample{Name: "app.latency", Kind: metrics.KindHistogram, Value: 0.01})
				span.End()
			}
		}(w)
	}
	wg.Wait()

	total := 0.0
	for i := 0; i < 4; i++ {
		total += counter.With(metrics.Labels{"worker": fmt.Sprintf("w%d", i)}).Get()
	}
	if total != float64(workers*ops) {
		t.Fatalf("counter lost increments under concurrency: got %g want %d", total, workers*ops)
	}
}
