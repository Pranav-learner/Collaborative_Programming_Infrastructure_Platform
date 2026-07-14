package exporters

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"cpip/internal/observability/config"
	"cpip/internal/observability/events"
	obslogger "cpip/internal/observability/logger"
	"cpip/internal/observability/logging"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/tracing"
)

func TestPrometheusRender(t *testing.T) {
	reg := metrics.NewRegistry()
	meter := metrics.NewMeter(reg, config.Default().Metrics)
	meter.Counter(metrics.Def{Name: "obs.requests.total", Help: "reqs", Labels: []string{"route"}}).With(metrics.Labels{"route": "/a"}).Add(3)
	meter.Histogram(metrics.Def{Name: "obs.latency", Buckets: []float64{1, 5}}).Observe(2)

	p := NewPrometheusExporter(reg.Gather)
	out := p.Render()
	if !strings.Contains(out, `obs_requests_total{route="/a"} 3`) {
		t.Fatalf("counter not rendered:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE obs_latency histogram") ||
		!strings.Contains(out, `obs_latency_bucket{le="+Inf"} 1`) ||
		!strings.Contains(out, "obs_latency_count 1") {
		t.Fatalf("histogram not rendered:\n%s", out)
	}
}

func TestOTLPExporterWritesEnvelopes(t *testing.T) {
	var buf bytes.Buffer
	exp := NewOTLPExporter(&buf, Resource{ServiceName: "cpip"})
	_ = exp.ExportLogs(context.Background(), []logging.Record{{Message: "hi", Level: logging.LevelInfo, Time: time.Now()}})
	_ = exp.ExportSpans(context.Background(), []tracing.SpanData{{Name: "s"}})
	out := buf.String()
	if !strings.Contains(out, `"signal":"logs"`) || !strings.Contains(out, `"signal":"traces"`) {
		t.Fatalf("otlp envelopes missing:\n%s", out)
	}
	if !strings.Contains(out, `"service.name":"cpip"`) {
		t.Fatalf("resource not stamped:\n%s", out)
	}
}

// failingExporter fails on demand to exercise isolation + health accounting.
type failingExporter struct{ fail bool }

func (failingExporter) Name() string { return "failing" }
func (f failingExporter) ExportLogs(context.Context, []logging.Record) error {
	if f.fail {
		return context.DeadlineExceeded
	}
	return nil
}
func (failingExporter) ExportSpans(context.Context, []tracing.SpanData) error { return nil }
func (failingExporter) ExportMetrics(context.Context, []metrics.Family) error { return nil }
func (failingExporter) Shutdown(context.Context) error                        { return nil }

func newManager(t *testing.T) (*Manager, *metrics.Registry) {
	reg := metrics.NewRegistry()
	meter := metrics.NewMeter(reg, config.Default().Metrics)
	m := NewManager(Params{
		Config: config.Exporters{QueueSize: 1024, BatchSize: 16, FlushInterval: 20 * time.Millisecond, MetricsPushInterval: time.Hour},
		Events: events.NewBus(), Logger: obslogger.New(nil), Meter: meter, Gather: reg.Gather,
	})
	t.Cleanup(func() { _ = m.Shutdown(context.Background()) })
	return m, reg
}

func TestManagerFanOutAndIsolation(t *testing.T) {
	m, _ := newManager(t)
	var buf bytes.Buffer
	var mu sync.Mutex
	safe := &syncBuf{buf: &buf, mu: &mu}
	_ = m.Register(NewConsoleExporter(safe, Resource{}))
	_ = m.Register(failingExporter{fail: true}) // must not break the console exporter
	m.Start(context.Background())

	for i := 0; i < 5; i++ {
		m.Write(logging.Record{Message: "line", Level: logging.LevelInfo, Time: time.Now()})
	}
	_ = m.Flush()

	mu.Lock()
	got := buf.String()
	mu.Unlock()
	if strings.Count(got, "[log]") != 5 {
		t.Fatalf("console exporter should have received 5 logs despite the failing peer:\n%s", got)
	}
	// The failing exporter should be marked unhealthy with errors accounted.
	var failing *Health
	for _, h := range m.Health() {
		if h.Name == "failing" {
			hh := h
			failing = &hh
		}
	}
	if failing == nil || failing.Healthy || failing.Errors == 0 {
		t.Fatalf("failing exporter health not accounted: %+v", failing)
	}
}

func TestBatcherDropsOnOverflow(t *testing.T) {
	var dropped int
	var mu sync.Mutex
	b := newBatcher(4, 1000, time.Hour, // huge batch + long flush so nothing drains
		func([]int) {},
		func() { mu.Lock(); dropped++; mu.Unlock() },
		nil,
	)
	b.start()
	defer b.shutdown()
	// Do not let the worker run: fill far beyond the queue quickly.
	for i := 0; i < 10000; i++ {
		b.add(i)
	}
	mu.Lock()
	d := dropped
	mu.Unlock()
	if d == 0 {
		t.Fatal("expected overflow drops to be accounted")
	}
}

// syncBuf is a mutex-guarded writer for concurrent console output in tests.
type syncBuf struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}
