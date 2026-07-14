package exporters

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cpip/internal/observability/logging"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/tracing"
)

// --- No-op exporter ---

// NoopExporter discards everything. It is the safe default when observability is
// disabled and the zero-overhead baseline for benchmarks.
type NoopExporter struct{}

func (NoopExporter) Name() string                                          { return "noop" }
func (NoopExporter) ExportLogs(context.Context, []logging.Record) error    { return nil }
func (NoopExporter) ExportSpans(context.Context, []tracing.SpanData) error { return nil }
func (NoopExporter) ExportMetrics(context.Context, []metrics.Family) error { return nil }
func (NoopExporter) Shutdown(context.Context) error                        { return nil }

// --- Console exporter (human-readable) ---

// ConsoleExporter writes human-readable telemetry to an io.Writer. It is for
// local development; production uses OTLP/Prometheus.
type ConsoleExporter struct {
	res Resource
	mu  sync.Mutex
	w   io.Writer
}

// NewConsoleExporter constructs a ConsoleExporter over w.
func NewConsoleExporter(w io.Writer, res Resource) *ConsoleExporter {
	return &ConsoleExporter{w: w, res: res}
}

func (c *ConsoleExporter) Name() string { return "console" }

func (c *ConsoleExporter) ExportLogs(_ context.Context, records []logging.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range records {
		fmt.Fprintf(c.w, "[log] %s %-5s %s %s\n", r.Time.UTC().Format(time.RFC3339), strings.ToUpper(r.Level.String()), r.Component, r.Message)
	}
	return nil
}

func (c *ConsoleExporter) ExportSpans(_ context.Context, spans []tracing.SpanData) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range spans {
		fmt.Fprintf(c.w, "[span] %s trace=%s span=%s parent=%s dur=%s status=%s\n",
			s.Name, s.Context.TraceID, s.Context.SpanID, s.ParentSpanID, s.Duration(), s.Status)
	}
	return nil
}

func (c *ConsoleExporter) ExportMetrics(_ context.Context, families []metrics.Family) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range families {
		for _, s := range f.Samples {
			fmt.Fprintf(c.w, "[metric] %s%s %s\n", f.Name, renderLabels(s.Labels), sampleValue(f.Kind, s))
		}
	}
	return nil
}

func (c *ConsoleExporter) Shutdown(context.Context) error { return nil }

func sampleValue(kind metrics.Kind, s metrics.Sample) string {
	switch kind {
	case metrics.KindHistogram, metrics.KindSummary:
		return fmt.Sprintf("count=%d sum=%g", s.Count, s.Sum)
	default:
		return strconv.FormatFloat(s.Value, 'g', -1, 64)
	}
}

// --- OTLP (OpenTelemetry) exporter ---

// OTLPExporter renders telemetry into an OpenTelemetry-shaped JSON envelope and
// writes it to an io.Writer. It represents the platform's DEFAULT telemetry
// implementation (OpenTelemetry) as an on-the-wire format, without importing the
// OTel SDK — so the platform interoperates with an OTLP collector while staying
// vendor-neutral. A future transport (gRPC/HTTP OTLP POST) swaps the writer for a
// network client with no change to callers.
type OTLPExporter struct {
	res Resource
	mu  sync.Mutex
	w   io.Writer
}

// NewOTLPExporter constructs an OTLPExporter writing envelopes to w.
func NewOTLPExporter(w io.Writer, res Resource) *OTLPExporter {
	return &OTLPExporter{w: w, res: res}
}

func (o *OTLPExporter) Name() string { return "otlp" }

type otlpEnvelope struct {
	Resource map[string]string `json:"resource"`
	Signal   string            `json:"signal"`
	Payload  any               `json:"payload"`
	SentAt   string            `json:"sent_at"`
}

func (o *OTLPExporter) write(signal string, payload any) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	env := otlpEnvelope{Resource: o.res.Map(), Signal: signal, Payload: payload, SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	return json.NewEncoder(o.w).Encode(env)
}

func (o *OTLPExporter) ExportLogs(_ context.Context, records []logging.Record) error {
	type logRec struct {
		Time     string         `json:"time"`
		Severity string         `json:"severity"`
		Body     string         `json:"body"`
		Attrs    map[string]any `json:"attributes,omitempty"`
	}
	out := make([]logRec, 0, len(records))
	for _, r := range records {
		attrs := map[string]any{"component": r.Component}
		for _, kv := range r.IDs.Fields() {
			attrs[kv[0]] = kv[1]
		}
		for _, f := range r.Fields {
			attrs[f.Key] = f.Value
		}
		out = append(out, logRec{Time: r.Time.UTC().Format(time.RFC3339Nano), Severity: r.Level.String(), Body: r.Message, Attrs: attrs})
	}
	return o.write("logs", out)
}

func (o *OTLPExporter) ExportSpans(_ context.Context, spans []tracing.SpanData) error {
	return o.write("traces", spans)
}

func (o *OTLPExporter) ExportMetrics(_ context.Context, families []metrics.Family) error {
	return o.write("metrics", families)
}

func (o *OTLPExporter) Shutdown(context.Context) error { return nil }

// --- Prometheus exporter (pull-based text exposition) ---

// PrometheusExporter renders metric families to the Prometheus text exposition
// format. Prometheus is a PULL system: metrics are scraped, so ExportMetrics is a
// no-op and the live data is served by Render / the /metrics handler, which pull
// straight from the registry via the injected gather function. Logs/spans are not
// Prometheus signals (ErrUnsupported).
type PrometheusExporter struct {
	gather func() []metrics.Family
}

// NewPrometheusExporter constructs a PrometheusExporter that pulls from gather
// (typically metrics.Registry.Gather).
func NewPrometheusExporter(gather func() []metrics.Family) *PrometheusExporter {
	return &PrometheusExporter{gather: gather}
}

func (p *PrometheusExporter) Name() string { return "prometheus" }

func (p *PrometheusExporter) ExportLogs(context.Context, []logging.Record) error {
	return ErrUnsupported
}
func (p *PrometheusExporter) ExportSpans(context.Context, []tracing.SpanData) error {
	return ErrUnsupported
}

// ExportMetrics is a no-op: Prometheus scrapes via Render/Handler.
func (p *PrometheusExporter) ExportMetrics(context.Context, []metrics.Family) error { return nil }

func (p *PrometheusExporter) Shutdown(context.Context) error { return nil }

// Render returns the current metrics in Prometheus text exposition format.
func (p *PrometheusExporter) Render() string {
	families := p.gather()
	var b strings.Builder
	for _, f := range families {
		name := sanitize(f.Name)
		if f.Help != "" {
			fmt.Fprintf(&b, "# HELP %s %s\n", name, f.Help)
		}
		fmt.Fprintf(&b, "# TYPE %s %s\n", name, f.Kind)
		for _, s := range f.Samples {
			switch f.Kind {
			case metrics.KindCounter, metrics.KindGauge:
				fmt.Fprintf(&b, "%s%s %s\n", name, promLabels(s.Labels, ""), formatFloat(s.Value))
			case metrics.KindHistogram:
				for _, bkt := range s.Buckets {
					le := "+Inf"
					if !isInf(bkt.UpperBound) {
						le = formatFloat(bkt.UpperBound)
					}
					fmt.Fprintf(&b, "%s_bucket%s %d\n", name, promLabels(s.Labels, "le="+strconv.Quote(le)), bkt.Count)
				}
				fmt.Fprintf(&b, "%s_sum%s %s\n", name, promLabels(s.Labels, ""), formatFloat(s.Sum))
				fmt.Fprintf(&b, "%s_count%s %d\n", name, promLabels(s.Labels, ""), s.Count)
			case metrics.KindSummary:
				for _, q := range s.Quantiles {
					fmt.Fprintf(&b, "%s%s %s\n", name, promLabels(s.Labels, "quantile="+strconv.Quote(formatFloat(q.Quantile))), formatFloat(q.Value))
				}
				fmt.Fprintf(&b, "%s_sum%s %s\n", name, promLabels(s.Labels, ""), formatFloat(s.Sum))
				fmt.Fprintf(&b, "%s_count%s %d\n", name, promLabels(s.Labels, ""), s.Count)
			}
		}
	}
	return b.String()
}

// --- rendering helpers ---

func renderLabels(l metrics.Labels) string {
	if len(l) == 0 {
		return ""
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+l[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func promLabels(l metrics.Labels, extra string) string {
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys)+1)
	for _, k := range keys {
		if l[k] == "" {
			continue
		}
		parts = append(parts, sanitize(k)+"="+strconv.Quote(l[k]))
	}
	if extra != "" {
		parts = append(parts, extra)
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func sanitize(name string) string {
	var b strings.Builder
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_', r == ':':
			b.WriteRune(r)
		case r >= '0' && r <= '9' && i > 0:
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

func isInf(f float64) bool { return f > 1e308 || f < -1e308 }

var (
	_ Exporter = NoopExporter{}
	_ Exporter = (*ConsoleExporter)(nil)
	_ Exporter = (*OTLPExporter)(nil)
	_ Exporter = (*PrometheusExporter)(nil)
)
