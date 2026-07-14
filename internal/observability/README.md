# Observability Platform (Stage 5 · Module 1)

The observability platform is the single, vendor-neutral abstraction for all
logging, metrics, tracing, health monitoring, and exporter integration across
CPIP. Business services depend only on the **Telemetry SDK** (`sdk.Telemetry`,
implemented by `telemetry.Provider`) — never on OpenTelemetry, Prometheus, or any
exporter. The default representation is OpenTelemetry (OTLP) for all signals and
Prometheus for metrics, but those are *exporter adapters* behind the seam:
swapping in Datadog, New Relic, CloudWatch, or Azure Monitor is a new adapter, not
a business-logic change.

```
Business Services  (execution · collaboration · sandbox · persistence · coordination)
        │
        ▼
Telemetry SDK  (sdk.Telemetry → telemetry.Provider)   ← the one abstraction
        │
        ├── Logging Framework   (structured, context-aware, sampled)
        ├── Metrics Framework   (counter/gauge/histogram/summary/timer + registry)
        ├── Tracing Framework   (spans, nesting, sampling, trace registry)
        ├── Health Framework    (liveness/readiness/startup, aggregation)
        ├── Correlation Manager (end-to-end IDs, W3C propagation)
        ├── Dashboard Layer     (unified subsystem models)
        └── Alert Framework     (threshold/rate/latency/health/resource)
        │
        ▼
Exporter Framework  (fan-out, async batching, isolation, self-observability)
        │
        ▼
OTLP · Prometheus · Console · No-op   ┊   Datadog · New Relic · CloudWatch · Azure (future)
```

## Packages

| Package | Responsibility |
|---|---|
| `config` | Configuration surface (logging, metrics, tracing, health, exporters, dashboard, alerts) with validation. |
| `registry` | Generic concurrent-safe registry primitive shared by the metrics/trace/logging registries. |
| `correlation` | **Correlation Manager** — the end-to-end ID set (correlation/request/execution/sandbox/worker/session/node/trace/span) + context plumbing + W3C traceparent propagation. |
| `events` | Cluster-wide **event system** (LogEmitted, MetricRecorded, TraceStarted/Finished, HealthChanged, AlertTriggered, ExporterRegistered, DashboardUpdated…). |
| `logger` | The platform's OWN internal diagnostic logger (self-observation; breaks the logging-about-logging recursion). |
| `logging` | **Logging Framework** — levels, structured fields, context enrichment, sampling, pluggable sinks; the Logging Registry of loggers + sinks. |
| `metrics` | **Metrics Framework** — Counter/Gauge/Histogram/Summary/Timer with labels; the concurrent Metrics Registry + pull-based Gather. |
| `tracing` | **Tracing Framework** — Span/Tracer, nested spans, sampling, attributes/events/links/errors; the Trace Registry (active spans, hierarchy, stats). |
| `health` | **Health Framework** — component/dependency checks, liveness/readiness/startup probes, worst-of aggregation, cached snapshots. |
| `exporters` | **Exporter Framework** — the `Exporter` seam + Console/OTLP/Prometheus/No-op adapters + the fan-out Manager (async batching, isolation, self-metrics). |
| `dashboard` | **Dashboard Aggregation Layer** — rolls metrics + health + traces + exporter health into unified, subsystem-grouped models. |
| `alerts` | **Alert Rule Framework** — configuration-driven threshold/error-rate/latency/health/resource rules with a For-duration state machine and a Notifier seam. |
| `sdk` | The **Telemetry SDK** interface (EmitLog, EmitMetric, StartSpan/EndSpan, RecordEvent/Error, RegisterHealthCheck, RegisterMetric …). |
| `telemetry` | Composition root: the `Provider` that wires everything and implements `sdk.Telemetry`. |
| `middleware` | HTTP integration: per-request correlation, server spans, access logs, request metrics, and /metrics + liveness/readiness handlers. |

## Logging workflow

`logger.Info(ctx, msg, fields…)` → enrich with `correlation.From(ctx)` IDs →
level filter → sampler (burst: first N/sec then every Kth) → fan out to every
**Sink**. The exporter Manager is registered as a sink, so the same record reaches
stdout (built-in `WriterSink`) and the export pipeline (OTLP…). An emit hook feeds
per-level throughput into the metrics registry — the platform logs about itself.

## Metrics collection workflow

Instruments are created through the **Meter** (idempotent registration into the
Registry). Counter/Gauge/Histogram are lock-free (atomic); Summary keeps a
mutex-guarded sliding window for quantiles. Labels resolve to per-series state.
Collection is **pull-based**: `Registry.Gather()` snapshots neutral `[]Family`
that any exporter renders — Prometheus scrapes it via `/metrics`; the Manager
pushes it to OTLP on an interval.

## Distributed tracing workflow

`StartSpan(ctx, name)` reads the active span (or an extracted upstream context)
as parent, mints a span id, applies the head-based **Sampler**, and returns a
child span plus a context carrying it *and* the trace/span ids (so logs join the
trace). `span.End()` builds neutral `SpanData` and hands it to the exporter and
the Trace Registry (which tracks active spans and hierarchy). Unsampled spans are
no-ops but still propagate ids, preserving correlation regardless of sampling.

## Correlation ID propagation

One `CorrelationID` spans an entire operation; each hop stamps its own
Request/Execution/Sandbox/Worker/Session/Node id via `correlation.Update(ctx, …)`
without clobbering upstream ids. Across process boundaries, `Inject`/`Extract` use
the **W3C traceparent** header plus a compact correlation header, so CPIP interops
with standard tracing tools — and the middleware does this automatically per HTTP
request, echoing the ids back in the response.

## Health monitoring workflow

Checks register with kinds (liveness/readiness/startup) and a critical flag. A
background runner executes them on an interval with per-check timeouts (a panic or
timeout ⇒ Down, never a crash) and caches results, so probe handlers answer O(1).
Aggregation is worst-of; a non-critical failure **degrades** rather than downs the
service; readiness additionally gates on startup completion. `/livez` and
`/readyz` return 200/503 from the cached snapshot.

## Exporter architecture

The **Manager** is the single fan-out hub: it implements `logging.Sink` and
`tracing.SpanExporter` and pushes metrics on a timer. Logs and spans flow through
bounded **async batchers** (drop-and-account on overflow — the back-pressure
story), then to every registered Exporter with **per-exporter isolation**: one
exporter's failure is counted, marks it unhealthy, emits `ExporterFailed`, and
never affects its peers. The Manager records its own throughput, drops, queue
depth, and per-exporter health as metrics — the platform observes itself. Adapters
shipped: **OTLP** (OpenTelemetry-shaped JSON, the default representation),
**Prometheus** (text exposition, pull), **Console**, **No-op**.

## Concurrency strategy

- Lock-free metric counters/gauges/histogram buckets (atomic) → thousands of
  concurrent records without contention; summaries take a short window lock.
- Every framework's registry is a concurrent-safe generic `Registry[T]`.
- Spans are mutex-guarded per instance and no-op after End (double-End safe).
- The event bus, log sinks, and export queues are best-effort: they drop for slow
  consumers rather than block a hot path.
- Async batchers isolate the emitting goroutine from slow exporters.
- Verified under `go test -race`: 20k concurrent spans+logs+metrics through the
  Provider, 50k concurrent counter increments, 2k concurrent log emits, 1k
  concurrent spans — all with exact accounting and no data races.

## Usage

```go
tel, _ := telemetry.New(telemetry.Params{Config: config.Default()})
_ = tel.Start(ctx); defer tel.Shutdown(ctx)

ctx, ids := tel.Correlate(ctx)              // end-to-end id
ctx, span := tel.StartSpan(ctx, "handle")   // trace
defer span.End()
tel.EmitLog(ctx, logging.LevelInfo, "processing", logging.String("user", ids.CorrelationID))
tel.Meter().Counter(metrics.Def{Name: "app.requests.total"}).Inc()
tel.RegisterHealthCheck(dbCheck, health.Options{Critical: true})

// HTTP wiring
mw := middleware.New(middleware.Params{Telemetry: tel, PrometheusText: tel.PrometheusText, Health: tel.Health()})
mux.Handle("/metrics", mw.MetricsHandler())
mux.Handle("/readyz", mw.ReadinessHandler())
mux.Handle("/api/jobs", mw.Instrument("/api/jobs", jobsHandler))
```

## Future integration points

- **Grafana:** point Grafana at the Prometheus `/metrics` endpoint and the
  Dashboard Layer's models; no code change (dashboards are a deployment concern).
- **Jaeger / Tempo:** the OTLP exporter already emits OTel-shaped spans — swap its
  writer for an OTLP/gRPC transport to a collector that fans out to Jaeger.
- **Datadog / New Relic / CloudWatch / Azure Monitor:** implement the `Exporter`
  interface (ExportLogs/ExportSpans/ExportMetrics) and register it; business logic
  is untouched.
- **OpenTelemetry SDK:** the neutral tracing/metrics/logging models map 1:1 to OTel;
  a bridge exporter can forward to a real OTel SDK pipeline behind the same seam.

Explicitly **out of scope** (later modules / deployment): Grafana dashboards,
Alertmanager integrations, distributed log aggregation, Jaeger deployment, and any
vendor-specific infrastructure. This module builds the platform and abstractions.
