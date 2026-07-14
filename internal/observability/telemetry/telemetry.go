// Package telemetry is the composition root of the Observability Platform: it
// constructs and wires every framework (logging, metrics, tracing, correlation,
// health, exporters, dashboard, alerts) from a single Config and exposes them
// behind the sdk.Telemetry facade — the one abstraction the rest of CPIP depends
// on for all logging, metrics, tracing, health, and exporter integration.
//
// It realizes the platform's layering:
//
//	Business Services → Telemetry SDK (Provider) → Logging / Metrics / Tracing / Health → Exporter Layer → OTLP / Prometheus / …
//
// The Provider owns the pipeline wiring: logs and spans flow through the exporter
// Manager (registered as a log sink and span exporter), metrics are pulled by the
// Manager and Prometheus, and the platform observes itself via self-metrics.
package telemetry

import (
	"context"
	"io"
	"os"

	"cpip/internal/id"
	"cpip/internal/observability/alerts"
	"cpip/internal/observability/config"
	"cpip/internal/observability/dashboard"
	"cpip/internal/observability/events"
	"cpip/internal/observability/exporters"
	"cpip/internal/observability/health"
	obslogger "cpip/internal/observability/logger"
	"cpip/internal/observability/logging"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/tracing"
)

// Provider is the wired observability platform implementing sdk.Telemetry.
type Provider struct {
	cfg config.Config
	res exporters.Resource

	bus      *events.Bus
	internal *obslogger.Logger

	logging   *logging.Manager
	metricReg *metrics.Registry
	meter     *metrics.Meter
	traceReg  *tracing.Registry
	tracer    *tracing.Tracer
	health    *health.Registry
	exporters *exporters.Manager
	prom      *exporters.PrometheusExporter
	dashboard *dashboard.Builder
	alerts    *alerts.Evaluator

	// self-observability instruments
	mLogs   metrics.Counter
	mTraces metrics.Counter
}

// Params configures a Provider.
type Params struct {
	Config config.Config
	// Bus / Metrics recorder are created if nil.
	Events *events.Bus
	// InternalLogger backs the platform's own diagnostics (defaults to slog.Default).
	InternalLogger *obslogger.Logger
	// ConsoleWriter is where the console exporter writes (default os.Stdout).
	ConsoleWriter io.Writer
	// OTLPWriter is where the OTLP exporter writes envelopes (default io.Discard —
	// point it at a collector transport in production).
	OTLPWriter io.Writer
	// LogWriter backs the logging stdout sink (default os.Stdout).
	LogWriter io.Writer
	// AlertRules pre-loads alert rules.
	AlertRules []alerts.Rule
}

// New constructs and wires the platform. Call Start to launch background loops.
func New(p Params) (*Provider, error) {
	cfg, err := p.Config.Validate()
	if err != nil {
		return nil, err
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = id.New()
	}

	bus := p.Events
	if bus == nil {
		bus = events.NewBus()
	}
	internal := p.InternalLogger
	if internal == nil {
		internal = obslogger.New(nil)
	}
	res := exporters.Resource{
		ServiceName: cfg.ServiceName,
		Environment: cfg.Environment,
		Version:     cfg.Version,
		InstanceID:  cfg.InstanceID,
	}

	// --- Metrics (registry + meter) ---
	metricReg := metrics.NewRegistry()
	meter := metrics.NewMeter(metricReg, cfg.Metrics)

	// --- Exporter Manager (self-metrics + metric pull) ---
	expMgr := exporters.NewManager(exporters.Params{
		Config: cfg.Exporters, Events: bus, Logger: internal, Meter: meter, Gather: metricReg.Gather,
	})

	// --- Logging ---
	var sampler logging.Sampler = logging.AllSampler{}
	if cfg.Logging.SampleInitial > 0 || cfg.Logging.SampleThereafter > 1 {
		sampler = logging.NewCountSampler(cfg.Logging.SampleInitial, cfg.Logging.SampleThereafter)
	}
	logSinks := []logging.Sink{expMgr} // logs flow to the exporter pipeline
	if cfg.Logging.StdoutSink {
		lw := p.LogWriter
		if lw == nil {
			lw = os.Stdout
		}
		logSinks = append(logSinks, logging.NewWriterSink("stdout", lw, cfg.Logging.JSON))
	}
	logMgr := logging.NewManager(logging.Params{
		Level: logging.ParseLevel(cfg.Logging.Level), Sampler: sampler, Sinks: logSinks,
	})

	// --- Tracing ---
	traceReg := tracing.NewRegistry()
	tracer := tracing.NewTracer(tracing.Params{
		Config: cfg.Tracing, Registry: traceReg, Exporter: expMgr, Resource: res.Map(),
	})

	// --- Health ---
	healthReg := health.NewRegistry(health.Params{Config: cfg.Health, Events: bus, Logger: internal})

	// --- Register configured exporters ---
	prom := registerExporters(cfg, expMgr, metricReg, res, p, internal)

	// --- Dashboard + Alerts ---
	dash := dashboard.New(dashboard.Params{
		Config: cfg, Gather: metricReg.Gather, Health: healthReg, Traces: traceReg, Exporters: expMgr, Events: bus,
	})
	alertEval := alerts.New(alerts.Params{Config: cfg.Alerts, Gather: metricReg.Gather, Health: healthReg, Events: bus, Logger: internal})
	for _, r := range p.AlertRules {
		alertEval.AddRule(r)
	}

	prov := &Provider{
		cfg: cfg, res: res, bus: bus, internal: internal,
		logging: logMgr, metricReg: metricReg, meter: meter,
		traceReg: traceReg, tracer: tracer, health: healthReg,
		exporters: expMgr, prom: prom, dashboard: dash, alerts: alertEval,
	}

	// --- Self-observability wiring ---
	prov.mLogs = meter.Counter(metrics.Def{Name: "obs_logs_emitted_total", Help: "log records emitted", Labels: []string{"level"}})
	prov.mTraces = meter.Counter(metrics.Def{Name: "obs_spans_finished_total", Help: "spans finished"})
	logMgr.SetEmitHook(func(r logging.Record) {
		prov.mLogs.With(metrics.Labels{"level": r.Level.String()}).Inc()
		if r.Level >= logging.LevelWarn {
			bus.Emit(events.LogEmitted, "logging", func(e *events.Event) { e.Name = r.Level.String(); e.Payload = r.Message })
		}
	})
	tracer.SetOnEnd(func(d tracing.SpanData) {
		prov.mTraces.Inc()
		bus.Emit(events.TraceFinished, "tracing", func(e *events.Event) { e.Name = d.Name })
	})

	return prov, nil
}

// registerExporters constructs and registers the exporters named in config,
// returning the Prometheus exporter (if enabled) for handler exposure.
func registerExporters(cfg config.Config, mgr *exporters.Manager, reg *metrics.Registry, res exporters.Resource, p Params, log *obslogger.Logger) *exporters.PrometheusExporter {
	var prom *exporters.PrometheusExporter
	for _, name := range cfg.Exporters.Enabled {
		var exp exporters.Exporter
		switch name {
		case "console":
			w := p.ConsoleWriter
			if w == nil {
				w = os.Stdout
			}
			exp = exporters.NewConsoleExporter(w, res)
		case "otlp":
			w := p.OTLPWriter
			if w == nil {
				w = io.Discard
			}
			exp = exporters.NewOTLPExporter(w, res)
		case "prometheus":
			prom = exporters.NewPrometheusExporter(reg.Gather)
			exp = prom
		case "noop":
			exp = exporters.NoopExporter{}
		default:
			log.Warn(context.Background(), "unknown exporter in config", "exporter", name)
			continue
		}
		if err := mgr.Register(exp); err != nil {
			log.Error(context.Background(), "exporter registration failed", err, "exporter", name)
		}
	}
	return prom
}
