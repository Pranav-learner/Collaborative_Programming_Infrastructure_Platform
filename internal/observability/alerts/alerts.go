// Package alerts implements the Alert Rule Framework: configuration-driven rules
// that evaluate the platform's metrics and health on an interval and fire when a
// condition holds for a sustained duration. It covers threshold, error-rate,
// latency, health, and resource alerts through one Rule model, and publishes
// AlertTriggered / AlertResolved on the event bus. Notification channels (Slack,
// PagerDuty, email) are a future concern behind the Notifier seam — this package
// decides WHAT is wrong, not HOW it is delivered.
package alerts

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cpip/internal/observability/config"
	"cpip/internal/observability/events"
	"cpip/internal/observability/health"
	"cpip/internal/observability/logger"
	"cpip/internal/observability/metrics"
)

// Kind classifies what a rule evaluates.
type Kind string

const (
	// Threshold compares a gauge/counter value to a bound.
	Threshold Kind = "threshold"
	// ErrorRate compares the per-second increase of a counter to a bound.
	ErrorRate Kind = "error_rate"
	// Latency compares a summary quantile (or histogram average) to a bound.
	Latency Kind = "latency"
	// Health fires when a health component (or the aggregate) is not Up.
	HealthKind Kind = "health"
	// Resource compares a resource gauge (memory, queue depth) to a bound.
	Resource Kind = "resource"
)

// Comparator is the breach test.
type Comparator string

const (
	GT  Comparator = "gt"
	GTE Comparator = "gte"
	LT  Comparator = "lt"
	LTE Comparator = "lte"
)

// Severity ranks an alert.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Rule is a single alert definition (configuration-driven).
type Rule struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Kind        Kind           `json:"kind"`
	Metric      string         `json:"metric,omitempty"`
	Labels      metrics.Labels `json:"labels,omitempty"`
	Comparator  Comparator     `json:"comparator"`
	Threshold   float64        `json:"threshold"`
	// Quantile selects the summary quantile for Latency rules (e.g. 0.99).
	Quantile float64 `json:"quantile,omitempty"`
	// Component selects the health component for Health rules ("" = aggregate).
	Component string `json:"component,omitempty"`
	// For requires the condition to hold this long before firing (0 = immediate).
	For      time.Duration `json:"for"`
	Severity Severity      `json:"severity"`
}

// State is a rule's evaluation state.
type State string

const (
	StateInactive State = "inactive"
	StatePending  State = "pending" // breaching but For not yet elapsed
	StateFiring   State = "firing"
)

// Alert is an active alert instance surfaced to notifiers/subscribers.
type Alert struct {
	Rule      string    `json:"rule"`
	Severity  Severity  `json:"severity"`
	State     State     `json:"state"`
	Value     float64   `json:"value"`
	Threshold float64   `json:"threshold"`
	Message   string    `json:"message"`
	Since     time.Time `json:"since"`
	FiredAt   time.Time `json:"fired_at,omitempty"`
}

// Notifier delivers a fired/resolved alert to a channel. Implementations must not
// block; heavy delivery should be async. Future channels (Slack, PagerDuty) plug
// in here without touching evaluation.
type Notifier interface {
	Notify(ctx context.Context, alert Alert)
}

// NotifierFunc adapts a function to a Notifier.
type NotifierFunc func(ctx context.Context, alert Alert)

// Notify implements Notifier.
func (f NotifierFunc) Notify(ctx context.Context, a Alert) { f(ctx, a) }

type ruleState struct {
	rule      Rule
	state     State
	since     time.Time // when the current breach began
	firedAt   time.Time
	lastValue float64
	prevValue float64
	prevAt    time.Time
	hasPrev   bool
}

// Evaluator evaluates rules against the metrics registry and health.
type Evaluator struct {
	cfg    config.Alerts
	gather func() []metrics.Family
	health *health.Registry
	bus    *events.Bus
	log    *logger.Logger
	now    func() time.Time

	mu        sync.Mutex
	rules     map[string]*ruleState
	notifiers []Notifier

	cancel context.CancelFunc
	done   chan struct{}
}

// Params configures an Evaluator.
type Params struct {
	Config config.Alerts
	Gather func() []metrics.Family
	Health *health.Registry
	Events *events.Bus
	Logger *logger.Logger
}

// New constructs an Evaluator.
func New(p Params) *Evaluator {
	return &Evaluator{
		cfg:    p.Config,
		gather: p.Gather,
		health: p.Health,
		bus:    p.Events,
		log:    p.Logger.With("subsystem", "alerts"),
		now:    time.Now,
		rules:  make(map[string]*ruleState),
	}
}

// AddRule registers (or replaces) a rule.
func (e *Evaluator) AddRule(r Rule) {
	e.mu.Lock()
	e.rules[r.Name] = &ruleState{rule: r, state: StateInactive}
	e.mu.Unlock()
}

// AddNotifier registers a notification channel.
func (e *Evaluator) AddNotifier(n Notifier) {
	e.mu.Lock()
	e.notifiers = append(e.notifiers, n)
	e.mu.Unlock()
}

// Active returns all currently firing alerts.
func (e *Evaluator) Active() []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []Alert
	for _, rs := range e.rules {
		if rs.state == StateFiring {
			out = append(out, e.alertOf(rs))
		}
	}
	return out
}

// EvaluateOnce runs one evaluation pass over all rules.
func (e *Evaluator) EvaluateOnce(ctx context.Context) {
	families := map[string]metrics.Family{}
	if e.gather != nil {
		for _, f := range e.gather() {
			families[f.Name] = f
		}
	}
	now := e.now()

	e.mu.Lock()
	rules := make([]*ruleState, 0, len(e.rules))
	for _, rs := range e.rules {
		rules = append(rules, rs)
	}
	e.mu.Unlock()

	for _, rs := range rules {
		value, ok := e.evalValue(ctx, rs, families, now)
		if !ok {
			continue
		}
		breaching := compare(value, rs.rule.Comparator, rs.rule.Threshold)
		e.transition(ctx, rs, breaching, value, now)
	}
}

// evalValue computes the current value for a rule; ok=false when unavailable.
func (e *Evaluator) evalValue(ctx context.Context, rs *ruleState, fams map[string]metrics.Family, now time.Time) (float64, bool) {
	switch rs.rule.Kind {
	case HealthKind:
		if e.health == nil {
			return 0, false
		}
		snap := e.health.CheckAll(ctx)
		status := snap.Status
		if rs.rule.Component != "" {
			c, ok := snap.Components[rs.rule.Component]
			if !ok {
				return 0, false
			}
			status = c.Status
		}
		// Encode: Up=0, Degraded=1, Down/Unknown=2 — compare with Threshold (e.g. >=1).
		return healthLevel(string(status)), true
	case ErrorRate:
		fam, ok := fams[rs.rule.Metric]
		if !ok {
			return 0, false
		}
		cur := sampleScalar(fam, rs.rule)
		rate := 0.0
		if rs.hasPrev {
			dt := now.Sub(rs.prevAt).Seconds()
			if dt > 0 {
				rate = (cur - rs.prevValue) / dt
			}
		}
		rs.prevValue, rs.prevAt, rs.hasPrev = cur, now, true
		return rate, rs.hasPrev
	case Latency:
		fam, ok := fams[rs.rule.Metric]
		if !ok {
			return 0, false
		}
		return sampleQuantile(fam, rs.rule), true
	default: // Threshold, Resource
		fam, ok := fams[rs.rule.Metric]
		if !ok {
			return 0, false
		}
		return sampleScalar(fam, rs.rule), true
	}
}

// transition advances a rule's state machine and fires/resolves as needed.
func (e *Evaluator) transition(ctx context.Context, rs *ruleState, breaching bool, value float64, now time.Time) {
	e.mu.Lock()
	rs.lastValue = value
	prevState := rs.state
	switch {
	case breaching && rs.state == StateInactive:
		rs.state = StatePending
		rs.since = now
		if rs.rule.For <= 0 {
			rs.state = StateFiring
			rs.firedAt = now
		}
	case breaching && rs.state == StatePending:
		if now.Sub(rs.since) >= rs.rule.For {
			rs.state = StateFiring
			rs.firedAt = now
		}
	case !breaching && rs.state != StateInactive:
		rs.state = StateInactive
		rs.since = time.Time{}
	}
	newState := rs.state
	alert := e.alertOf(rs)
	notifiers := append([]Notifier(nil), e.notifiers...)
	e.mu.Unlock()

	if prevState != StateFiring && newState == StateFiring {
		e.log.Slog().Warn("alert_triggered", "rule", rs.rule.Name, "severity", string(rs.rule.Severity), "value", value, "threshold", rs.rule.Threshold)
		e.bus.Emit(events.AlertTriggered, "alerts", func(ev *events.Event) { ev.Name = rs.rule.Name; ev.Payload = alert })
		for _, n := range notifiers {
			n.Notify(ctx, alert)
		}
	} else if prevState == StateFiring && newState == StateInactive {
		e.log.Slog().Info("alert_resolved", "rule", rs.rule.Name)
		e.bus.Emit(events.AlertResolved, "alerts", func(ev *events.Event) { ev.Name = rs.rule.Name; ev.Payload = alert })
		for _, n := range notifiers {
			n.Notify(ctx, alert)
		}
	}
}

func (e *Evaluator) alertOf(rs *ruleState) Alert {
	return Alert{
		Rule:      rs.rule.Name,
		Severity:  rs.rule.Severity,
		State:     rs.state,
		Value:     rs.lastValue,
		Threshold: rs.rule.Threshold,
		Message:   fmt.Sprintf("%s: %s %s %g (value=%g)", rs.rule.Name, rs.rule.Metric, rs.rule.Comparator, rs.rule.Threshold, rs.lastValue),
		Since:     rs.since,
		FiredAt:   rs.firedAt,
	}
}

// Start launches the periodic evaluation loop.
func (e *Evaluator) Start(ctx context.Context) {
	if e.done != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	e.cancel = cancel
	e.done = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(e.cfg.EvalInterval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				e.EvaluateOnce(loopCtx)
			}
		}
	}()
}

// Stop halts the evaluation loop.
func (e *Evaluator) Stop() {
	cancel, done := e.cancel, e.done
	e.cancel = nil
	e.done = nil
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// --- helpers ---

func compare(value float64, cmp Comparator, threshold float64) bool {
	switch cmp {
	case GT:
		return value > threshold
	case GTE:
		return value >= threshold
	case LT:
		return value < threshold
	case LTE:
		return value <= threshold
	default:
		return false
	}
}

// sampleScalar returns the counter/gauge value (or hist/summary sum) of the
// series matching the rule's labels.
func sampleScalar(f metrics.Family, r Rule) float64 {
	s, ok := matchSample(f, r.Labels)
	if !ok {
		return 0
	}
	switch f.Kind {
	case metrics.KindHistogram, metrics.KindSummary:
		if s.Count > 0 {
			return s.Sum / float64(s.Count) // average as the scalar
		}
		return 0
	default:
		return s.Value
	}
}

// sampleQuantile returns the requested quantile from a summary series, or the
// histogram average if the metric is a histogram.
func sampleQuantile(f metrics.Family, r Rule) float64 {
	s, ok := matchSample(f, r.Labels)
	if !ok {
		return 0
	}
	if f.Kind == metrics.KindSummary {
		for _, q := range s.Quantiles {
			if q.Quantile == r.Quantile {
				return q.Value
			}
		}
	}
	if s.Count > 0 {
		return s.Sum / float64(s.Count)
	}
	return 0
}

func matchSample(f metrics.Family, want metrics.Labels) (metrics.Sample, bool) {
	if len(want) == 0 {
		if len(f.Samples) > 0 {
			return f.Samples[0], true
		}
		return metrics.Sample{}, false
	}
	for _, s := range f.Samples {
		if labelsMatch(s.Labels, want) {
			return s, true
		}
	}
	return metrics.Sample{}, false
}

func labelsMatch(have, want metrics.Labels) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func healthLevel(status string) float64 {
	switch status {
	case "up":
		return 0
	case "degraded":
		return 1
	default: // down, unknown
		return 2
	}
}
