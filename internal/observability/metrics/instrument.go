package metrics

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// --- atomic float64 helper ---

type atomicFloat struct{ bits atomic.Uint64 }

func (a *atomicFloat) add(delta float64) {
	for {
		old := a.bits.Load()
		nv := math.Float64frombits(old) + delta
		if a.bits.CompareAndSwap(old, math.Float64bits(nv)) {
			return
		}
	}
}
func (a *atomicFloat) set(v float64) { a.bits.Store(math.Float64bits(v)) }
func (a *atomicFloat) load() float64 { return math.Float64frombits(a.bits.Load()) }

// Counter is a monotonically-increasing value.
type Counter interface {
	Inc()
	Add(v float64)
	Get() float64
	With(labels Labels) Counter
}

// Gauge is an arbitrary up/down value.
type Gauge interface {
	Set(v float64)
	Add(v float64)
	Sub(v float64)
	Inc()
	Dec()
	Get() float64
	With(labels Labels) Gauge
}

// Histogram observes values into cumulative buckets.
type Histogram interface {
	Observe(v float64)
	With(labels Labels) Histogram
}

// Summary observes values and reports quantiles over a sliding window.
type Summary interface {
	Observe(v float64)
	With(labels Labels) Summary
}

// series holds the accumulated state for one label combination of a metric.
type series struct {
	labels Labels

	// counter/gauge
	value atomicFloat

	// histogram
	bounds     []float64
	bucketHits []atomic.Uint64
	histSum    atomicFloat
	histCount  atomic.Uint64

	// summary
	sumMu    sync.Mutex
	window   []float64
	windowSz int
	ring     int
	sumSum   float64
	sumCount uint64
	quants   []float64
}

func (s *series) observeHist(v float64) {
	// Cumulative buckets: increment the first bound >= v and everything above is
	// derived at gather time.
	idx := sort.SearchFloat64s(s.bounds, v)
	if idx < len(s.bucketHits) {
		s.bucketHits[idx].Add(1)
	} // values > last finite bound fall only into +Inf (tracked via histCount)
	s.histSum.add(v)
	s.histCount.Add(1)
}

func (s *series) observeSummary(v float64) {
	s.sumMu.Lock()
	if len(s.window) < s.windowSz {
		s.window = append(s.window, v)
	} else {
		s.window[s.ring] = v
		s.ring = (s.ring + 1) % s.windowSz
	}
	s.sumSum += v
	s.sumCount++
	s.sumMu.Unlock()
}

func (s *series) snapshotHist() Sample {
	total := s.histCount.Load()
	buckets := make([]Bucket, 0, len(s.bounds)+1)
	var cum uint64
	for i, b := range s.bounds {
		cum += s.bucketHits[i].Load()
		buckets = append(buckets, Bucket{UpperBound: b, Count: cum})
	}
	buckets = append(buckets, Bucket{UpperBound: math.Inf(1), Count: total})
	return Sample{Labels: s.labels, Count: total, Sum: s.histSum.load(), Buckets: buckets}
}

func (s *series) snapshotSummary() Sample {
	s.sumMu.Lock()
	win := append([]float64(nil), s.window...)
	count, sum := s.sumCount, s.sumSum
	s.sumMu.Unlock()
	sort.Float64s(win)
	qs := make([]Quantile, 0, len(s.quants))
	for _, q := range s.quants {
		qs = append(qs, Quantile{Quantile: q, Value: quantile(win, q)})
	}
	return Sample{Labels: s.labels, Count: count, Sum: sum, Quantiles: qs}
}

// quantile returns the q-th quantile of a sorted slice (nearest-rank).
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// --- bound handles (returned by With; also the no-label default handle) ---

type counterHandle struct {
	metric *metric
	series *series
}

func (c counterHandle) Inc() { c.series.value.add(1) }
func (c counterHandle) Add(v float64) {
	if v < 0 {
		return // counters never decrease
	}
	c.series.value.add(v)
}
func (c counterHandle) Get() float64          { return c.series.value.load() }
func (c counterHandle) With(l Labels) Counter { return counterHandle{c.metric, c.metric.seriesFor(l)} }

type gaugeHandle struct {
	metric *metric
	series *series
}

func (g gaugeHandle) Set(v float64)       { g.series.value.set(v) }
func (g gaugeHandle) Add(v float64)       { g.series.value.add(v) }
func (g gaugeHandle) Sub(v float64)       { g.series.value.add(-v) }
func (g gaugeHandle) Inc()                { g.series.value.add(1) }
func (g gaugeHandle) Dec()                { g.series.value.add(-1) }
func (g gaugeHandle) Get() float64        { return g.series.value.load() }
func (g gaugeHandle) With(l Labels) Gauge { return gaugeHandle{g.metric, g.metric.seriesFor(l)} }

type histHandle struct {
	metric *metric
	series *series
}

func (h histHandle) Observe(v float64)       { h.series.observeHist(v) }
func (h histHandle) With(l Labels) Histogram { return histHandle{h.metric, h.metric.seriesFor(l)} }

type summaryHandle struct {
	metric *metric
	series *series
}

func (s summaryHandle) Observe(v float64)     { s.series.observeSummary(v) }
func (s summaryHandle) With(l Labels) Summary { return summaryHandle{s.metric, s.metric.seriesFor(l)} }

// Timer is a Histogram-backed convenience for measuring durations in seconds.
type Timer struct {
	h Histogram
}

// NewTimer wraps a Histogram as a Timer.
func NewTimer(h Histogram) *Timer { return &Timer{h: h} }

// Record observes a duration.
func (t *Timer) Record(d time.Duration) { t.h.Observe(d.Seconds()) }

// Start returns a function that, when called, records the elapsed time. Usage:
//
//	stop := timer.Start(); defer stop()
func (t *Timer) Start() func() {
	start := time.Now()
	return func() { t.h.Observe(time.Since(start).Seconds()) }
}

// With returns a Timer bound to labels.
func (t *Timer) With(l Labels) *Timer { return &Timer{h: t.h.With(l)} }
