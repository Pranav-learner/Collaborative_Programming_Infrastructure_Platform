package metrics

import (
	"sync"
	"testing"

	"cpip/internal/observability/config"
)

func newMeter() *Meter {
	return NewMeter(NewRegistry(), config.Default().Metrics)
}

func TestCounterAndGauge(t *testing.T) {
	m := newMeter()
	c := m.Counter(Def{Name: "reqs", Help: "requests"})
	c.Inc()
	c.Add(4)
	c.Add(-1) // counters ignore negatives
	if c.Get() != 5 {
		t.Fatalf("counter = %g, want 5", c.Get())
	}
	g := m.Gauge(Def{Name: "temp"})
	g.Set(10)
	g.Add(5)
	g.Sub(3)
	g.Dec()
	if g.Get() != 11 {
		t.Fatalf("gauge = %g, want 11", g.Get())
	}
}

func TestLabeledSeries(t *testing.T) {
	m := newMeter()
	c := m.Counter(Def{Name: "hits", Labels: []string{"route"}})
	c.With(Labels{"route": "/a"}).Add(2)
	c.With(Labels{"route": "/b"}).Add(3)
	c.With(Labels{"route": "/a"}).Inc()

	fam := findFamily(m.Registry().Gather(), "hits")
	if fam == nil || len(fam.Samples) != 3 { // /a, /b, and the empty default series
		t.Fatalf("expected 3 series, got %+v", fam)
	}
	var a, b float64
	for _, s := range fam.Samples {
		switch s.Labels["route"] {
		case "/a":
			a = s.Value
		case "/b":
			b = s.Value
		}
	}
	if a != 3 || b != 3 {
		t.Fatalf("labeled values wrong: a=%g b=%g", a, b)
	}
}

func TestHistogram(t *testing.T) {
	m := newMeter()
	h := m.Histogram(Def{Name: "lat", Buckets: []float64{1, 2, 5}})
	for _, v := range []float64{0.5, 1.5, 3, 10} {
		h.Observe(v)
	}
	fam := findFamily(m.Registry().Gather(), "lat")
	s := fam.Samples[0]
	if s.Count != 4 {
		t.Fatalf("count = %d, want 4", s.Count)
	}
	if s.Sum != 15 {
		t.Fatalf("sum = %g, want 15", s.Sum)
	}
	// Cumulative buckets: le=1 →1, le=2 →2, le=5 →3, +Inf →4.
	want := []uint64{1, 2, 3, 4}
	for i, b := range s.Buckets {
		if b.Count != want[i] {
			t.Fatalf("bucket %d count = %d, want %d", i, b.Count, want[i])
		}
	}
}

func TestSummaryQuantiles(t *testing.T) {
	m := newMeter()
	s := m.Summary(Def{Name: "sizes", Objectives: []float64{0.5, 0.99}})
	for i := 1; i <= 100; i++ {
		s.Observe(float64(i))
	}
	fam := findFamily(m.Registry().Gather(), "sizes")
	smp := fam.Samples[0]
	if smp.Count != 100 {
		t.Fatalf("count = %d, want 100", smp.Count)
	}
	q := map[float64]float64{}
	for _, x := range smp.Quantiles {
		q[x.Quantile] = x.Value
	}
	if q[0.5] < 45 || q[0.5] > 55 {
		t.Fatalf("p50 = %g, want ~50", q[0.5])
	}
	if q[0.99] < 95 {
		t.Fatalf("p99 = %g, want ~99", q[0.99])
	}
}

func TestKindConflict(t *testing.T) {
	m := newMeter()
	m.Counter(Def{Name: "x"})
	if _, err := m.TryCounter(Def{Name: "x"}); err != nil {
		t.Fatalf("same-kind re-registration should succeed: %v", err)
	}
	// Requesting a different kind returns a no-op (and TryCounter would error).
	g := m.Gauge(Def{Name: "x"})
	g.Set(5)
	if g.Get() != 0 {
		t.Fatalf("conflicting gauge should be a no-op")
	}
}

func TestTimer(t *testing.T) {
	m := newMeter()
	timer := m.Timer(Def{Name: "op_seconds"})
	stop := timer.Start()
	stop()
	fam := findFamily(m.Registry().Gather(), "op_seconds")
	if fam.Samples[0].Count != 1 {
		t.Fatalf("timer should have recorded 1 observation")
	}
}

func TestConcurrentCounter(t *testing.T) {
	m := newMeter()
	c := m.Counter(Def{Name: "concurrent", Labels: []string{"g"}})
	const goroutines, each = 50, 1000
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h := c.With(Labels{"g": "shared"})
			for j := 0; j < each; j++ {
				h.Inc()
			}
		}(i)
	}
	wg.Wait()
	got := c.With(Labels{"g": "shared"}).Get()
	if got != float64(goroutines*each) {
		t.Fatalf("concurrent counter = %g, want %d", got, goroutines*each)
	}
}

func findFamily(fams []Family, name string) *Family {
	for i := range fams {
		if fams[i].Name == name {
			return &fams[i]
		}
	}
	return nil
}
