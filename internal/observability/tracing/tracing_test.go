package tracing

import (
	"context"
	"errors"
	"sync"
	"testing"

	"cpip/internal/observability/config"
)

type captureExporter struct {
	mu    sync.Mutex
	spans []SpanData
}

func (c *captureExporter) ExportSpans(spans []SpanData) {
	c.mu.Lock()
	c.spans = append(c.spans, spans...)
	c.mu.Unlock()
}
func (c *captureExporter) get() []SpanData {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]SpanData(nil), c.spans...)
}

func newTracer(ratio float64, exp SpanExporter) *Tracer {
	return NewTracer(Params{Config: config.Tracing{SampleRatio: ratio, MaxAttributes: 8, MaxEvents: 8}, Exporter: exp})
}

func TestSpanNestingAndExport(t *testing.T) {
	exp := &captureExporter{}
	tr := newTracer(1, exp)

	ctx, root := tr.StartSpan(context.Background(), "root")
	ctx, child := tr.StartSpan(ctx, "child")
	child.SetAttribute("k", "v")
	child.AddEvent("started", map[string]any{"n": 1})
	child.End()
	root.End()

	spans := exp.get()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	var rootData, childData SpanData
	for _, s := range spans {
		if s.Name == "root" {
			rootData = s
		} else {
			childData = s
		}
	}
	if childData.ParentSpanID != rootData.Context.SpanID {
		t.Fatalf("child parent = %s, want %s", childData.ParentSpanID, rootData.Context.SpanID)
	}
	if childData.Context.TraceID != rootData.Context.TraceID {
		t.Fatalf("child should share the trace id")
	}
	if childData.Attributes["k"] != "v" || len(childData.Events) != 1 {
		t.Fatalf("attributes/events not recorded: %+v", childData)
	}
}

func TestRecordError(t *testing.T) {
	exp := &captureExporter{}
	tr := newTracer(1, exp)
	_, span := tr.StartSpan(context.Background(), "op")
	span.RecordError(errors.New("boom"), nil)
	span.End()
	s := exp.get()[0]
	if s.Status != StatusError {
		t.Fatalf("status = %s, want error", s.Status)
	}
	if len(s.Events) != 1 || s.Events[0].Name != "exception" {
		t.Fatalf("error event not recorded: %+v", s.Events)
	}
}

func TestSamplingOff(t *testing.T) {
	exp := &captureExporter{}
	tr := newTracer(0, exp)
	ctx, span := tr.StartSpan(context.Background(), "unsampled")
	if span.IsRecording() {
		t.Fatal("span should not be recording with ratio 0")
	}
	span.End()
	if len(exp.get()) != 0 {
		t.Fatal("unsampled span should not be exported")
	}
	// But IDs still propagate for correlation.
	if span.TraceID() == "" {
		t.Fatal("trace id should still be assigned for correlation")
	}
	_ = ctx
}

func TestAttributeCap(t *testing.T) {
	exp := &captureExporter{}
	tr := newTracer(1, exp)
	_, span := tr.StartSpan(context.Background(), "capped")
	for i := 0; i < 20; i++ {
		span.SetAttribute(string(rune('a'+i)), i)
	}
	span.End()
	s := exp.get()[0]
	if len(s.Attributes) != 8 {
		t.Fatalf("attributes = %d, want cap 8", len(s.Attributes))
	}
	if s.DroppedAttributes != 12 {
		t.Fatalf("dropped = %d, want 12", s.DroppedAttributes)
	}
}

func TestRegistryStats(t *testing.T) {
	exp := &captureExporter{}
	tr := newTracer(1, exp)
	_, a := tr.StartSpan(context.Background(), "a")
	_, b := tr.StartSpan(context.Background(), "b")
	if st := tr.Registry().Stats(); st.ActiveSpans != 2 {
		t.Fatalf("active spans = %d, want 2", st.ActiveSpans)
	}
	a.End()
	b.End()
	st := tr.Registry().Stats()
	if st.ActiveSpans != 0 || st.FinishedTotal != 2 {
		t.Fatalf("after end: %+v", st)
	}
}

func TestConcurrentSpans(t *testing.T) {
	exp := &captureExporter{}
	tr := newTracer(1, exp)
	const n = 1000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, s := tr.StartSpan(context.Background(), "op")
			s.SetAttribute("x", 1)
			s.End()
		}()
	}
	wg.Wait()
	if len(exp.get()) != n {
		t.Fatalf("concurrent spans lost: got %d want %d", len(exp.get()), n)
	}
}
