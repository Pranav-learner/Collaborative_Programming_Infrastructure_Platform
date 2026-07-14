package logging

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/observability/correlation"
)

// captureSink records written records for assertions.
type captureSink struct {
	mu      sync.Mutex
	records []Record
}

func (s *captureSink) Name() string { return "capture" }
func (s *captureSink) Write(r Record) {
	s.mu.Lock()
	s.records = append(s.records, r)
	s.mu.Unlock()
}
func (s *captureSink) Flush() error { return nil }
func (s *captureSink) Close() error { return nil }
func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

func TestLevelFiltering(t *testing.T) {
	cap := &captureSink{}
	m := NewManager(Params{Level: LevelWarn, Sinks: []Sink{cap}})
	log := m.Logger()
	log.Debug(context.Background(), "debug")
	log.Info(context.Background(), "info")
	log.Warn(context.Background(), "warn")
	log.Error(context.Background(), "error")
	if cap.count() != 2 {
		t.Fatalf("expected 2 records (warn+error), got %d", cap.count())
	}
}

func TestContextIDsEnrichment(t *testing.T) {
	cap := &captureSink{}
	m := NewManager(Params{Level: LevelInfo, Sinks: []Sink{cap}})
	ctx := correlation.With(context.Background(), correlation.IDs{CorrelationID: "cid", ExecutionID: "eid"})
	m.Logger().Info(ctx, "hello")
	if cap.records[0].IDs.CorrelationID != "cid" || cap.records[0].IDs.ExecutionID != "eid" {
		t.Fatalf("record not enriched with context IDs: %+v", cap.records[0].IDs)
	}
}

func TestWithFieldsAndComponent(t *testing.T) {
	cap := &captureSink{}
	m := NewManager(Params{Level: LevelInfo, Sinks: []Sink{cap}})
	log := m.Logger().WithComponent("api").With(String("k", "v"))
	log.Info(context.Background(), "msg", Int("n", 5))
	r := cap.records[0]
	if r.Component != "api" {
		t.Fatalf("component = %q", r.Component)
	}
	if len(r.Fields) != 2 || r.Fields[0].Key != "k" || r.Fields[1].Key != "n" {
		t.Fatalf("fields wrong: %+v", r.Fields)
	}
}

func TestSampling(t *testing.T) {
	cap := &captureSink{}
	// Emit first 2 per second, then every 3rd.
	m := NewManager(Params{Level: LevelInfo, Sampler: NewCountSampler(2, 3), Sinks: []Sink{cap}})
	log := m.Logger()
	for i := 0; i < 11; i++ {
		log.Info(context.Background(), "spam")
	}
	// 2 initial + (11-2)=9 → indices 3,6,9 emitted → 2 + 3 = 5.
	if cap.count() != 5 {
		t.Fatalf("sampled count = %d, want 5", cap.count())
	}
}

func TestEmitHook(t *testing.T) {
	cap := &captureSink{}
	m := NewManager(Params{Level: LevelInfo, Sinks: []Sink{cap}})
	var hooked atomic.Int64
	m.SetEmitHook(func(Record) { hooked.Add(1) })
	m.Logger().Info(context.Background(), "x")
	if hooked.Load() != 1 {
		t.Fatalf("emit hook not called")
	}
}

func TestConcurrentLogging(t *testing.T) {
	cap := &captureSink{}
	m := NewManager(Params{Level: LevelInfo, Sinks: []Sink{cap}})
	log := m.Logger()
	const n = 2000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info(context.Background(), "concurrent", Int64("t", time.Now().UnixNano()))
		}()
	}
	wg.Wait()
	if cap.count() != n {
		t.Fatalf("concurrent logging lost records: got %d want %d", cap.count(), n)
	}
}
