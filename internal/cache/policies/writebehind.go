package policies

import (
	"context"
	"sync"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/events"
	"cpip/internal/cache/metrics"
)

// writeOp is a single deferred write to the system of record.
type writeOp struct {
	cache  string
	key    string
	value  string
	writer Writer
}

// writeBehind buffers writes and flushes them asynchronously, coalescing
// repeated writes to the same key (last-writer-wins) to shed load on hot keys.
// The buffer is bounded: when full, the enqueue path degrades to a synchronous
// write so durability is never silently sacrificed to protect memory.
type writeBehind struct {
	cfg config.Policy
	rec metrics.Recorder
	bus *events.Bus

	mu      sync.Mutex
	pending map[string]writeOp // coalesce key = cache+"\x00"+key
	started bool

	stopCh   chan struct{}
	doneCh   chan struct{}
	once     sync.Once
	stopOnce sync.Once
}

func newWriteBehind(cfg config.Policy, rec metrics.Recorder, bus *events.Bus) *writeBehind {
	return &writeBehind{
		cfg:     cfg,
		rec:     rec,
		bus:     bus,
		pending: make(map[string]writeOp),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

func coalesceKey(cache, key string) string { return cache + "\x00" + key }

// ensureStarted launches the flush loop exactly once.
func (w *writeBehind) ensureStarted() {
	w.once.Do(func() {
		w.mu.Lock()
		w.started = true
		w.mu.Unlock()
		go w.loop()
	})
}

// enqueue coalesces op into the pending buffer, or writes synchronously if the
// buffer is at capacity (backpressure that preserves durability).
func (w *writeBehind) enqueue(op writeOp) {
	if op.writer == nil {
		return
	}
	w.mu.Lock()
	if len(w.pending) >= w.cfg.WriteBehindBuffer {
		w.mu.Unlock()
		// Degrade to synchronous write to bound memory without losing data.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = op.writer(ctx, op.key, op.value)
		w.rec.IncCounter(metrics.MetricCacheError, map[string]string{"cache": op.cache, "op": "write_behind_overflow"})
		return
	}
	w.pending[coalesceKey(op.cache, op.key)] = op
	w.mu.Unlock()
}

func (w *writeBehind) loop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.cfg.WriteBehindFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			_ = w.flushAll(context.Background())
			return
		case <-ticker.C:
			_ = w.flushAll(context.Background())
		}
	}
}

// flushAll drains the pending buffer, dispatching writes across a bounded worker
// fan-out. It returns the first error encountered (writes are best-effort;
// individual failures are counted and logged via metrics).
func (w *writeBehind) flushAll(ctx context.Context) error {
	w.mu.Lock()
	if len(w.pending) == 0 {
		w.mu.Unlock()
		return nil
	}
	batch := w.pending
	w.pending = make(map[string]writeOp)
	w.mu.Unlock()

	ops := make([]writeOp, 0, len(batch))
	for _, op := range batch {
		ops = append(ops, op)
	}

	workers := w.cfg.WriteBehindWorkers
	if workers <= 0 {
		workers = 1
	}
	if workers > len(ops) {
		workers = len(ops)
	}

	var (
		wg      sync.WaitGroup
		errMu   sync.Mutex
		firstEr error
		jobs    = make(chan writeOp)
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for op := range jobs {
				if err := op.writer(ctx, op.key, op.value); err != nil {
					errMu.Lock()
					if firstEr == nil {
						firstEr = err
					}
					errMu.Unlock()
					w.rec.IncCounter(metrics.MetricCacheError, map[string]string{"cache": op.cache, "op": "write_behind"})
					continue
				}
				w.rec.IncCounter(metrics.MetricCacheSet, map[string]string{"cache": op.cache, "op": "write_behind"})
			}
		}()
	}
	for _, op := range ops {
		jobs <- op
	}
	close(jobs)
	wg.Wait()
	return firstEr
}

func (w *writeBehind) stop() error {
	w.mu.Lock()
	started := w.started
	w.mu.Unlock()
	if !started {
		return w.flushAll(context.Background())
	}
	w.stopOnce.Do(func() { close(w.stopCh) })
	select {
	case <-w.doneCh:
	case <-time.After(5 * time.Second):
	}
	return nil
}

// --- small concurrency helpers ---

// keySet is a concurrency-safe set used to dedupe in-flight refresh reloads.
type keySet struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newKeySet() *keySet { return &keySet{m: make(map[string]struct{})} }

// add returns true if the key was newly added (caller owns the refresh).
func (k *keySet) add(key string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.m[key]; ok {
		return false
	}
	k.m[key] = struct{}{}
	return true
}

func (k *keySet) remove(key string) {
	k.mu.Lock()
	delete(k.m, key)
	k.mu.Unlock()
}

// registry is a concurrency-safe cache-name → Registration map.
type registry struct {
	mu sync.RWMutex
	m  map[string]Registration
}

func newRegistry() *registry { return &registry{m: make(map[string]Registration)} }

func (r *registry) set(cache string, reg Registration) {
	r.mu.Lock()
	r.m[cache] = reg
	r.mu.Unlock()
}

func (r *registry) get(cache string) (Registration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reg, ok := r.m[cache]
	return reg, ok
}
