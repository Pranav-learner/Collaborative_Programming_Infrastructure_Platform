package exporters

import (
	"sync"
	"sync/atomic"
	"time"
)

// batcher is a bounded async batching pipeline. Producers call add (non-blocking:
// it drops and accounts on overflow, the module's back-pressure story); a worker
// coalesces items into batches flushed on size or interval. It is the mechanism
// behind "thousands of concurrent signals" without blocking the emitting
// goroutine on a slow exporter.
type batcher[T any] struct {
	queue     chan T
	batchSize int
	flush     time.Duration
	fn        func([]T)
	onDrop    func()
	onDepth   func(int)

	forceFlush chan chan struct{}
	stop       chan struct{}
	wg         sync.WaitGroup
	started    atomic.Bool
	closed     atomic.Bool
}

func newBatcher[T any](queueSize, batchSize int, flush time.Duration, fn func([]T), onDrop func(), onDepth func(int)) *batcher[T] {
	if queueSize <= 0 {
		queueSize = 1024
	}
	if batchSize <= 0 {
		batchSize = 256
	}
	if flush <= 0 {
		flush = time.Second
	}
	return &batcher[T]{
		queue:      make(chan T, queueSize),
		batchSize:  batchSize,
		flush:      flush,
		fn:         fn,
		onDrop:     onDrop,
		onDepth:    onDepth,
		forceFlush: make(chan chan struct{}),
		stop:       make(chan struct{}),
	}
}

func (b *batcher[T]) start() {
	if b.started.Swap(true) {
		return
	}
	b.wg.Add(1)
	go b.run()
}

// add enqueues an item without blocking; on a full queue it drops and accounts.
func (b *batcher[T]) add(item T) {
	if b.closed.Load() {
		return
	}
	select {
	case b.queue <- item:
		if b.onDepth != nil {
			b.onDepth(len(b.queue))
		}
	default:
		if b.onDrop != nil {
			b.onDrop()
		}
	}
}

func (b *batcher[T]) depth() int { return len(b.queue) }

func (b *batcher[T]) run() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.flush)
	defer ticker.Stop()
	batch := make([]T, 0, b.batchSize)

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		out := make([]T, len(batch))
		copy(out, batch)
		batch = batch[:0]
		b.fn(out)
	}
	// drainQueue moves everything currently buffered into batch (non-blocking).
	drainQueue := func() {
		for {
			select {
			case item := <-b.queue:
				batch = append(batch, item)
			default:
				return
			}
		}
	}

	for {
		select {
		case item := <-b.queue:
			batch = append(batch, item)
			if len(batch) >= b.batchSize {
				flushBatch()
			}
		case <-ticker.C:
			flushBatch()
		case ack := <-b.forceFlush:
			drainQueue()
			flushBatch()
			close(ack)
		case <-b.stop:
			drainQueue()
			flushBatch()
			return
		}
	}
}

// flushNow forces a synchronous flush of everything queued so far.
func (b *batcher[T]) flushNow() {
	if !b.started.Load() || b.closed.Load() {
		return
	}
	ack := make(chan struct{})
	select {
	case b.forceFlush <- ack:
		<-ack
	case <-b.stop:
	}
}

// shutdown stops the worker after a final drain. Idempotent.
func (b *batcher[T]) shutdown() {
	if b.closed.Swap(true) {
		return
	}
	if !b.started.Load() {
		return
	}
	close(b.stop)
	b.wg.Wait()
}
