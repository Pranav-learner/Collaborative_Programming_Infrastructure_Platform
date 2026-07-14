package bulkhead

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cpip/internal/reliability/config"
	"cpip/internal/reliability/events"
	"cpip/internal/reliability/metrics"
)

// ErrBulkheadFull is returned when execution capacity limits are exceeded.
var ErrBulkheadFull = errors.New("bulkhead execution capacity full; request rejected")

// Bulkhead isolates concurrent workers to avoid cascading platform exhaustion.
type Bulkhead interface {
	Acquire(ctx context.Context) (func(), error)
	Execute(ctx context.Context, fn func() error) error
	Close()
}

// SemaphoreBulkhead limits concurrency using buffered channels.
type SemaphoreBulkhead struct {
	name    string
	sem     chan struct{}
	bus     *events.Bus
	metrics metrics.Recorder
}

func NewSemaphoreBulkhead(name string, maxConcurrent int, bus *events.Bus, rec metrics.Recorder) *SemaphoreBulkhead {
	return &SemaphoreBulkhead{
		name:    name,
		sem:     make(chan struct{}, maxConcurrent),
		bus:     bus,
		metrics: rec,
	}
}

func (b *SemaphoreBulkhead) Acquire(ctx context.Context) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case b.sem <- struct{}{}:
		if b.metrics != nil {
			b.metrics.Inc(metrics.MetricBulkheadActive)
		}
		var once sync.Once
		return func() {
			once.Do(func() {
				<-b.sem
				if b.metrics != nil {
					b.metrics.Dec(metrics.MetricBulkheadActive)
				}
			})
		}, nil
	default:
		if b.metrics != nil {
			b.metrics.Inc(metrics.MetricBulkheadRejections)
		}
		if b.bus != nil {
			b.bus.Publish(events.Event{
				Type:      events.BulkheadRejected,
				Timestamp: time.Now(),
				Policy:    b.name,
				Detail:    fmt.Sprintf("Semaphore bulkhead %q capacity exceeded", b.name),
			})
		}
		return nil, ErrBulkheadFull
	}
}

func (b *SemaphoreBulkhead) Execute(ctx context.Context, fn func() error) error {
	release, err := b.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return fn()
}

func (b *SemaphoreBulkhead) Close() {}

// PoolBulkhead implements job queue worker pool isolation.
type PoolBulkhead struct {
	name    string
	jobs    chan func() error
	results chan error
	wg      sync.WaitGroup
	bus     *events.Bus
	metrics metrics.Recorder
	closed  chan struct{}
	closeOnce sync.Once
}

func NewPoolBulkhead(name string, maxWorkers, queueCapacity int, bus *events.Bus, rec metrics.Recorder) *PoolBulkhead {
	pb := &PoolBulkhead{
		name:    name,
		jobs:    make(chan func() error, queueCapacity),
		results: make(chan error, queueCapacity),
		bus:     bus,
		metrics: rec,
		closed:  make(chan struct{}),
	}

	for i := 0; i < maxWorkers; i++ {
		pb.wg.Add(1)
		go pb.worker()
	}
	return pb
}

func (pb *PoolBulkhead) worker() {
	defer pb.wg.Done()
	for {
		select {
		case <-pb.closed:
			return
		case fn, ok := <-pb.jobs:
			if !ok {
				return
			}
			if pb.metrics != nil {
				pb.metrics.Inc(metrics.MetricBulkheadActive)
			}
			err := fn()
			pb.results <- err
			if pb.metrics != nil {
				pb.metrics.Dec(metrics.MetricBulkheadActive)
			}
		}
	}
}

func (pb *PoolBulkhead) Acquire(ctx context.Context) (func(), error) {
	// Not supported natively for worker pools. Returns dummy or executes in-place.
	return nil, errors.New("acquire operation not supported on worker pools bulkhead; use Execute")
}

func (pb *PoolBulkhead) Execute(ctx context.Context, fn func() error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-pb.closed:
		return errors.New("bulkhead worker pool closed")
	case pb.jobs <- fn:
		// Sent successfully. Now wait for result or cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-pb.results:
			return err
		}
	default:
		if pb.metrics != nil {
			pb.metrics.Inc(metrics.MetricBulkheadRejections)
		}
		if pb.bus != nil {
			pb.bus.Publish(events.Event{
				Type:      events.BulkheadRejected,
				Timestamp: time.Now(),
				Policy:    pb.name,
				Detail:    fmt.Sprintf("Pool bulkhead %q queue capacity exceeded", pb.name),
			})
		}
		return ErrBulkheadFull
	}
}

func (pb *PoolBulkhead) Close() {
	pb.closeOnce.Do(func() {
		close(pb.closed)
		close(pb.jobs)
		pb.wg.Wait()
		close(pb.results)
	})
}

// Factory constructs a Bulkhead from configuration.
func Factory(name string, cfg config.BulkheadConfig, bus *events.Bus, rec metrics.Recorder) Bulkhead {
	if cfg.Type == config.BulkheadPool {
		return NewPoolBulkhead(name, cfg.MaxConcurrent, cfg.QueueCapacity, bus, rec)
	}
	return NewSemaphoreBulkhead(name, cfg.MaxConcurrent, bus, rec)
}
