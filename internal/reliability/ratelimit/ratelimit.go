package ratelimit

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

// ErrRateLimitExceeded is returned when traffic limits are breached.
var ErrRateLimitExceeded = errors.New("rate limit exceeded; request blocked")

// RateLimiter defines the interface for throttling algorithms.
type RateLimiter interface {
	Allow() bool
	Wait(ctx context.Context) error
}

// TokenBucket limits requests using a refilled bucket of tokens.
type TokenBucket struct {
	mu         sync.Mutex
	rate       float64 // tokens per second
	capacity   float64 // burst capacity
	tokens     float64
	lastRefill time.Time
	name       string
	bus        *events.Bus
	metrics    metrics.Recorder
}

func NewTokenBucket(name string, rate float64, burst int, bus *events.Bus, rec metrics.Recorder) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		capacity:   float64(burst),
		tokens:     float64(burst),
		lastRefill: time.Now(),
		name:       name,
		bus:        bus,
		metrics:    rec,
	}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}

	if tb.metrics != nil {
		tb.metrics.Inc(metrics.MetricRateLimitRejections)
	}
	if tb.bus != nil {
		tb.bus.Publish(events.Event{
			Type:      events.RateLimitExceeded,
			Timestamp: now,
			Policy:    tb.name,
			Detail:    fmt.Sprintf("Token Bucket rate limit %q exceeded", tb.name),
		})
	}
	return false
}

func (tb *TokenBucket) Wait(ctx context.Context) error {
	for {
		if tb.Allow() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// LeakyBucket limits traffic by smoothing flow rates.
type LeakyBucket struct {
	mu       sync.Mutex
	capacity float64
	rate     float64 // leaks per second
	water    float64
	lastLeak time.Time
	name     string
	bus      *events.Bus
	metrics  metrics.Recorder
}

func NewLeakyBucket(name string, rate float64, capacity int, bus *events.Bus, rec metrics.Recorder) *LeakyBucket {
	return &LeakyBucket{
		capacity: float64(capacity),
		rate:     rate,
		water:    0,
		lastLeak: time.Now(),
		name:     name,
		bus:      bus,
		metrics:  rec,
	}
}

func (lb *LeakyBucket) Allow() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(lb.lastLeak).Seconds()
	lb.lastLeak = now

	lb.water -= elapsed * lb.rate
	if lb.water < 0 {
		lb.water = 0
	}

	if lb.water+1.0 <= lb.capacity {
		lb.water += 1.0
		return true
	}

	if lb.metrics != nil {
		lb.metrics.Inc(metrics.MetricRateLimitRejections)
	}
	if lb.bus != nil {
		lb.bus.Publish(events.Event{
			Type:      events.RateLimitExceeded,
			Timestamp: now,
			Policy:    lb.name,
			Detail:    fmt.Sprintf("Leaky Bucket rate limit %q exceeded", lb.name),
		})
	}
	return false
}

func (lb *LeakyBucket) Wait(ctx context.Context) error {
	for {
		if lb.Allow() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// SlidingWindow keeps track of individual request timestamps.
type SlidingWindow struct {
	mu          sync.Mutex
	windowSize  time.Duration
	maxRequests int
	timestamps  []time.Time
	name        string
	bus         *events.Bus
	metrics     metrics.Recorder
}

func NewSlidingWindow(name string, windowSize time.Duration, maxRequests int, bus *events.Bus, rec metrics.Recorder) *SlidingWindow {
	return &SlidingWindow{
		windowSize:  windowSize,
		maxRequests: maxRequests,
		timestamps:  make([]time.Time, 0),
		name:        name,
		bus:         bus,
		metrics:     rec,
	}
}

func (sw *SlidingWindow) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-sw.windowSize)

	// Prune old timestamps
	wIdx := 0
	for i, t := range sw.timestamps {
		if t.After(cutoff) {
			wIdx = i
			break
		}
	}
	if len(sw.timestamps) > 0 && sw.timestamps[len(sw.timestamps)-1].Before(cutoff) {
		sw.timestamps = nil
	} else if wIdx > 0 {
		sw.timestamps = sw.timestamps[wIdx:]
	}

	if len(sw.timestamps) < sw.maxRequests {
		sw.timestamps = append(sw.timestamps, now)
		return true
	}

	if sw.metrics != nil {
		sw.metrics.Inc(metrics.MetricRateLimitRejections)
	}
	if sw.bus != nil {
		sw.bus.Publish(events.Event{
			Type:      events.RateLimitExceeded,
			Timestamp: now,
			Policy:    sw.name,
			Detail:    fmt.Sprintf("Sliding Window rate limit %q exceeded", sw.name),
		})
	}
	return false
}

func (sw *SlidingWindow) Wait(ctx context.Context) error {
	for {
		if sw.Allow() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Factory constructs a RateLimiter based on config.
func Factory(name string, cfg config.RateLimitConfig, bus *events.Bus, rec metrics.Recorder) RateLimiter {
	if cfg.Type == config.RateLimitLeakyBucket {
		return NewLeakyBucket(name, cfg.Rate, cfg.Burst, bus, rec)
	} else if cfg.Type == config.RateLimitSlidingWindow {
		return NewSlidingWindow(name, cfg.Interval, cfg.Burst, bus, rec)
	}
	return NewTokenBucket(name, cfg.Rate, cfg.Burst, bus, rec)
}
