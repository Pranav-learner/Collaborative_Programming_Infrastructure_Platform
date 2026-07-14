package backoff

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// Strategy calculates delay intervals between retry attempts.
type Strategy interface {
	NextDelay(attempt int, baseInterval, maxInterval time.Duration, lastDelay time.Duration) time.Duration
}

// SafeRand provides a thread-safe random generator.
type SafeRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func NewSafeRand() *SafeRand {
	return &SafeRand{
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sr *SafeRand) Intn(n int) int {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.r.Intn(n)
}

func (sr *SafeRand) Float64() float64 {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.r.Float64()
}

var globalRand = NewSafeRand()

// FixedStrategy returns a constant delay.
type FixedStrategy struct{}

func (s *FixedStrategy) NextDelay(_ int, base, _, _ time.Duration) time.Duration {
	return base
}

// LinearStrategy returns delay proportional to attempt.
type LinearStrategy struct{}

func (s *LinearStrategy) NextDelay(attempt int, base, max, _ time.Duration) time.Duration {
	delay := base * time.Duration(attempt)
	if delay > max {
		return max
	}
	return delay
}

// ExponentialStrategy multiplies interval base exponentially.
type ExponentialStrategy struct{}

func (s *ExponentialStrategy) NextDelay(attempt int, base, max, _ time.Duration) time.Duration {
	factor := math.Pow(2, float64(attempt-1))
	delay := time.Duration(float64(base) * factor)
	if delay > max || delay <= 0 {
		return max
	}
	return delay
}

// ExponentialJitterStrategy adds full random jitter to the exponential calculation.
type ExponentialJitterStrategy struct {
	exp ExponentialStrategy
}

func (s *ExponentialJitterStrategy) NextDelay(attempt int, base, max, last time.Duration) time.Duration {
	fullExp := s.exp.NextDelay(attempt, base, max, last)
	if fullExp <= 0 {
		return base
	}
	jitter := time.Duration(globalRand.Float64() * float64(fullExp))
	if jitter < base {
		return base
	}
	return jitter
}

// DecorrelatedJitterStrategy calculates delay based on previous delay intervals.
type DecorrelatedJitterStrategy struct{}

func (s *DecorrelatedJitterStrategy) NextDelay(_ int, base, max, last time.Duration) time.Duration {
	if last <= 0 {
		last = base
	}
	// Formula: min(max, rand(base, last * 3))
	minRange := int64(base)
	maxRange := int64(last * 3)
	if maxRange <= minRange {
		maxRange = minRange + 1
	}

	diff := maxRange - minRange
	randVal := minRange + int64(globalRand.Float64()*float64(diff))
	delay := time.Duration(randVal)

	if delay > max {
		return max
	}
	if delay < base {
		return base
	}
	return delay
}
