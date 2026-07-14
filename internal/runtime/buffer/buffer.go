package buffer

import (
	"bytes"
	"sync"
	"sync/atomic"

	"cpip/internal/runtime/types"
)

// SharedCounter is a thread-safe counter used to track combined output size across stdout and stderr.
type SharedCounter struct {
	value int64
}

// NewSharedCounter creates a new SharedCounter.
func NewSharedCounter() *SharedCounter {
	return &SharedCounter{}
}

// Add adds delta to the counter and returns the new value.
func (c *SharedCounter) Add(delta int64) int64 {
	return atomic.AddInt64(&c.value, delta)
}

// Value returns the current counter value.
func (c *SharedCounter) Value() int64 {
	return atomic.LoadInt64(&c.value)
}

// LimitBuffer is a thread-safe, limit-bounded writer wrapper for capturing output.
type LimitBuffer struct {
	mu           sync.RWMutex
	buf          bytes.Buffer
	limit        int64
	written      int64
	sharedLimit  int64
	sharedCount  *SharedCounter
	onOverflow   func()
	onChunk      func([]byte)
	overflown    bool
}

// NewLimitBuffer creates a new LimitBuffer.
func NewLimitBuffer(
	limit int64,
	sharedLimit int64,
	sharedCount *SharedCounter,
	onOverflow func(),
	onChunk func([]byte),
) *LimitBuffer {
	if sharedCount == nil {
		sharedCount = NewSharedCounter()
	}
	return &LimitBuffer{
		limit:       limit,
		sharedLimit: sharedLimit,
		sharedCount: sharedCount,
		onOverflow:  onOverflow,
		onChunk:     onChunk,
	}
}

// Write appends data to the buffer, checking both individual and shared limits.
// If either limit is exceeded, it writes up to the allowed amount, triggers the overflow callback,
// and returns ErrOutputLimitExceeded.
func (lb *LimitBuffer) Write(p []byte) (n int, err error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if lb.overflown {
		return 0, types.ErrOutputLimitExceeded
	}

	n = len(p)
	if n == 0 {
		return 0, nil
	}

	// 1. Check individual limit
	individualLeft := lb.limit - lb.written
	if individualLeft <= 0 {
		lb.overflown = true
		if lb.onOverflow != nil {
			lb.onOverflow()
		}
		return 0, types.ErrOutputLimitExceeded
	}

	// 2. Check shared limit
	sharedLeft := lb.sharedLimit - lb.sharedCount.Value()
	if sharedLeft <= 0 {
		lb.overflown = true
		if lb.onOverflow != nil {
			lb.onOverflow()
		}
		return 0, types.ErrOutputLimitExceeded
	}

	// Determine the minimum allowed write size
	allowed := int64(n)
	if allowed > individualLeft {
		allowed = individualLeft
	}
	if allowed > sharedLeft {
		allowed = sharedLeft
	}

	if allowed < int64(n) {
		lb.overflown = true
		if allowed > 0 {
			lb.buf.Write(p[:allowed])
			lb.written += allowed
			lb.sharedCount.Add(allowed)
			if lb.onChunk != nil {
				lb.onChunk(p[:allowed])
			}
		}
		if lb.onOverflow != nil {
			lb.onOverflow()
		}
		return int(allowed), types.ErrOutputLimitExceeded
	}

	lb.buf.Write(p)
	lb.written += int64(n)
	lb.sharedCount.Add(int64(n))
	if lb.onChunk != nil {
		lb.onChunk(p)
	}

	return n, nil
}

// String returns the buffer's contents as a string.
func (lb *LimitBuffer) String() string {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.buf.String()
}

// Bytes returns a slice of the buffer's contents.
func (lb *LimitBuffer) Bytes() []byte {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.buf.Bytes()
}

// Len returns the number of bytes written to this buffer.
func (lb *LimitBuffer) Len() int64 {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.written
}
