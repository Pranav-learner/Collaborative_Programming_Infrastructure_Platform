package redis

import (
	"context"

	"cpip/internal/cache/types"
)

// emSub is the emulator's Subscription implementation. It carries either an
// exact channel set or a pattern set, and delivers matching messages on a
// buffered channel with best-effort (non-blocking) semantics — mirroring how a
// real subscriber that cannot keep up loses messages rather than stalling the
// server.
type emSub struct {
	e        *Emulator
	channels map[string]struct{}
	patterns map[string]struct{}
	out      chan Message
	done     chan struct{}
	closed   bool
}

const emSubBuffer = 1024

func (e *Emulator) newSub(channels, patterns []string) *emSub {
	s := &emSub{
		e:        e,
		channels: make(map[string]struct{}, len(channels)),
		patterns: make(map[string]struct{}, len(patterns)),
		out:      make(chan Message, emSubBuffer),
		done:     make(chan struct{}),
	}
	for _, c := range channels {
		s.channels[c] = struct{}{}
	}
	for _, p := range patterns {
		s.patterns[p] = struct{}{}
	}
	return s
}

// Channel implements Subscription.
func (s *emSub) Channel() <-chan Message { return s.out }

// Close implements Subscription.
func (s *emSub) Close() error {
	s.e.mu.Lock()
	defer s.e.mu.Unlock()
	s.closeLocked()
	delete(s.e.subs, s)
	return nil
}

// closeLocked closes the subscription's channels. Caller must hold e.mu.
func (s *emSub) closeLocked() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.done)
	close(s.out)
}

// deliver enqueues a message if it matches, dropping on overflow. Caller holds e.mu.
func (s *emSub) deliver(channel, pattern, payload string) {
	if s.closed {
		return
	}
	select {
	case s.out <- Message{Channel: channel, Pattern: pattern, Payload: payload}:
	default:
		// Best-effort delivery: drop for a slow subscriber. The pub/sub manager
		// layered above this provides bounded buffering and backpressure policy.
	}
}

// Subscribe implements Client.
func (e *Emulator) Subscribe(_ context.Context, channels ...string) (Subscription, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, types.ErrRedisUnavailable
	}
	s := e.newSub(channels, nil)
	e.subs[s] = struct{}{}
	return s, nil
}

// PSubscribe implements Client.
func (e *Emulator) PSubscribe(_ context.Context, patterns ...string) (Subscription, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, types.ErrRedisUnavailable
	}
	s := e.newSub(nil, patterns)
	e.subs[s] = struct{}{}
	return s, nil
}

// Publish implements Client. Returns the number of subscribers that received
// (or were eligible to receive) the message.
func (e *Emulator) Publish(_ context.Context, channel, message string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, types.ErrRedisUnavailable
	}
	var n int64
	for s := range e.subs {
		if _, ok := s.channels[channel]; ok {
			s.deliver(channel, "", message)
			n++
			continue
		}
		for p := range s.patterns {
			if MatchGlob(p, channel) {
				s.deliver(channel, p, message)
				n++
				break
			}
		}
	}
	return n, nil
}

// MatchGlob reports whether name matches a Redis-style glob pattern supporting
// '*' (any run), '?' (single char), '[...]' character classes, and '\' escapes.
// It is used by both pub/sub pattern routing and ScanKeys.
func MatchGlob(pattern, name string) bool {
	return globMatch(pattern, name)
}

func globMatch(pat, s string) bool {
	// Iterative backtracking matcher — O(len(pat)*len(s)) worst case, no recursion.
	px, sx := 0, 0
	starPx, starSx := -1, -1
	for sx < len(s) {
		if px < len(pat) {
			switch pat[px] {
			case '?':
				px++
				sx++
				continue
			case '*':
				starPx = px
				starSx = sx
				px++
				continue
			case '[':
				if end, ok := matchClass(pat, px, s[sx]); ok {
					px = end
					sx++
					continue
				}
			case '\\':
				if px+1 < len(pat) && pat[px+1] == s[sx] {
					px += 2
					sx++
					continue
				}
			default:
				if pat[px] == s[sx] {
					px++
					sx++
					continue
				}
			}
		}
		if starPx >= 0 {
			// Backtrack: let the last '*' consume one more character.
			px = starPx + 1
			starSx++
			sx = starSx
			continue
		}
		return false
	}
	for px < len(pat) && pat[px] == '*' {
		px++
	}
	return px == len(pat)
}

// matchClass matches a '[...]' character class beginning at pat[px] against c.
// Returns the index just past the class and whether c matched. Supports leading
// '^' negation and 'a-z' ranges.
func matchClass(pat string, px int, c byte) (int, bool) {
	// pat[px] == '['
	i := px + 1
	negate := false
	if i < len(pat) && pat[i] == '^' {
		negate = true
		i++
	}
	matched := false
	for i < len(pat) && pat[i] != ']' {
		if pat[i] == '\\' && i+1 < len(pat) {
			if pat[i+1] == c {
				matched = true
			}
			i += 2
			continue
		}
		if i+2 < len(pat) && pat[i+1] == '-' && pat[i+2] != ']' {
			lo, hi := pat[i], pat[i+2]
			if lo <= c && c <= hi {
				matched = true
			}
			i += 3
			continue
		}
		if pat[i] == c {
			matched = true
		}
		i++
	}
	if i >= len(pat) {
		// Unterminated class: treat '[' literally.
		return px + 1, pat[px] == c
	}
	end := i + 1 // past ']'
	return end, matched != negate
}
