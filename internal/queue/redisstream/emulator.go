package redisstream

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cpip/internal/queue/types"
)

// Emulator is an in-memory Client with faithful Redis-Streams consumer-group
// semantics: monotonic entry IDs, per-group pending-entries lists with delivery
// counts and idle time, blocking reads, and idle-based auto-claim. It is the
// backbone of the test suite and a valid development backend.
type Emulator struct {
	mu      sync.Mutex
	streams map[string]*emStream
	ms      int64
	seq     int64
	closed  bool
	// now is overridable for deterministic idle-time tests.
	now func() time.Time
}

type emStream struct {
	entries []*emEntry
	index   map[string]*emEntry
	groups  map[string]*emGroup
	waiters []chan struct{}
}

type emEntry struct {
	id      string
	fields  map[string]string
	deleted bool
}

type emGroup struct {
	lastDelivered string
	pending       map[string]*emPending
}

type emPending struct {
	id          string
	consumer    string
	deliveredAt time.Time
	count       int64
}

// NewEmulator constructs an in-memory stream client.
func NewEmulator() *Emulator {
	return &Emulator{streams: make(map[string]*emStream), now: time.Now}
}

// SetClock overrides the emulator clock (tests). Not safe to call concurrently
// with other operations.
func (e *Emulator) SetClock(now func() time.Time) { e.now = now }

func (e *Emulator) stream(name string) *emStream {
	s, ok := e.streams[name]
	if !ok {
		s = &emStream{index: make(map[string]*emEntry), groups: make(map[string]*emGroup)}
		e.streams[name] = s
	}
	return s
}

func (e *Emulator) nextID() string {
	now := e.now().UnixMilli()
	if now > e.ms {
		e.ms = now
		e.seq = 0
	} else {
		e.seq++
	}
	return fmt.Sprintf("%d-%d", e.ms, e.seq)
}

func parseID(id string) (int64, int64) {
	dash := strings.IndexByte(id, '-')
	if dash < 0 {
		ms, _ := strconv.ParseInt(id, 10, 64)
		return ms, 0
	}
	ms, _ := strconv.ParseInt(id[:dash], 10, 64)
	seq, _ := strconv.ParseInt(id[dash+1:], 10, 64)
	return ms, seq
}

// idLess reports whether a < b under Redis stream ID ordering.
func idLess(a, b string) bool {
	am, as := parseID(a)
	bm, bs := parseID(b)
	if am != bm {
		return am < bm
	}
	return as < bs
}

// Add implements Client.
func (e *Emulator) Add(_ context.Context, stream string, fields map[string]string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return "", types.ErrRedisUnavailable
	}
	s := e.stream(stream)
	id := e.nextID()
	entry := &emEntry{id: id, fields: copyFields(fields)}
	s.entries = append(s.entries, entry)
	s.index[id] = entry
	s.wake()
	return id, nil
}

// CreateGroup implements Client.
func (e *Emulator) CreateGroup(_ context.Context, stream, group, start string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return types.ErrRedisUnavailable
	}
	s := e.stream(stream)
	if _, ok := s.groups[group]; ok {
		return nil // benign: group already exists
	}
	last := "0-0"
	if start == "$" && len(s.entries) > 0 {
		last = s.entries[len(s.entries)-1].id
	}
	s.groups[group] = &emGroup{lastDelivered: last, pending: make(map[string]*emPending)}
	return nil
}

// ReadGroup implements Client, honoring ">" (new-message) semantics and blocking.
func (e *Emulator) ReadGroup(ctx context.Context, args ReadGroupArgs) ([]Entry, error) {
	deadline := e.now().Add(args.Block)
	for {
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return nil, types.ErrRedisUnavailable
		}
		s, ok := e.streams[args.Stream]
		if !ok {
			e.mu.Unlock()
			return nil, fmt.Errorf("%w: no such stream %q", types.ErrRedisUnavailable, args.Stream)
		}
		g, ok := s.groups[args.Group]
		if !ok {
			e.mu.Unlock()
			return nil, fmt.Errorf("%w: no such group %q", types.ErrRedisUnavailable, args.Group)
		}

		var out []Entry
		count := args.Count
		if count <= 0 {
			count = len(s.entries)
		}
		for _, entry := range s.entries {
			if len(out) >= count {
				break
			}
			if entry.deleted || !idLess(g.lastDelivered, entry.id) {
				continue
			}
			g.lastDelivered = entry.id
			if !args.NoAck {
				g.pending[entry.id] = &emPending{id: entry.id, consumer: args.Consumer, deliveredAt: e.now(), count: 1}
			}
			out = append(out, Entry{ID: entry.id, Fields: copyFields(entry.fields)})
		}

		if len(out) > 0 || args.Block <= 0 {
			e.mu.Unlock()
			return out, nil
		}

		remaining := deadline.Sub(e.now())
		if remaining <= 0 {
			e.mu.Unlock()
			return nil, nil
		}
		wait := s.subscribe()
		e.mu.Unlock()

		timer := time.NewTimer(remaining)
		select {
		case <-wait:
			timer.Stop()
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

// Ack implements Client.
func (e *Emulator) Ack(_ context.Context, stream, group string, ids ...string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.streams[stream]
	if !ok {
		return 0, nil
	}
	g, ok := s.groups[group]
	if !ok {
		return 0, nil
	}
	var n int64
	for _, id := range ids {
		if _, ok := g.pending[id]; ok {
			delete(g.pending, id)
			n++
		}
	}
	return n, nil
}

// Pending implements Client.
func (e *Emulator) Pending(_ context.Context, args PendingArgs) ([]PendingEntry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.streams[args.Stream]
	if !ok {
		return nil, nil
	}
	g, ok := s.groups[args.Group]
	if !ok {
		return nil, nil
	}
	now := e.now()
	var out []PendingEntry
	for _, p := range g.pending {
		idle := now.Sub(p.deliveredAt)
		if idle < args.Idle {
			continue
		}
		if args.Consumer != "" && p.consumer != args.Consumer {
			continue
		}
		if args.Start != "" && args.Start != "-" && idLess(p.id, args.Start) {
			continue
		}
		if args.End != "" && args.End != "+" && idLess(args.End, p.id) {
			continue
		}
		out = append(out, PendingEntry{ID: p.id, Consumer: p.consumer, Idle: idle, DeliveryCount: p.count})
	}
	sort.Slice(out, func(i, j int) bool { return idLess(out[i].ID, out[j].ID) })
	if args.Count > 0 && len(out) > args.Count {
		out = out[:args.Count]
	}
	return out, nil
}

// AutoClaim implements Client.
func (e *Emulator) AutoClaim(_ context.Context, args AutoClaimArgs) (string, []Entry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.streams[args.Stream]
	if !ok {
		return "0-0", nil, nil
	}
	g, ok := s.groups[args.Group]
	if !ok {
		return "0-0", nil, nil
	}
	start := args.Start
	if start == "" {
		start = "0-0"
	}
	// Sort pending IDs to scan deterministically from the cursor.
	ids := make([]string, 0, len(g.pending))
	for id := range g.pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return idLess(ids[i], ids[j]) })

	now := e.now()
	count := args.Count
	if count <= 0 {
		count = 100
	}
	var out []Entry
	nextCursor := "0-0"
	for i, id := range ids {
		if idLess(id, start) {
			continue
		}
		p := g.pending[id]
		if now.Sub(p.deliveredAt) < args.MinIdle {
			continue
		}
		entry, live := s.index[id]
		if !live || entry.deleted {
			// XAUTOCLAIM drops entries deleted from the stream.
			delete(g.pending, id)
			continue
		}
		if len(out) >= count {
			nextCursor = id // resume here next call
			break
		}
		p.consumer = args.Consumer
		p.deliveredAt = now
		p.count++
		out = append(out, Entry{ID: id, Fields: copyFields(entry.fields)})
		_ = i
	}
	return nextCursor, out, nil
}

// Len implements Client.
func (e *Emulator) Len(_ context.Context, stream string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.streams[stream]
	if !ok {
		return 0, nil
	}
	var n int64
	for _, entry := range s.entries {
		if !entry.deleted {
			n++
		}
	}
	return n, nil
}

// Delete implements Client.
func (e *Emulator) Delete(_ context.Context, stream string, ids ...string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.streams[stream]
	if !ok {
		return 0, nil
	}
	var n int64
	for _, id := range ids {
		if entry, ok := s.index[id]; ok && !entry.deleted {
			entry.deleted = true
			delete(s.index, id)
			n++
		}
	}
	return n, nil
}

// Close implements Client.
func (e *Emulator) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	for _, s := range e.streams {
		s.wake()
	}
	return nil
}

// PendingCount returns the size of a group's pending-entries list (test helper).
func (e *Emulator) PendingCount(stream, group string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.streams[stream]
	if !ok {
		return 0
	}
	g, ok := s.groups[group]
	if !ok {
		return 0
	}
	return len(g.pending)
}

// --- stream waiter machinery (for blocking reads) ---

func (s *emStream) subscribe() chan struct{} {
	ch := make(chan struct{})
	s.waiters = append(s.waiters, ch)
	return ch
}

func (s *emStream) wake() {
	for _, ch := range s.waiters {
		close(ch)
	}
	s.waiters = nil
}

func copyFields(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
