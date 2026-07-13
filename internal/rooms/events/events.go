// Package events is the room subsystem's internal publish/subscribe bus.
//
// It is the seam through which future modules observe room activity without the
// room manager depending on them (inversion of control): the Presence module
// subscribes to UserJoined/UserLeft, the CRDT module subscribes to
// RoomCreated/RoomClosed to load and flush documents, the execution pipeline
// subscribes to RoomClosed to tear down sandboxes, and so on. The room manager
// only ever calls Publish.
//
// Design constraints that shape the implementation:
//
//   - Publishing must never block a room operation. Room mutations happen while
//     locks are held; a slow subscriber must not stall joins for everyone. So
//     channel delivery is non-blocking: if a subscriber's buffer is full the
//     event is dropped for that subscriber and counted, never blocked on.
//   - Delivery is best-effort and per-subscriber ordered. Within one subscriber
//     events arrive in publish order; across subscribers there is no ordering
//     guarantee.
//   - Two subscription styles are offered: buffered channel subscribers (the
//     common case — the consumer runs its own goroutine) and synchronous
//     callback subscribers (for cheap, non-blocking in-line reactions).
//
// The bus depends only on the leaf packages permissions and lifecycle, so it can
// be imported anywhere in the room subtree without cycles.
package events

import (
	"sync"
	"time"

	"cpip/internal/rooms/lifecycle"
	"cpip/internal/rooms/permissions"
)

// Type enumerates the kinds of room events. New kinds may be appended without
// affecting existing subscribers (they simply ignore types they do not handle).
type Type uint8

const (
	// RoomCreated fires when a room is created and registered.
	RoomCreated Type = iota
	// RoomClosed fires when a room transitions to Closed.
	RoomClosed
	// RoomDestroyed fires when a room is removed from the registry (terminal).
	RoomDestroyed
	// UserJoined fires when a participant joins a room for the first time.
	UserJoined
	// UserLeft fires when a participant leaves or is removed from a room.
	UserLeft
	// OwnerChanged fires when ownership is transferred.
	OwnerChanged
	// RoomExpired fires when a room enters its Expiring state due to inactivity.
	RoomExpired
	// RoomRecovered fires when a disconnected participant reconnects within its
	// recovery window, or a room is rescued from Expiring back to Active.
	RoomRecovered
	// MembershipUpdated fires when a participant's attributes change (role,
	// connection state) without a join/leave.
	MembershipUpdated
	// StateChanged fires on every lifecycle transition, carrying From/To.
	StateChanged
)

// String renders the event type as a stable label.
func (t Type) String() string {
	switch t {
	case RoomCreated:
		return "room_created"
	case RoomClosed:
		return "room_closed"
	case RoomDestroyed:
		return "room_destroyed"
	case UserJoined:
		return "user_joined"
	case UserLeft:
		return "user_left"
	case OwnerChanged:
		return "owner_changed"
	case RoomExpired:
		return "room_expired"
	case RoomRecovered:
		return "room_recovered"
	case MembershipUpdated:
		return "membership_updated"
	case StateChanged:
		return "state_changed"
	default:
		return "unknown_event"
	}
}

// Event is an immutable notification of something that happened in a room. Only
// primitive/value fields are carried so an event can be safely fanned out to
// many subscribers without shared mutable state.
type Event struct {
	Type Type
	// RoomID is the affected room. Always set.
	RoomID string
	// ActorID is the principal that caused the event (empty for system-driven
	// events such as expiry).
	ActorID string
	// TargetID is the principal the event is about, when distinct from the actor
	// (e.g. the kicked user, the new owner). Empty when not applicable.
	TargetID string
	// Role is the relevant role for membership events (e.g. the joiner's role).
	Role permissions.Role
	// From/To carry the lifecycle transition for StateChanged / RoomClosed /
	// RoomExpired events; both are StateCreated (zero) otherwise.
	From lifecycle.State
	To   lifecycle.State
	// Reason is a low-cardinality explanation for close/leave/expire events.
	Reason string
	// At is the wall-clock time the event was produced.
	At time.Time
}

// Handler is a synchronous, in-line event callback. It MUST return quickly and
// MUST NOT block or panic; it runs on the publisher's goroutine.
type Handler func(Event)

// Subscription is a handle to an active subscription. Close it to stop
// receiving events and release resources. Close is idempotent.
type Subscription struct {
	id     uint64
	bus    *Bus
	ch     chan Event // non-nil for channel subscriptions
	fn     Handler    // non-nil for synchronous subscriptions
	closed chan struct{}
	once   sync.Once
}

// C returns the channel a channel-subscription delivers on. It returns nil for
// synchronous (callback) subscriptions. The channel is closed when the
// subscription is Closed.
func (s *Subscription) C() <-chan Event { return s.ch }

// Close unsubscribes and releases the subscription. It is safe to call multiple
// times and from any goroutine.
func (s *Subscription) Close() {
	s.once.Do(func() {
		s.bus.remove(s.id)
		close(s.closed)
		if s.ch != nil {
			close(s.ch)
		}
	})
}

// Bus is a concurrency-safe in-process event bus. The zero value is not usable;
// construct one with New.
type Bus struct {
	mu     sync.RWMutex
	nextID uint64
	subs   map[uint64]*Subscription
	onDrop func() // optional metrics hook, invoked when a channel send is dropped
	onPub  func() // optional metrics hook, invoked once per Publish
	closed bool
}

// Options configure a Bus.
type Options struct {
	// OnDrop is called (if non-nil) each time an event is dropped because a
	// channel subscriber's buffer was full. Wire this to metrics.EventDropped.
	OnDrop func()
	// OnPublish is called (if non-nil) once per published event.
	OnPublish func()
}

// New builds an event Bus.
func New(opts Options) *Bus {
	return &Bus{
		subs:   make(map[uint64]*Subscription),
		onDrop: opts.OnDrop,
		onPub:  opts.OnPublish,
	}
}

// Subscribe registers a buffered channel subscriber and returns its
// Subscription. buffer bounds how many undelivered events may queue before
// further events are dropped for this subscriber; choose it based on how bursty
// the source is and how fast the consumer drains. A buffer <= 0 is raised to 1.
//
// The consumer typically runs `for e := range sub.C() { ... }` on its own
// goroutine and calls sub.Close() when done.
func (b *Bus) Subscribe(buffer int) *Subscription {
	if buffer <= 0 {
		buffer = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	s := &Subscription{
		id:     b.nextID,
		bus:    b,
		ch:     make(chan Event, buffer),
		closed: make(chan struct{}),
	}
	if !b.closed {
		b.subs[s.id] = s
	} else {
		// Bus already closed: hand back an already-closed subscription.
		close(s.closed)
		close(s.ch)
	}
	return s
}

// SubscribeFunc registers a synchronous callback subscriber. The handler runs
// in-line on the publishing goroutine, so it MUST be fast and non-blocking. Use
// it for lightweight reactions (e.g. cache invalidation); use Subscribe for
// anything that may block or do real work.
func (b *Bus) SubscribeFunc(fn Handler) *Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	s := &Subscription{
		id:     b.nextID,
		bus:    b,
		fn:     fn,
		closed: make(chan struct{}),
	}
	if !b.closed {
		b.subs[s.id] = s
	} else {
		close(s.closed)
	}
	return s
}

// Publish delivers e to every current subscriber. Channel subscribers receive a
// non-blocking send (dropped and counted if their buffer is full); callback
// subscribers are invoked in-line. Publish never blocks and never panics out to
// the caller even if a callback misbehaves — callbacks are shielded so one bad
// subscriber cannot break delivery to the others or the room operation that
// triggered the event.
func (b *Bus) Publish(e Event) {
	if b.onPub != nil {
		b.onPub()
	}
	b.mu.RLock()
	// Snapshot subscribers so we deliver without holding the lock (a callback
	// might itself Subscribe/Close, and a channel send should not hold the bus
	// lock).
	targets := make([]*Subscription, 0, len(b.subs))
	for _, s := range b.subs {
		targets = append(targets, s)
	}
	b.mu.RUnlock()

	for _, s := range targets {
		if s.ch != nil {
			select {
			case s.ch <- e:
			default:
				if b.onDrop != nil {
					b.onDrop()
				}
			}
			continue
		}
		if s.fn != nil {
			b.safeInvoke(s.fn, e)
		}
	}
}

// safeInvoke runs a callback subscriber, recovering from any panic so a faulty
// subscriber cannot crash the publisher.
func (b *Bus) safeInvoke(fn Handler, e Event) {
	defer func() { _ = recover() }()
	fn(e)
}

// remove detaches a subscription by id. Called by Subscription.Close.
func (b *Bus) remove(id uint64) {
	b.mu.Lock()
	delete(b.subs, id)
	b.mu.Unlock()
}

// SubscriberCount returns the number of active subscriptions. Intended for
// tests and introspection.
func (b *Bus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

// Close shuts the bus down: it detaches and closes every subscription and
// rejects future subscribers (which are handed an already-closed handle). It is
// idempotent.
func (b *Bus) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	subs := b.subs
	b.subs = make(map[uint64]*Subscription)
	b.mu.Unlock()

	for _, s := range subs {
		s.Close()
	}
}
