// Package events implements the internal publish/subscribe bus for presence updates.
package events

import (
	"sync"
	"time"
)

// Type enumerates kinds of presence events.
type Type uint8

const (
	PresenceOnline Type = iota
	PresenceOffline
	PresenceRecovered
	CursorMoved
	SelectionChanged
	TypingStarted
	TypingStopped
	UserIdle
	UserAway
	HeartbeatReceived
	HeartbeatTimeout
	MetadataUpdated
	ActivityChanged
)

func (t Type) String() string {
	switch t {
	case PresenceOnline:
		return "PresenceOnline"
	case PresenceOffline:
		return "PresenceOffline"
	case PresenceRecovered:
		return "PresenceRecovered"
	case CursorMoved:
		return "CursorMoved"
	case SelectionChanged:
		return "SelectionChanged"
	case TypingStarted:
		return "TypingStarted"
	case TypingStopped:
		return "TypingStopped"
	case UserIdle:
		return "UserIdle"
	case UserAway:
		return "UserAway"
	case HeartbeatReceived:
		return "HeartbeatReceived"
	case HeartbeatTimeout:
		return "HeartbeatTimeout"
	case MetadataUpdated:
		return "MetadataUpdated"
	case ActivityChanged:
		return "ActivityChanged"
	default:
		return "Unknown"
	}
}

// Event carries information about a presence update.
type Event struct {
	Type      Type      `json:"type"`
	UserID    string    `json:"user_id"`
	RoomID    string    `json:"room_id"`
	SessionID string    `json:"session_id"`
	ConnID    string    `json:"conn_id"`
	At        time.Time `json:"at"`
	Payload   any       `json:"payload,omitempty"`
}

// Options configure the event bus hooks.
type Options struct {
	OnDrop    func()
	OnPublish func()
}

// Bus distributes events to all registered subscribers asynchronously and without blocking.
type Bus struct {
	mu          sync.RWMutex
	options     Options
	subscribers map[chan Event]struct{}
	closed      bool
}

// New constructs a Bus.
func New(opts Options) *Bus {
	return &Bus{
		options:     opts,
		subscribers: make(map[chan Event]struct{}),
	}
}

// Subscribe registers a new subscriber channel with a buffer size.
func (b *Bus) Subscribe(bufferSize int) chan Event {
	ch := make(chan Event, bufferSize)
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.subscribers[ch] = struct{}{}
	} else {
		close(ch)
	}
	return ch
}

// Unsubscribe removes and closes a subscriber channel.
func (b *Bus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subscribers[ch]; ok {
		delete(b.subscribers, ch)
		close(ch)
	}
}

// Publish sends an event to all subscribers in a non-blocking fashion.
func (b *Bus) Publish(ev Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}

	if b.options.OnPublish != nil {
		b.options.OnPublish()
	}

	for ch := range b.subscribers {
		select {
		case ch <- ev:
		default:
			// Dropped to prevent stalling publishing thread
			if b.options.OnDrop != nil {
				b.options.OnDrop()
			}
		}
	}
}

// Close unsubscribes and closes all subscriber channels.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for ch := range b.subscribers {
		close(ch)
	}
	b.subscribers = make(map[chan Event]struct{})
}
