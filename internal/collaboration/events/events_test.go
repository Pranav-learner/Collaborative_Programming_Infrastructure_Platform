package events

import (
	"testing"
	"time"
)

func TestPublishSubscribe(t *testing.T) {
	b := New(Options{})
	ch := b.Subscribe(4)
	defer b.Unsubscribe(ch)

	b.Publish(Event{Type: DocumentCreated, DocID: "d1"})
	select {
	case ev := <-ch:
		if ev.Type != DocumentCreated || ev.DocID != "d1" {
			t.Fatalf("unexpected event %+v", ev)
		}
		if ev.Timestamp.IsZero() {
			t.Fatal("timestamp not stamped")
		}
	case <-time.After(time.Second):
		t.Fatal("no event received")
	}
}

func TestNonBlockingDropInvokesHook(t *testing.T) {
	dropped := 0
	b := New(Options{OnDrop: func() { dropped++ }})
	ch := b.Subscribe(1) // capacity 1
	defer b.Unsubscribe(ch)

	b.Publish(Event{Type: UpdateApplied}) // fills buffer
	b.Publish(Event{Type: UpdateApplied}) // dropped
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	b := New(Options{})
	ch := b.Subscribe(1)
	b.Unsubscribe(ch)
	if _, open := <-ch; open {
		t.Fatal("channel should be closed after unsubscribe")
	}
}

func TestCloseClosesAllSubscribers(t *testing.T) {
	b := New(Options{})
	ch1 := b.Subscribe(1)
	ch2 := b.Subscribe(1)
	b.Close()
	if _, open := <-ch1; open {
		t.Fatal("ch1 not closed")
	}
	if _, open := <-ch2; open {
		t.Fatal("ch2 not closed")
	}
}

func TestTypeStringStable(t *testing.T) {
	cases := map[Type]string{
		DocumentCreated:          "document_created",
		UpdateApplied:            "update_applied",
		SynchronizationCompleted: "synchronization_completed",
		ParticipantJoined:        "participant_joined",
	}
	for typ, want := range cases {
		if got := typ.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", typ, got, want)
		}
	}
}
