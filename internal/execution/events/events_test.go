package events

import (
	"testing"
	"time"
)

func TestPublishSubscribe(t *testing.T) {
	b := New(Options{})
	ch := b.Subscribe(4)
	defer b.Unsubscribe(ch)

	b.Publish(Event{Type: JobCreated, JobID: "j1"})
	select {
	case e := <-ch:
		if e.Type != JobCreated || e.JobID != "j1" || e.Timestamp.IsZero() {
			t.Fatalf("unexpected event %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("no event")
	}
}

func TestNonBlockingDrop(t *testing.T) {
	dropped := 0
	b := New(Options{OnDrop: func() { dropped++ }})
	ch := b.Subscribe(1)
	defer b.Unsubscribe(ch)
	b.Publish(Event{Type: JobQueued})
	b.Publish(Event{Type: JobQueued}) // buffer full → dropped
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
}

func TestCloseClosesSubscribers(t *testing.T) {
	b := New(Options{})
	ch := b.Subscribe(1)
	b.Close()
	if _, open := <-ch; open {
		t.Fatal("channel not closed after Close")
	}
}

func TestTypeStringStable(t *testing.T) {
	cases := map[Type]string{
		ExecutionRequested: "execution_requested",
		JobCompleted:       "job_completed",
		ExecutionArchived:  "execution_archived",
	}
	for typ, want := range cases {
		if typ.String() != want {
			t.Errorf("%d.String() = %q, want %q", typ, typ.String(), want)
		}
	}
}
