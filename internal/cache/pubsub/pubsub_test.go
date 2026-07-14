package pubsub_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/pubsub"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/types"
)

func newHub(t *testing.T) (*pubsub.Manager, *redis.Emulator) {
	t.Helper()
	em := redis.NewEmulator()
	m := pubsub.New(pubsub.Params{
		Client: em,
		Config: config.Default().PubSub,
		Keys:   keys.New("cpip"),
	})
	return m, em
}

func TestPubSubPublishSubscribe(t *testing.T) {
	m, _ := newHub(t)
	defer m.Close()
	m.RegisterTopic(pubsub.TopicSpec{Name: "chat"})

	sub, err := m.Subscribe("chat")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	time.Sleep(20 * time.Millisecond) // router warmup

	if _, err := m.Publish(context.Background(), "chat", "hi"); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-sub.Messages():
		if msg.Payload != "hi" || msg.Topic != "chat" {
			t.Fatalf("unexpected %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestPubSubUnregisteredTopic(t *testing.T) {
	m, _ := newHub(t)
	defer m.Close()
	if _, err := m.Publish(context.Background(), "ghost", "x"); err != types.ErrTopicNotRegistered {
		t.Fatalf("expected ErrTopicNotRegistered, got %v", err)
	}
	if _, err := m.Subscribe("ghost"); err != types.ErrTopicNotRegistered {
		t.Fatalf("expected ErrTopicNotRegistered, got %v", err)
	}
}

func TestPubSubFanOutMultipleSubscribers(t *testing.T) {
	m, _ := newHub(t)
	defer m.Close()
	m.RegisterTopic(pubsub.TopicSpec{Name: "broadcast"})

	const n = 5
	subs := make([]*pubsub.Subscription, n)
	for i := range subs {
		s, err := m.Subscribe("broadcast")
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = s
		defer s.Close()
	}
	time.Sleep(20 * time.Millisecond)

	m.Publish(context.Background(), "broadcast", "all")

	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func(s *pubsub.Subscription) {
			defer wg.Done()
			select {
			case msg := <-s.Messages():
				if msg.Payload != "all" {
					t.Errorf("payload = %q", msg.Payload)
				}
			case <-time.After(time.Second):
				t.Error("subscriber timed out")
			}
		}(s)
	}
	wg.Wait()
}

// A slow subscriber that never drains must not block the router or its peers;
// with drop-on-backpressure it simply loses messages.
func TestPubSubBackpressureIsolation(t *testing.T) {
	em := redis.NewEmulator()
	cfg := config.Default().PubSub
	cfg.SubscriberBuffer = 2
	cfg.DropOnBackpressure = true
	m := pubsub.New(pubsub.Params{Client: em, Config: cfg, Keys: keys.New("cpip")})
	defer m.Close()
	m.RegisterTopic(pubsub.TopicSpec{Name: "firehose"})

	slow, _ := m.Subscribe("firehose") // never drained
	defer slow.Close()
	fast, _ := m.Subscribe("firehose")
	defer fast.Close()
	time.Sleep(20 * time.Millisecond)

	// Flood the topic; the slow subscriber's tiny buffer overflows and drops.
	for i := 0; i < 50; i++ {
		m.Publish(context.Background(), "firehose", "x")
	}
	// The fast subscriber can still receive without being starved by the slow one.
	got := 0
	deadline := time.After(time.Second)
	for got < 2 {
		select {
		case <-fast.Messages():
			got++
		case <-deadline:
			t.Fatalf("fast subscriber only received %d messages; slow subscriber blocked the router", got)
		}
	}
}
