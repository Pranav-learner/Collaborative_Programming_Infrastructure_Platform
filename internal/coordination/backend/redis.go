package backend

import (
	"context"
	"errors"
	"time"

	cacheredis "cpip/internal/cache/redis"
	cachetypes "cpip/internal/cache/types"
	"cpip/internal/coordination/types"
)

// redisBackend adapts the platform's Redis client (internal/cache/redis) to the
// coordination Backend interface. It is the ONLY file in the module that names
// the Redis client type; every cluster service depends on Backend, so replacing
// Redis with etcd/Consul is a sibling of this file, not a rewrite.
//
// The adapter reuses the cache module's battle-tested client (and its faithful
// emulator) rather than re-implementing Redis semantics, while keeping
// coordination's public surface completely Redis-agnostic.
type redisBackend struct {
	client cacheredis.Client
}

// NewRedis wraps an existing Redis client as a coordination Backend. Passing the
// cache module's emulator (cacheredis.NewEmulator) yields a dependency-free
// backend with real pub/sub semantics for multi-goroutine tests.
func NewRedis(client cacheredis.Client) Backend {
	return &redisBackend{client: client}
}

func (r *redisBackend) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := r.client.Get(ctx, key)
	if errors.Is(err, cachetypes.ErrNil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, mapErr(err)
	}
	return v, true, nil
}

func (r *redisBackend) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return mapErr(r.client.Set(ctx, key, value, ttl))
}

func (r *redisBackend) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	ok, err := r.client.SetNX(ctx, key, value, ttl)
	return ok, mapErr(err)
}

func (r *redisBackend) Delete(ctx context.Context, keys ...string) (int64, error) {
	n, err := r.client.Del(ctx, keys...)
	return n, mapErr(err)
}

func (r *redisBackend) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := r.client.Expire(ctx, key, ttl)
	return ok, mapErr(err)
}

func (r *redisBackend) TTL(ctx context.Context, key string) (time.Duration, error) {
	d, err := r.client.TTL(ctx, key)
	return d, mapErr(err)
}

func (r *redisBackend) CompareAndSwap(ctx context.Context, key, expected, newValue string, ttl time.Duration) (bool, error) {
	ok, err := r.client.CompareAndSet(ctx, key, expected, newValue, ttl)
	return ok, mapErr(err)
}

func (r *redisBackend) CompareAndDelete(ctx context.Context, key, expected string) (bool, error) {
	ok, err := r.client.CompareAndDelete(ctx, key, expected)
	return ok, mapErr(err)
}

func (r *redisBackend) CompareAndExpire(ctx context.Context, key, expected string, ttl time.Duration) (bool, error) {
	ok, err := r.client.CompareAndExtend(ctx, key, expected, ttl)
	return ok, mapErr(err)
}

func (r *redisBackend) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	n, err := r.client.SAdd(ctx, key, members...)
	return n, mapErr(err)
}

func (r *redisBackend) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	n, err := r.client.SRem(ctx, key, members...)
	return n, mapErr(err)
}

func (r *redisBackend) SMembers(ctx context.Context, key string) ([]string, error) {
	m, err := r.client.SMembers(ctx, key)
	return m, mapErr(err)
}

func (r *redisBackend) Scan(ctx context.Context, prefix string) ([]string, error) {
	keys, err := r.client.ScanKeys(ctx, prefix+"*", 512)
	return keys, mapErr(err)
}

func (r *redisBackend) Publish(ctx context.Context, channel, payload string) error {
	_, err := r.client.Publish(ctx, channel, payload)
	return mapErr(err)
}

func (r *redisBackend) Subscribe(ctx context.Context, channel string) (Subscription, error) {
	sub, err := r.client.Subscribe(ctx, channel)
	if err != nil {
		return nil, mapErr(err)
	}
	return newRedisSub(sub), nil
}

func (r *redisBackend) Ping(ctx context.Context) error { return mapErr(r.client.Ping(ctx)) }

func (r *redisBackend) Close() error { return r.client.Close() }

// redisSub bridges a cacheredis.Subscription (which carries Message structs) to
// the coordination Subscription (raw payloads), running a small pump goroutine.
type redisSub struct {
	inner cacheredis.Subscription
	out   chan string
	done  chan struct{}
}

func newRedisSub(inner cacheredis.Subscription) *redisSub {
	s := &redisSub{inner: inner, out: make(chan string, 256), done: make(chan struct{})}
	go s.pump()
	return s
}

func (s *redisSub) pump() {
	defer close(s.out)
	for {
		select {
		case <-s.done:
			return
		case msg, ok := <-s.inner.Channel():
			if !ok {
				return
			}
			select {
			case s.out <- msg.Payload:
			default: // drop on backpressure
			}
		}
	}
}

func (s *redisSub) Messages() <-chan string { return s.out }

func (s *redisSub) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return s.inner.Close()
}

// mapErr normalizes backend errors onto the coordination sentinel set so callers
// match with errors.Is without knowing the concrete backend.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, cachetypes.ErrRedisUnavailable) {
		return errors.Join(types.ErrBackendUnavailable, err)
	}
	if errors.Is(err, cachetypes.ErrClosed) {
		return errors.Join(types.ErrClosed, err)
	}
	return err
}

var _ Backend = (*redisBackend)(nil)
