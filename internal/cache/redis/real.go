package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"cpip/internal/cache/config"
	"cpip/internal/cache/types"
)

// Redis is the production Client backed by github.com/redis/go-redis/v9. It maps
// the module's operations onto Redis commands, implements the atomic
// compare-and-* primitives with server-side Lua (so lock fencing and state CAS
// are race-free even under a shared connection pool), and normalizes backend
// errors to the module's canonical error set.
type Redis struct {
	rdb *goredis.Client
}

// NewRedisFromClient wraps an already-configured go-redis client. Useful when
// the caller owns connection lifecycle (e.g. sharing a client across modules).
func NewRedisFromClient(rdb *goredis.Client) *Redis { return &Redis{rdb: rdb} }

// NewRedis constructs a production client from module configuration and verifies
// connectivity before returning.
func NewRedis(cfg config.Redis) (*Redis, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:            cfg.Addr,
		Username:        cfg.Username,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		PoolTimeout:     cfg.PoolTimeout,
		MaxRetries:      cfg.MaxRetries,
		MinRetryBackoff: cfg.MinRetryBackoff,
		MaxRetryBackoff: cfg.MaxRetryBackoff,
	})
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("%w: initial ping: %v", types.ErrRedisUnavailable, err)
	}
	return &Redis{rdb: rdb}, nil
}

// Underlying exposes the raw go-redis client for advanced callers (e.g. a future
// Redis Streams integration). Use sparingly; it bypasses the module's error
// normalization.
func (r *Redis) Underlying() *goredis.Client { return r.rdb }

func unavailable(err error) error {
	return fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
}

// --- Strings ---

func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	s, err := r.rdb.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return "", types.ErrNil
	}
	if err != nil {
		return "", unavailable(err)
	}
	return s, nil
}

func (r *Redis) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	exp := ttl
	if ttl == KeepTTL {
		exp = goredis.KeepTTL
	}
	if err := r.rdb.Set(ctx, key, value, exp).Err(); err != nil {
		return unavailable(err)
	}
	return nil
}

func (r *Redis) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	ok, err := r.rdb.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, unavailable(err)
	}
	return ok, nil
}

func (r *Redis) Del(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	n, err := r.rdb.Del(ctx, keys...).Result()
	if err != nil {
		return 0, unavailable(err)
	}
	return n, nil
}

func (r *Redis) Exists(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	n, err := r.rdb.Exists(ctx, keys...).Result()
	if err != nil {
		return 0, unavailable(err)
	}
	return n, nil
}

func (r *Redis) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := r.rdb.Expire(ctx, key, ttl).Result()
	if err != nil {
		return false, unavailable(err)
	}
	return ok, nil
}

func (r *Redis) TTL(ctx context.Context, key string) (time.Duration, error) {
	d, err := r.rdb.TTL(ctx, key).Result()
	if err != nil {
		return 0, unavailable(err)
	}
	// go-redis encodes the -2/-1 sentinels as negative nanosecond durations.
	switch d {
	case -2 * time.Nanosecond:
		return -2 * time.Second, nil
	case -1 * time.Nanosecond:
		return -1 * time.Second, nil
	}
	return d, nil
}

func (r *Redis) Persist(ctx context.Context, key string) (bool, error) {
	ok, err := r.rdb.Persist(ctx, key).Result()
	if err != nil {
		return false, unavailable(err)
	}
	return ok, nil
}

func (r *Redis) Incr(ctx context.Context, key string) (int64, error) {
	n, err := r.rdb.Incr(ctx, key).Result()
	if err != nil {
		return 0, unavailable(err)
	}
	return n, nil
}

// --- Atomic compare-and-* via Lua ---

var (
	scriptCompareAndDelete = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)

	scriptCompareAndExtend = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`)

	scriptCompareAndSet = goredis.NewScript(`
local cur = redis.call("GET", KEYS[1])
if (ARGV[1] == "" and cur == false) or cur == ARGV[1] then
	if tonumber(ARGV[3]) > 0 then
		redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
	else
		redis.call("SET", KEYS[1], ARGV[2])
	end
	return 1
end
return 0`)
)

func (r *Redis) CompareAndDelete(ctx context.Context, key, expected string) (bool, error) {
	res, err := scriptCompareAndDelete.Run(ctx, r.rdb, []string{key}, expected).Int64()
	if err != nil {
		return false, unavailable(err)
	}
	return res == 1, nil
}

func (r *Redis) CompareAndExtend(ctx context.Context, key, expected string, ttl time.Duration) (bool, error) {
	res, err := scriptCompareAndExtend.Run(ctx, r.rdb, []string{key}, expected, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, unavailable(err)
	}
	return res == 1, nil
}

func (r *Redis) CompareAndSet(ctx context.Context, key, expected, newValue string, ttl time.Duration) (bool, error) {
	res, err := scriptCompareAndSet.Run(ctx, r.rdb, []string{key}, expected, newValue, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, unavailable(err)
	}
	return res == 1, nil
}

// --- Bulk ---

func (r *Redis) MGet(ctx context.Context, keys ...string) ([]*string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	vals, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, unavailable(err)
	}
	out := make([]*string, len(vals))
	for i, v := range vals {
		if s, ok := v.(string); ok {
			cp := s
			out[i] = &cp
		}
	}
	return out, nil
}

func (r *Redis) SetMany(ctx context.Context, pairs map[string]string, ttl time.Duration) error {
	if len(pairs) == 0 {
		return nil
	}
	pipe := r.rdb.Pipeline()
	for k, v := range pairs {
		pipe.Set(ctx, k, v, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return unavailable(err)
	}
	return nil
}

func (r *Redis) ScanKeys(ctx context.Context, match string, count int64) ([]string, error) {
	if count <= 0 {
		count = 256
	}
	var (
		cursor uint64
		out    []string
	)
	for {
		keys, next, err := r.rdb.Scan(ctx, cursor, match, count).Result()
		if err != nil {
			return nil, unavailable(err)
		}
		out = append(out, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out, nil
}

// --- Hashes ---

func (r *Redis) HSet(ctx context.Context, key string, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}
	args := make([]any, 0, len(fields)*2)
	for f, v := range fields {
		args = append(args, f, v)
	}
	if err := r.rdb.HSet(ctx, key, args...).Err(); err != nil {
		return unavailable(err)
	}
	return nil
}

func (r *Redis) HGet(ctx context.Context, key, field string) (string, error) {
	s, err := r.rdb.HGet(ctx, key, field).Result()
	if errors.Is(err, goredis.Nil) {
		return "", types.ErrNil
	}
	if err != nil {
		return "", unavailable(err)
	}
	return s, nil
}

func (r *Redis) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	m, err := r.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, unavailable(err)
	}
	return m, nil
}

func (r *Redis) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	n, err := r.rdb.HDel(ctx, key, fields...).Result()
	if err != nil {
		return 0, unavailable(err)
	}
	return n, nil
}

// --- Sets ---

func (r *Redis) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	args := make([]any, len(members))
	for i, m := range members {
		args[i] = m
	}
	n, err := r.rdb.SAdd(ctx, key, args...).Result()
	if err != nil {
		return 0, unavailable(err)
	}
	return n, nil
}

func (r *Redis) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	args := make([]any, len(members))
	for i, m := range members {
		args[i] = m
	}
	n, err := r.rdb.SRem(ctx, key, args...).Result()
	if err != nil {
		return 0, unavailable(err)
	}
	return n, nil
}

func (r *Redis) SMembers(ctx context.Context, key string) ([]string, error) {
	m, err := r.rdb.SMembers(ctx, key).Result()
	if err != nil {
		return nil, unavailable(err)
	}
	return m, nil
}

func (r *Redis) SIsMember(ctx context.Context, key, member string) (bool, error) {
	ok, err := r.rdb.SIsMember(ctx, key, member).Result()
	if err != nil {
		return false, unavailable(err)
	}
	return ok, nil
}

// --- Pub/Sub ---

func (r *Redis) Publish(ctx context.Context, channel, message string) (int64, error) {
	n, err := r.rdb.Publish(ctx, channel, message).Result()
	if err != nil {
		return 0, unavailable(err)
	}
	return n, nil
}

func (r *Redis) Subscribe(ctx context.Context, channels ...string) (Subscription, error) {
	ps := r.rdb.Subscribe(ctx, channels...)
	return newGoredisSub(ps), nil
}

func (r *Redis) PSubscribe(ctx context.Context, patterns ...string) (Subscription, error) {
	ps := r.rdb.PSubscribe(ctx, patterns...)
	return newGoredisSub(ps), nil
}

// --- Health / lifecycle ---

func (r *Redis) Ping(ctx context.Context) error {
	if err := r.rdb.Ping(ctx).Err(); err != nil {
		return unavailable(err)
	}
	return nil
}

func (r *Redis) Close() error { return r.rdb.Close() }

// goredisSub adapts a *goredis.PubSub to the Subscription interface, translating
// *goredis.Message into the module's Message type on a forwarding goroutine.
type goredisSub struct {
	ps   *goredis.PubSub
	out  chan Message
	done chan struct{}
}

func newGoredisSub(ps *goredis.PubSub) *goredisSub {
	s := &goredisSub{ps: ps, out: make(chan Message, emSubBuffer), done: make(chan struct{})}
	go s.forward()
	return s
}

func (s *goredisSub) forward() {
	in := s.ps.Channel()
	for {
		select {
		case <-s.done:
			return
		case m, ok := <-in:
			if !ok {
				return
			}
			select {
			case s.out <- Message{Channel: m.Channel, Pattern: m.Pattern, Payload: m.Payload}:
			case <-s.done:
				return
			default:
				// Drop on overflow; the pub/sub manager owns backpressure policy.
			}
		}
	}
}

func (s *goredisSub) Channel() <-chan Message { return s.out }

func (s *goredisSub) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return s.ps.Close()
}

var (
	_ Client       = (*Redis)(nil)
	_ Subscription = (*goredisSub)(nil)
	_ Subscription = (*emSub)(nil)
)
