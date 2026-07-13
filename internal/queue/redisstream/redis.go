package redisstream

import (
	"context"
	"errors"
	"fmt"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"cpip/internal/queue/types"
)

// Redis is the production Client backed by github.com/redis/go-redis/v9. It maps
// the queue's stream operations onto Redis Streams commands and normalizes
// backend errors to the queue's canonical error set.
type Redis struct {
	rdb *goredis.Client
}

// NewRedis constructs a production Redis Streams client from a go-redis client.
func NewRedis(rdb *goredis.Client) *Redis {
	return &Redis{rdb: rdb}
}

// wrapErr normalizes a backend error, preserving detail while allowing callers
// to match errors.Is(err, types.ErrRedisUnavailable).
func wrapErr(err error) error {
	if err == nil || errors.Is(err, goredis.Nil) {
		return nil
	}
	return fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
}

// Add implements Client (XADD).
func (r *Redis) Add(ctx context.Context, stream string, fields map[string]string) (string, error) {
	values := make(map[string]any, len(fields))
	for k, v := range fields {
		values[k] = v
	}
	id, err := r.rdb.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: values}).Result()
	if err != nil {
		return "", fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
	}
	return id, nil
}

// CreateGroup implements Client (XGROUP CREATE ... MKSTREAM). An existing group
// is treated as success.
func (r *Redis) CreateGroup(ctx context.Context, stream, group, start string) error {
	if start == "" {
		start = "0"
	}
	err := r.rdb.XGroupCreateMkStream(ctx, stream, group, start).Err()
	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
	}
	return nil
}

// ReadGroup implements Client (XREADGROUP ... >).
func (r *Redis) ReadGroup(ctx context.Context, args ReadGroupArgs) ([]Entry, error) {
	block := args.Block
	if block <= 0 {
		block = -1 // non-blocking
	}
	res, err := r.rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    args.Group,
		Consumer: args.Consumer,
		Streams:  []string{args.Stream, ">"},
		Count:    int64(args.Count),
		Block:    block,
		NoAck:    args.NoAck,
	}).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
	}
	var out []Entry
	for _, s := range res {
		for _, m := range s.Messages {
			out = append(out, Entry{ID: m.ID, Fields: valuesToFields(m.Values)})
		}
	}
	return out, nil
}

// Ack implements Client (XACK).
func (r *Redis) Ack(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	n, err := r.rdb.XAck(ctx, stream, group, ids...).Result()
	if err != nil {
		return 0, fmt.Errorf("%w: %v", types.ErrAckFailed, err)
	}
	return n, nil
}

// Pending implements Client (XPENDING ... IDLE).
func (r *Redis) Pending(ctx context.Context, args PendingArgs) ([]PendingEntry, error) {
	start, end := args.Start, args.End
	if start == "" {
		start = "-"
	}
	if end == "" {
		end = "+"
	}
	count := int64(args.Count)
	if count <= 0 {
		count = 100
	}
	res, err := r.rdb.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream:   args.Stream,
		Group:    args.Group,
		Idle:     args.Idle,
		Start:    start,
		End:      end,
		Count:    count,
		Consumer: args.Consumer,
	}).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
	}
	out := make([]PendingEntry, 0, len(res))
	for _, p := range res {
		out = append(out, PendingEntry{ID: p.ID, Consumer: p.Consumer, Idle: p.Idle, DeliveryCount: p.RetryCount})
	}
	return out, nil
}

// AutoClaim implements Client (XAUTOCLAIM).
func (r *Redis) AutoClaim(ctx context.Context, args AutoClaimArgs) (string, []Entry, error) {
	start := args.Start
	if start == "" {
		start = "0"
	}
	count := int64(args.Count)
	if count <= 0 {
		count = 100
	}
	msgs, cursor, err := r.rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream:   args.Stream,
		Group:    args.Group,
		Consumer: args.Consumer,
		MinIdle:  args.MinIdle,
		Start:    start,
		Count:    count,
	}).Result()
	if err != nil {
		return "", nil, fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
	}
	out := make([]Entry, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, Entry{ID: m.ID, Fields: valuesToFields(m.Values)})
	}
	return cursor, out, nil
}

// Len implements Client (XLEN).
func (r *Redis) Len(ctx context.Context, stream string) (int64, error) {
	n, err := r.rdb.XLen(ctx, stream).Result()
	if err != nil {
		return 0, fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
	}
	return n, nil
}

// Delete implements Client (XDEL).
func (r *Redis) Delete(ctx context.Context, stream string, ids ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	n, err := r.rdb.XDel(ctx, stream, ids...).Result()
	if err != nil {
		return 0, fmt.Errorf("%w: %v", types.ErrRedisUnavailable, err)
	}
	return n, nil
}

// Close implements Client.
func (r *Redis) Close() error { return r.rdb.Close() }

func valuesToFields(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		switch s := v.(type) {
		case string:
			out[k] = s
		default:
			out[k] = fmt.Sprintf("%v", v)
		}
	}
	return out
}

// compile-time assertions.
var (
	_ Client = (*Redis)(nil)
	_ Client = (*Emulator)(nil)
)
