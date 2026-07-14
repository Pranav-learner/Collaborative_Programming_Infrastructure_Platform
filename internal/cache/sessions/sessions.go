// Package sessions is the distributed session store. Sessions live in Redis so
// any CPIP node can authenticate a request regardless of which node the user's
// WebSocket landed on. Each session is a JSON document guarded by optimistic
// concurrency (compare-and-set on the exact prior value), so two nodes updating
// the same session concurrently can never silently clobber each other.
//
// Multi-device support: a user may hold many concurrent sessions (laptop,
// phone, tablet). A per-user Redis set indexes the live session IDs so all of a
// user's devices can be listed or invalidated together (e.g. "sign out
// everywhere").
package sessions

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/events"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/logger"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/types"
)

// Session is a stored session document.
type Session struct {
	ID         string    `json:"id"`
	UserID     string    `json:"user_id"`
	DeviceID   string    `json:"device_id,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	LastSeenAt time.Time `json:"last_seen_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	// Version increments on every successful update (also used for CAS clarity).
	Version int64             `json:"version"`
	Data    map[string]string `json:"data,omitempty"`
}

// Expired reports whether the session's absolute expiry has passed.
func (s *Session) Expired(now time.Time) bool { return !s.ExpiresAt.IsZero() && now.After(s.ExpiresAt) }

// CreateParams describes a new session.
type CreateParams struct {
	UserID   string
	DeviceID string
	TTL      time.Duration // 0 → config default
	Data     map[string]string
}

// Store is the distributed session store.
type Store struct {
	client      redis.Client
	kb          keys.Builder
	defaultTTL  time.Duration
	bus         *events.Bus
	rec         metrics.Recorder
	log         *logger.Logger
	maxCASRetry int
	now         func() time.Time
}

// Params configures a Store.
type Params struct {
	Client  redis.Client
	Keys    keys.Builder
	Config  config.TTL
	Bus     *events.Bus
	Metrics metrics.Recorder
	Logger  *logger.Logger
}

// New constructs a session Store.
func New(p Params) *Store {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	log := p.Logger
	if log == nil {
		log = logger.New(nil)
	}
	ttl := p.Config.Session
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Store{
		client:      p.Client,
		kb:          p.Keys,
		defaultTTL:  ttl,
		bus:         p.Bus,
		rec:         rec,
		log:         log,
		maxCASRetry: 5,
		now:         time.Now,
	}
}

// SetClock overrides the clock (tests).
func (s *Store) SetClock(now func() time.Time) { s.now = now }

func newSessionID() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return "sess_" + hex.EncodeToString(b)
}

// Create mints a new session, persists it with a TTL, and indexes it under the
// user's device set for multi-device management.
func (s *Store) Create(ctx context.Context, p CreateParams) (*Session, error) {
	ttl := p.TTL
	if ttl <= 0 {
		ttl = s.defaultTTL
	}
	now := s.now()
	sess := &Session{
		ID:         newSessionID(),
		UserID:     p.UserID,
		DeviceID:   p.DeviceID,
		CreatedAt:  now,
		LastSeenAt: now,
		ExpiresAt:  now.Add(ttl),
		Version:    1,
		Data:       p.Data,
	}
	raw, err := json.Marshal(sess)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", types.ErrSerialization, err)
	}
	if err := s.client.Set(ctx, s.kb.Session(sess.ID), string(raw), ttl); err != nil {
		return nil, err
	}
	if p.UserID != "" {
		if _, err := s.client.SAdd(ctx, s.kb.UserSessions(p.UserID), sess.ID); err != nil {
			s.log.Session(ctx, "index_failed", sess.ID, p.UserID, err)
		}
	}
	s.rec.IncCounter(metrics.MetricSessionCreated, map[string]string{})
	s.bus.Emit(events.SessionCreated, "sessions", func(e *events.Event) { e.Key = sess.ID })
	s.log.Session(ctx, "created", sess.ID, p.UserID, nil)
	return sess, nil
}

// getRaw returns the stored JSON and parsed session, distinguishing a genuine
// miss from a backend error.
func (s *Store) getRaw(ctx context.Context, id string) (string, *Session, error) {
	raw, err := s.client.Get(ctx, s.kb.Session(id))
	if err != nil {
		if errors.Is(err, types.ErrNil) {
			return "", nil, types.ErrSessionNotFound
		}
		return "", nil, err
	}
	var sess Session
	if err := json.Unmarshal([]byte(raw), &sess); err != nil {
		return "", nil, fmt.Errorf("%w: %v", types.ErrDeserialization, err)
	}
	return raw, &sess, nil
}

// Get looks up a session. Expired sessions (past their absolute deadline) are
// treated as not found and best-effort cleaned up.
func (s *Store) Get(ctx context.Context, id string) (*Session, error) {
	_, sess, err := s.getRaw(ctx, id)
	if err != nil {
		return nil, err
	}
	if sess.Expired(s.now()) {
		_ = s.Invalidate(ctx, id)
		s.rec.IncCounter(metrics.MetricSessionExpired, map[string]string{})
		s.bus.Emit(events.SessionExpired, "sessions", func(e *events.Event) { e.Key = id })
		return nil, types.ErrSessionExpired
	}
	return sess, nil
}

// Renew extends a session's lifetime and refreshes last-seen. It uses the same
// optimistic-concurrency update path so a concurrent Update is never lost.
func (s *Store) Renew(ctx context.Context, id string, ttl time.Duration) (*Session, error) {
	if ttl <= 0 {
		ttl = s.defaultTTL
	}
	sess, err := s.Update(ctx, id, func(cur *Session) error {
		now := s.now()
		cur.LastSeenAt = now
		cur.ExpiresAt = now.Add(ttl)
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.rec.IncCounter(metrics.MetricSessionRenewed, map[string]string{})
	s.bus.Emit(events.SessionRenewed, "sessions", func(e *events.Event) { e.Key = id })
	return sess, nil
}

// Update applies mutate to the session and writes it back under optimistic
// concurrency: it compares-and-sets against the exact bytes it read, retrying on
// contention. Returns ErrSessionConflict if it loses the race maxCASRetry times.
func (s *Store) Update(ctx context.Context, id string, mutate func(*Session) error) (*Session, error) {
	for attempt := 0; attempt < s.maxCASRetry; attempt++ {
		raw, sess, err := s.getRaw(ctx, id)
		if err != nil {
			return nil, err
		}
		if sess.Expired(s.now()) {
			_ = s.Invalidate(ctx, id)
			return nil, types.ErrSessionExpired
		}
		if err := mutate(sess); err != nil {
			return nil, err
		}
		sess.Version++
		newRaw, err := json.Marshal(sess)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", types.ErrSerialization, err)
		}
		// TTL derived from the (possibly updated) absolute expiry.
		ttl := time.Until(sess.ExpiresAt)
		if ttl <= 0 {
			ttl = s.defaultTTL
		}
		ok, err := s.client.CompareAndSet(ctx, s.kb.Session(id), raw, string(newRaw), ttl)
		if err != nil {
			return nil, err
		}
		if ok {
			return sess, nil
		}
		// Lost the race — another writer updated the session; retry.
	}
	s.rec.IncCounter(metrics.MetricSessionInvalidated, map[string]string{"reason": "conflict"})
	return nil, fmt.Errorf("%w: session %q after %d attempts", types.ErrSessionConflict, id, s.maxCASRetry)
}

// Invalidate removes a session and de-indexes it from its user's device set.
func (s *Store) Invalidate(ctx context.Context, id string) error {
	// Best-effort de-index: read the session first to learn its user.
	_, sess, err := s.getRaw(ctx, id)
	if err == nil && sess.UserID != "" {
		_, _ = s.client.SRem(ctx, s.kb.UserSessions(sess.UserID), id)
	}
	if _, err := s.client.Del(ctx, s.kb.Session(id)); err != nil {
		return err
	}
	s.rec.IncCounter(metrics.MetricSessionInvalidated, map[string]string{})
	s.bus.Emit(events.SessionInvalidated, "sessions", func(e *events.Event) { e.Key = id })
	s.log.Session(ctx, "invalidated", id, "", nil)
	return nil
}

// ListByUser returns all of a user's live sessions (one per device), pruning any
// stale IDs left in the index by crashed nodes.
func (s *Store) ListByUser(ctx context.Context, userID string) ([]*Session, error) {
	ids, err := s.client.SMembers(ctx, s.kb.UserSessions(userID))
	if err != nil {
		return nil, err
	}
	out := make([]*Session, 0, len(ids))
	for _, id := range ids {
		sess, err := s.Get(ctx, id)
		if err != nil {
			if errors.Is(err, types.ErrSessionNotFound) || errors.Is(err, types.ErrSessionExpired) {
				// Prune the dangling index entry.
				_, _ = s.client.SRem(ctx, s.kb.UserSessions(userID), id)
				continue
			}
			return nil, err
		}
		out = append(out, sess)
	}
	return out, nil
}

// InvalidateUser signs a user out of every device.
func (s *Store) InvalidateUser(ctx context.Context, userID string) (int, error) {
	ids, err := s.client.SMembers(ctx, s.kb.UserSessions(userID))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		if err := s.Invalidate(ctx, id); err == nil {
			n++
		}
	}
	_, _ = s.client.Del(ctx, s.kb.UserSessions(userID))
	return n, nil
}

// Exists reports whether a session key is present (cheap; no decode).
func (s *Store) Exists(ctx context.Context, id string) (bool, error) {
	n, err := s.client.Exists(ctx, s.kb.Session(id))
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
