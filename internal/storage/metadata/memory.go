package metadata

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"cpip/internal/storage/artifacts"
)

// MemoryStore is a thread-safe, transactionally-correct in-memory Store. It is
// the reference implementation: every operation observes the same invariants the
// Postgres store enforces (unique lineage/version, single head per lineage,
// guarded transitions). A single RWMutex serializes mutations, which is more than
// sufficient for tests and small single-node deployments; the Postgres store is
// the horizontally-scalable production path.
type MemoryStore struct {
	mu sync.RWMutex
	// byID indexes every artifact by its ID.
	byID map[string]*artifacts.Artifact
	// lineages indexes artifact IDs by lineage for fast version queries.
	lineages map[string]map[string]struct{}
}

// NewMemoryStore constructs an empty in-memory metadata store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		byID:     make(map[string]*artifacts.Artifact),
		lineages: make(map[string]map[string]struct{}),
	}
}

func (s *MemoryStore) indexLocked(a *artifacts.Artifact) {
	s.byID[a.ID] = a
	set := s.lineages[a.LineageID]
	if set == nil {
		set = make(map[string]struct{})
		s.lineages[a.LineageID] = set
	}
	set[a.ID] = struct{}{}
}

func (s *MemoryStore) lineageLocked(lineageID string) []*artifacts.Artifact {
	ids := s.lineages[lineageID]
	out := make([]*artifacts.Artifact, 0, len(ids))
	for id := range ids {
		out = append(out, s.byID[id])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// Create inserts a fully-populated record.
func (s *MemoryStore) Create(_ context.Context, a *artifacts.Artifact) error {
	if a == nil || a.ID == "" {
		return fmt.Errorf("%w: nil or id-less artifact", artifacts.ErrInvalidArtifact)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[a.ID]; ok {
		return fmt.Errorf("%w: id %s", artifacts.ErrAlreadyExists, a.ID)
	}
	if a.Version > 0 {
		for _, ex := range s.lineageLocked(a.LineageID) {
			if ex.Version == a.Version {
				return fmt.Errorf("%w: lineage %s version %d", artifacts.ErrAlreadyExists, a.LineageID, a.Version)
			}
		}
	}
	s.indexLocked(a.Clone())
	return nil
}

// AppendVersion atomically assigns the next version and flips the head.
func (s *MemoryStore) AppendVersion(_ context.Context, a *artifacts.Artifact) error {
	if a == nil || a.ID == "" || a.LineageID == "" {
		return fmt.Errorf("%w: nil, id-less, or lineage-less artifact", artifacts.ErrInvalidArtifact)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[a.ID]; ok {
		return fmt.Errorf("%w: id %s", artifacts.ErrAlreadyExists, a.ID)
	}
	var maxV int64
	for _, ex := range s.lineageLocked(a.LineageID) {
		if ex.Version > maxV {
			maxV = ex.Version
		}
		ex.IsLatest = false
	}
	a.Version = maxV + 1
	a.IsLatest = true
	s.indexLocked(a.Clone())
	return nil
}

// Get returns a deep copy so callers cannot mutate stored state.
func (s *MemoryStore) Get(_ context.Context, id string) (*artifacts.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.byID[id]
	if !ok {
		return nil, fmt.Errorf("%w: id %s", artifacts.ErrNotFound, id)
	}
	return a.Clone(), nil
}

// Update replaces mutable fields by ID.
func (s *MemoryStore) Update(_ context.Context, a *artifacts.Artifact) error {
	if a == nil || a.ID == "" {
		return fmt.Errorf("%w: nil or id-less artifact", artifacts.ErrInvalidArtifact)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.byID[a.ID]
	if !ok {
		return fmt.Errorf("%w: id %s", artifacts.ErrNotFound, a.ID)
	}
	upd := a.Clone()
	// Preserve immutable identity/lineage fields.
	upd.LineageID = cur.LineageID
	upd.UpdatedAt = time.Now().UTC()
	s.byID[a.ID] = upd
	return nil
}

// UpdateState performs a guarded transition.
func (s *MemoryStore) UpdateState(_ context.Context, id string, expected, next artifacts.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.byID[id]
	if !ok {
		return fmt.Errorf("%w: id %s", artifacts.ErrNotFound, id)
	}
	if cur.State != expected {
		return fmt.Errorf("%w: %s expected state %s but was %s", artifacts.ErrIllegalTransition, id, expected, cur.State)
	}
	if !artifacts.CanTransition(cur.State, next) {
		return fmt.Errorf("%w: %s -> %s", artifacts.ErrIllegalTransition, cur.State, next)
	}
	cp := cur.Clone()
	cp.State = next
	cp.UpdatedAt = time.Now().UTC()
	if next == artifacts.Deleted && cp.DeletedAt == nil {
		t := cp.UpdatedAt
		cp.DeletedAt = &t
	}
	if next == artifacts.Available {
		cp.DeletedAt = nil
	}
	s.byID[id] = cp
	return nil
}

// Delete hard-removes a metadata row.
func (s *MemoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.byID[id]
	if !ok {
		return fmt.Errorf("%w: id %s", artifacts.ErrNotFound, id)
	}
	delete(s.byID, id)
	if set := s.lineages[a.LineageID]; set != nil {
		delete(set, id)
		if len(set) == 0 {
			delete(s.lineages, a.LineageID)
		}
	}
	return nil
}

// GetLatest returns the head of a lineage.
func (s *MemoryStore) GetLatest(_ context.Context, lineageID string) (*artifacts.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.lineageLocked(lineageID) {
		if a.IsLatest {
			return a.Clone(), nil
		}
	}
	return nil, fmt.Errorf("%w: lineage %s", artifacts.ErrNotFound, lineageID)
}

// GetVersion returns a specific version within a lineage.
func (s *MemoryStore) GetVersion(_ context.Context, lineageID string, version int64) (*artifacts.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.lineageLocked(lineageID) {
		if a.Version == version {
			return a.Clone(), nil
		}
	}
	return nil, fmt.Errorf("%w: lineage %s version %d", artifacts.ErrVersionNotFound, lineageID, version)
}

// ListLineage returns all versions of a lineage ordered ascending by version.
func (s *MemoryStore) ListLineage(_ context.Context, lineageID string) ([]*artifacts.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.lineageLocked(lineageID)
	out := make([]*artifacts.Artifact, 0, len(src))
	for _, a := range src {
		out = append(out, a.Clone())
	}
	return out, nil
}

// SetLatest atomically moves the head pointer.
func (s *MemoryStore) SetLatest(_ context.Context, lineageID, artifactID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for _, a := range s.lineageLocked(lineageID) {
		if a.ID == artifactID {
			a.IsLatest = true
			a.UpdatedAt = time.Now().UTC()
			found = true
		} else {
			a.IsLatest = false
		}
	}
	if !found {
		return fmt.Errorf("%w: lineage %s artifact %s", artifacts.ErrNotFound, lineageID, artifactID)
	}
	return nil
}

// FindByContentHash returns an Available artifact with matching content.
func (s *MemoryStore) FindByContentHash(_ context.Context, bucket, hash string) (*artifacts.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.byID {
		if a.Bucket == bucket && a.ContentHash == hash && a.State == artifacts.Available {
			return a.Clone(), nil
		}
	}
	return nil, fmt.Errorf("%w: hash %s", artifacts.ErrNotFound, hash)
}

// CountReferences counts non-deleted artifacts pointing at the same object.
func (s *MemoryStore) CountReferences(_ context.Context, bucket, objectKey string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, a := range s.byID {
		if a.Bucket == bucket && a.ObjectKey == objectKey && a.State != artifacts.Deleted {
			n++
		}
	}
	return n, nil
}

func (s *MemoryStore) matches(a *artifacts.Artifact, q Query) bool {
	if !q.IncludeDeleted && a.State == artifacts.Deleted {
		return false
	}
	if q.Owner != "" && a.Owner != q.Owner {
		return false
	}
	if q.JobID != "" && a.JobID != q.JobID {
		return false
	}
	if q.RoomID != "" && a.RoomID != q.RoomID {
		return false
	}
	if q.DocumentID != "" && a.DocumentID != q.DocumentID {
		return false
	}
	if q.Bucket != "" && a.Bucket != q.Bucket {
		return false
	}
	if q.LineageID != "" && a.LineageID != q.LineageID {
		return false
	}
	if q.Type != "" && a.Type != q.Type {
		return false
	}
	if q.State != "" && a.State != q.State {
		return false
	}
	if q.LatestOnly && !a.IsLatest {
		return false
	}
	if q.ContentHash != "" && a.ContentHash != q.ContentHash {
		return false
	}
	if !q.CreatedAfter.IsZero() && !a.CreatedAt.After(q.CreatedAfter) {
		return false
	}
	if !q.CreatedBefore.IsZero() && !a.CreatedAt.Before(q.CreatedBefore) {
		return false
	}
	return true
}

// List runs a Query with ordering and pagination.
func (s *MemoryStore) List(_ context.Context, q Query) ([]*artifacts.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*artifacts.Artifact
	for _, a := range s.byID {
		if s.matches(a, q) {
			out = append(out, a.Clone())
		}
	}
	sortArtifacts(out, q)
	// Pagination.
	if q.Offset > 0 {
		if q.Offset >= len(out) {
			return nil, nil
		}
		out = out[q.Offset:]
	}
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

// Count runs a Query and returns the matching count (ignoring pagination).
func (s *MemoryStore) Count(_ context.Context, q Query) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	for _, a := range s.byID {
		if s.matches(a, q) {
			n++
		}
	}
	return n, nil
}

// FindExpired returns serveable artifacts whose RetainUntil policy has elapsed.
func (s *MemoryStore) FindExpired(_ context.Context, now time.Time, limit int) ([]*artifacts.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*artifacts.Artifact
	for _, a := range s.byID {
		if a.State != artifacts.Available && a.State != artifacts.Archived {
			continue
		}
		if a.IsExpired(now) {
			out = append(out, a.Clone())
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// Ping always succeeds for the in-memory store.
func (s *MemoryStore) Ping(_ context.Context) error { return nil }

// Close is a no-op.
func (s *MemoryStore) Close() error { return nil }

func sortArtifacts(a []*artifacts.Artifact, q Query) {
	less := func(i, j int) bool { return a[i].CreatedAt.Before(a[j].CreatedAt) }
	switch q.Sort {
	case SortByUpdatedAt:
		less = func(i, j int) bool { return a[i].UpdatedAt.Before(a[j].UpdatedAt) }
	case SortBySize:
		less = func(i, j int) bool { return a[i].Size < a[j].Size }
	case SortByVersion:
		less = func(i, j int) bool { return a[i].Version < a[j].Version }
	}
	sort.SliceStable(a, func(i, j int) bool {
		if q.Descending {
			return less(j, i)
		}
		return less(i, j)
	})
}

var _ Store = (*MemoryStore)(nil)
