package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"cpip/internal/storage/artifacts"
)

// PostgresStore is the durable, multi-node metadata system-of-record. It uses
// database/sql with the pgx stdlib driver (the same stack as the persistence
// module) and enforces every lineage invariant with transactions and unique
// indexes, so concurrent uploads across nodes cannot corrupt versioning.
//
// The store is intentionally provider-neutral: it depends only on *sql.DB, so it
// composes with the persistence module's pool or any independently-constructed
// connection. It never imports an object-storage vendor SDK.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore wraps an existing *sql.DB. The caller owns the pool lifecycle
// unless CloseDB is used. Call Migrate once at startup to ensure the schema.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// Migrate applies the idempotent schema DDL.
func (s *PostgresStore) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, Schema); err != nil {
		return fmt.Errorf("%w: migrate: %v", artifacts.ErrBackendUnavailable, err)
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

// scanArtifact reads one row (from the canonical `columns` order) into an Artifact.
func scanArtifact(sc rowScanner) (*artifacts.Artifact, error) {
	var (
		a          artifacts.Artifact
		deletedAt  sql.NullTime
		expireAt   sql.NullTime
		legalHold  bool
		comp, enc  []byte
		ret, meta  []byte
		stats, cdn []byte
	)
	err := sc.Scan(
		&a.ID, &a.ObjectKey, &a.Bucket, &a.ContentHash, &a.Size, &a.ContentType, &a.Type,
		&a.Owner, &a.JobID, &a.RoomID, &a.DocumentID, &a.Language, &a.Version, &a.LineageID, &a.IsLatest,
		&comp, &enc, &ret, &a.State, &a.CreatedAt, &a.UpdatedAt, &deletedAt,
		&expireAt, &legalHold, &meta, &stats, &cdn,
	)
	if err != nil {
		return nil, err
	}
	if deletedAt.Valid {
		t := deletedAt.Time
		a.DeletedAt = &t
	}
	unmarshal(comp, &a.Compression)
	unmarshal(enc, &a.Encryption)
	unmarshal(ret, &a.Retention)
	unmarshal(meta, &a.Metadata)
	unmarshal(stats, &a.Statistics)
	unmarshal(cdn, &a.CDNMetadata)
	// Trust the denormalized columns for the reaper-critical fields.
	a.Retention.LegalHold = legalHold
	if expireAt.Valid {
		t := expireAt.Time
		a.Retention.ExpireAt = &t
	}
	return &a, nil
}

func unmarshal(b []byte, dst any) {
	if len(b) == 0 {
		return
	}
	_ = json.Unmarshal(b, dst)
}

func marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// bindArgs returns the 27 positional args for an INSERT in `columns` order.
func bindArgs(a *artifacts.Artifact) []any {
	var deletedAt any
	if a.DeletedAt != nil {
		deletedAt = *a.DeletedAt
	}
	var expireAt any
	if a.Retention.ExpireAt != nil {
		expireAt = *a.Retention.ExpireAt
	}
	return []any{
		a.ID, a.ObjectKey, a.Bucket, a.ContentHash, a.Size, a.ContentType, a.Type,
		a.Owner, a.JobID, a.RoomID, a.DocumentID, a.Language, a.Version, a.LineageID, a.IsLatest,
		marshal(a.Compression), marshal(a.Encryption), marshal(a.Retention), a.State, a.CreatedAt, a.UpdatedAt, deletedAt,
		expireAt, a.Retention.LegalHold, marshal(a.Metadata), marshal(a.Statistics), marshal(a.CDNMetadata),
	}
}

func placeholders(n int) string {
	ps := make([]string, n)
	for i := range ps {
		ps[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(ps, ", ")
}

var insertSQL = fmt.Sprintf(
	"INSERT INTO %s (%s) VALUES (%s)", Table, columns, placeholders(27))

func mapInsertErr(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "duplicate") || strings.Contains(msg, "unique") {
		return fmt.Errorf("%w: %v", artifacts.ErrAlreadyExists, err)
	}
	return fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
}

// Create inserts a fully-populated record.
func (s *PostgresStore) Create(ctx context.Context, a *artifacts.Artifact) error {
	if a == nil || a.ID == "" {
		return fmt.Errorf("%w: nil or id-less artifact", artifacts.ErrInvalidArtifact)
	}
	_, err := s.db.ExecContext(ctx, insertSQL, bindArgs(a)...)
	return mapInsertErr(err)
}

// AppendVersion atomically assigns the next version and flips the head.
func (s *PostgresStore) AppendVersion(ctx context.Context, a *artifacts.Artifact) error {
	if a == nil || a.ID == "" || a.LineageID == "" {
		return fmt.Errorf("%w: nil, id-less, or lineage-less artifact", artifacts.ErrInvalidArtifact)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		var maxV sql.NullInt64
		if err := tx.QueryRowContext(ctx,
			`SELECT MAX(version) FROM storage_artifacts WHERE lineage_id = $1`, a.LineageID,
		).Scan(&maxV); err != nil {
			return fmt.Errorf("%w: max version: %v", artifacts.ErrBackendUnavailable, err)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE storage_artifacts SET is_latest = FALSE WHERE lineage_id = $1 AND is_latest`, a.LineageID,
		); err != nil {
			return fmt.Errorf("%w: demote head: %v", artifacts.ErrBackendUnavailable, err)
		}
		a.Version = maxV.Int64 + 1
		a.IsLatest = true
		if _, err := tx.ExecContext(ctx, insertSQL, bindArgs(a)...); err != nil {
			return mapInsertErr(err)
		}
		return nil
	})
}

// Get returns an artifact by ID.
func (s *PostgresStore) Get(ctx context.Context, id string) (*artifacts.Artifact, error) {
	row := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE id = $1", columns, Table), id)
	a, err := scanArtifact(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: id %s", artifacts.ErrNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	return a, nil
}

// Update replaces mutable fields by ID.
func (s *PostgresStore) Update(ctx context.Context, a *artifacts.Artifact) error {
	if a == nil || a.ID == "" {
		return fmt.Errorf("%w: nil or id-less artifact", artifacts.ErrInvalidArtifact)
	}
	a.UpdatedAt = time.Now().UTC()
	var deletedAt any
	if a.DeletedAt != nil {
		deletedAt = *a.DeletedAt
	}
	var expireAt any
	if a.Retention.ExpireAt != nil {
		expireAt = *a.Retention.ExpireAt
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE storage_artifacts SET
			object_key=$2, bucket=$3, content_hash=$4, size=$5, content_type=$6, type=$7,
			owner=$8, job_id=$9, room_id=$10, document_id=$11, language=$12,
			is_latest=$13, compression=$14, encryption=$15, retention=$16, state=$17,
			updated_at=$18, deleted_at=$19, expire_at=$20, legal_hold=$21,
			metadata=$22, statistics=$23, cdn_metadata=$24
		WHERE id=$1`,
		a.ID, a.ObjectKey, a.Bucket, a.ContentHash, a.Size, a.ContentType, a.Type,
		a.Owner, a.JobID, a.RoomID, a.DocumentID, a.Language,
		a.IsLatest, marshal(a.Compression), marshal(a.Encryption), marshal(a.Retention), a.State,
		a.UpdatedAt, deletedAt, expireAt, a.Retention.LegalHold,
		marshal(a.Metadata), marshal(a.Statistics), marshal(a.CDNMetadata),
	)
	if err != nil {
		return fmt.Errorf("%w: update: %v", artifacts.ErrBackendUnavailable, err)
	}
	return affectedOrNotFound(res, a.ID)
}

// UpdateState performs a guarded transition entirely inside the database.
func (s *PostgresStore) UpdateState(ctx context.Context, id string, expected, next artifacts.State) error {
	if !artifacts.CanTransition(expected, next) {
		return fmt.Errorf("%w: %s -> %s", artifacts.ErrIllegalTransition, expected, next)
	}
	now := time.Now().UTC()
	var deletedClause string
	switch next {
	case artifacts.Deleted:
		deletedClause = ", deleted_at = $5"
	case artifacts.Available:
		deletedClause = ", deleted_at = NULL"
	}
	q := fmt.Sprintf(
		`UPDATE storage_artifacts SET state=$2, updated_at=$3 %s WHERE id=$1 AND state=$4`, deletedClause)
	args := []any{id, next, now, expected}
	if next == artifacts.Deleted {
		args = append(args, now)
	}
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("%w: update state: %v", artifacts.ErrBackendUnavailable, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Either the row is gone or the expected-state guard failed.
		return fmt.Errorf("%w: %s expected state %s", artifacts.ErrIllegalTransition, id, expected)
	}
	return nil
}

// Delete hard-removes a metadata row.
func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM storage_artifacts WHERE id=$1`, id)
	if err != nil {
		return fmt.Errorf("%w: delete: %v", artifacts.ErrBackendUnavailable, err)
	}
	return affectedOrNotFound(res, id)
}

// GetLatest returns the head of a lineage.
func (s *PostgresStore) GetLatest(ctx context.Context, lineageID string) (*artifacts.Artifact, error) {
	row := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE lineage_id=$1 AND is_latest", columns, Table), lineageID)
	a, err := scanArtifact(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: lineage %s", artifacts.ErrNotFound, lineageID)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	return a, nil
}

// GetVersion returns a specific version within a lineage.
func (s *PostgresStore) GetVersion(ctx context.Context, lineageID string, version int64) (*artifacts.Artifact, error) {
	row := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE lineage_id=$1 AND version=$2", columns, Table), lineageID, version)
	a, err := scanArtifact(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: lineage %s version %d", artifacts.ErrVersionNotFound, lineageID, version)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	return a, nil
}

// ListLineage returns all versions ordered ascending.
func (s *PostgresStore) ListLineage(ctx context.Context, lineageID string) ([]*artifacts.Artifact, error) {
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE lineage_id=$1 ORDER BY version ASC", columns, Table), lineageID)
	if err != nil {
		return nil, fmt.Errorf("%w: list lineage: %v", artifacts.ErrBackendUnavailable, err)
	}
	return collectRows(rows)
}

// SetLatest atomically moves the head pointer.
func (s *PostgresStore) SetLatest(ctx context.Context, lineageID, artifactID string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`UPDATE storage_artifacts SET is_latest=FALSE WHERE lineage_id=$1 AND is_latest`, lineageID,
		); err != nil {
			return fmt.Errorf("%w: demote: %v", artifacts.ErrBackendUnavailable, err)
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE storage_artifacts SET is_latest=TRUE, updated_at=now() WHERE id=$1 AND lineage_id=$2`,
			artifactID, lineageID)
		if err != nil {
			return fmt.Errorf("%w: promote: %v", artifacts.ErrBackendUnavailable, err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("%w: lineage %s artifact %s", artifacts.ErrNotFound, lineageID, artifactID)
		}
		return nil
	})
}

// FindByContentHash returns an Available artifact with matching content.
func (s *PostgresStore) FindByContentHash(ctx context.Context, bucket, hash string) (*artifacts.Artifact, error) {
	row := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE bucket=$1 AND content_hash=$2 AND state='available' LIMIT 1", columns, Table),
		bucket, hash)
	a, err := scanArtifact(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: hash %s", artifacts.ErrNotFound, hash)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	return a, nil
}

// CountReferences counts non-deleted artifacts pointing at the same object.
func (s *PostgresStore) CountReferences(ctx context.Context, bucket, objectKey string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM storage_artifacts WHERE bucket=$1 AND object_key=$2 AND state <> 'deleted'`,
		bucket, objectKey).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("%w: count refs: %v", artifacts.ErrBackendUnavailable, err)
	}
	return n, nil
}

// List runs a Query with a dynamically-built, fully-parameterized WHERE clause.
func (s *PostgresStore) List(ctx context.Context, q Query) ([]*artifacts.Artifact, error) {
	where, args := buildWhere(q)
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "SELECT %s FROM %s %s", columns, Table, where)
	sb.WriteString(orderBy(q))
	if q.Limit > 0 {
		args = append(args, q.Limit)
		fmt.Fprintf(sb, " LIMIT $%d", len(args))
	}
	if q.Offset > 0 {
		args = append(args, q.Offset)
		fmt.Fprintf(sb, " OFFSET $%d", len(args))
	}
	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("%w: list: %v", artifacts.ErrBackendUnavailable, err)
	}
	return collectRows(rows)
}

// Count runs a Query and returns the matching count (ignoring pagination).
func (s *PostgresStore) Count(ctx context.Context, q Query) (int64, error) {
	where, args := buildWhere(q)
	var n int64
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s %s", Table, where), args...).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("%w: count: %v", artifacts.ErrBackendUnavailable, err)
	}
	return n, nil
}

// FindExpired returns serveable, expired artifacts using the expiry index.
func (s *PostgresStore) FindExpired(ctx context.Context, now time.Time, limit int) ([]*artifacts.Artifact, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT %s FROM %s
		WHERE state IN ('available','archived')
		  AND legal_hold = FALSE
		  AND expire_at IS NOT NULL
		  AND expire_at <= $1
		ORDER BY expire_at ASC
		LIMIT $2`, columns, Table), now, limit)
	if err != nil {
		return nil, fmt.Errorf("%w: find expired: %v", artifacts.ErrBackendUnavailable, err)
	}
	return collectRows(rows)
}

// Ping verifies connectivity.
func (s *PostgresStore) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("%w: %v", artifacts.ErrBackendUnavailable, err)
	}
	return nil
}

// Close is a no-op; the caller owns the *sql.DB. Use CloseDB to close the pool.
func (s *PostgresStore) Close() error { return nil }

// CloseDB closes the underlying pool (use only when this store owns it).
func (s *PostgresStore) CloseDB() error { return s.db.Close() }

// --- helpers ---

func (s *PostgresStore) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin tx: %v", artifacts.ErrBackendUnavailable, err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: commit: %v", artifacts.ErrBackendUnavailable, err)
	}
	return nil
}

func collectRows(rows *sql.Rows) ([]*artifacts.Artifact, error) {
	defer rows.Close()
	var out []*artifacts.Artifact
	for rows.Next() {
		a, err := scanArtifact(rows)
		if err != nil {
			return nil, fmt.Errorf("%w: scan: %v", artifacts.ErrBackendUnavailable, err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: rows: %v", artifacts.ErrBackendUnavailable, err)
	}
	return out, nil
}

func affectedOrNotFound(res sql.Result, id string) error {
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: id %s", artifacts.ErrNotFound, id)
	}
	return nil
}

// buildWhere assembles a parameterized WHERE clause from a Query.
func buildWhere(q Query) (string, []any) {
	var conds []string
	var args []any
	add := func(cond string, val any) {
		args = append(args, val)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if !q.IncludeDeleted {
		conds = append(conds, "state <> 'deleted'")
	}
	if q.Owner != "" {
		add("owner = $%d", q.Owner)
	}
	if q.JobID != "" {
		add("job_id = $%d", q.JobID)
	}
	if q.RoomID != "" {
		add("room_id = $%d", q.RoomID)
	}
	if q.DocumentID != "" {
		add("document_id = $%d", q.DocumentID)
	}
	if q.Bucket != "" {
		add("bucket = $%d", q.Bucket)
	}
	if q.LineageID != "" {
		add("lineage_id = $%d", q.LineageID)
	}
	if q.Type != "" {
		add("type = $%d", string(q.Type))
	}
	if q.State != "" {
		add("state = $%d", string(q.State))
	}
	if q.LatestOnly {
		conds = append(conds, "is_latest")
	}
	if q.ContentHash != "" {
		add("content_hash = $%d", q.ContentHash)
	}
	if !q.CreatedAfter.IsZero() {
		add("created_at > $%d", q.CreatedAfter)
	}
	if !q.CreatedBefore.IsZero() {
		add("created_at < $%d", q.CreatedBefore)
	}
	if len(conds) == 0 {
		return "", args
	}
	return "WHERE " + strings.Join(conds, " AND "), args
}

func orderBy(q Query) string {
	col := "created_at"
	switch q.Sort {
	case SortByUpdatedAt:
		col = "updated_at"
	case SortBySize:
		col = "size"
	case SortByVersion:
		col = "version"
	}
	dir := "ASC"
	if q.Descending {
		dir = "DESC"
	}
	return fmt.Sprintf(" ORDER BY %s %s", col, dir)
}

var _ Store = (*PostgresStore)(nil)
