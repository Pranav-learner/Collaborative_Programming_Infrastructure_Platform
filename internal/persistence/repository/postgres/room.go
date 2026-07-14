package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cpip/internal/persistence/locking"
	"cpip/internal/persistence/query"
	"cpip/internal/persistence/repository"
)

type RoomRepository struct {
	exec repository.Executor
}

func NewRoomRepository(exec repository.Executor) *RoomRepository {
	return &RoomRepository{exec: exec}
}

func (r *RoomRepository) Create(ctx context.Context, room *repository.RoomEntity) error {
	metaJSON, err := json.Marshal(room.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal room metadata: %w", err)
	}

	if room.Version == 0 {
		room.Version = 1
	}

	queryStr := `
		INSERT INTO rooms (
			id, name, owner_id, state, created_at, last_activity,
			max_participants, idle_timeout_ns, expire_timeout_ns, recovery_timeout_ns,
			visibility, metadata, version, deleted_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`

	_, err = r.exec.ExecContext(ctx, queryStr,
		room.ID, room.Name, room.OwnerID, room.State, room.CreatedAt, room.LastActivity,
		room.MaxParticipants, room.IdleTimeoutNs, room.ExpireTimeoutNs, room.RecoveryTimeoutNs,
		room.Visibility, metaJSON, room.Version, room.DeletedAt,
	)
	return err
}

func (r *RoomRepository) Update(ctx context.Context, room *repository.RoomEntity) error {
	metaJSON, err := json.Marshal(room.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal room metadata: %w", err)
	}

	oldVersion := room.Version
	room.Version++

	queryStr := `
		UPDATE rooms SET
			name = $1, owner_id = $2, state = $3, last_activity = $4,
			max_participants = $5, idle_timeout_ns = $6, expire_timeout_ns = $7,
			recovery_timeout_ns = $8, visibility = $9, metadata = $10, version = $11
		WHERE id = $12 AND version = $13 AND deleted_at IS NULL
	`

	res, err := r.exec.ExecContext(ctx, queryStr,
		room.Name, room.OwnerID, room.State, room.LastActivity,
		room.MaxParticipants, room.IdleTimeoutNs, room.ExpireTimeoutNs,
		room.RecoveryTimeoutNs, room.Visibility, metaJSON, room.Version,
		room.ID, oldVersion,
	)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		// Roll back our increment to reflect database state
		room.Version = oldVersion
		return locking.ErrOptimisticLockConflict
	}

	return nil
}

func (r *RoomRepository) GetByID(ctx context.Context, id string, includeDeleted bool) (*repository.RoomEntity, error) {
	queryStr := `
		SELECT
			id, name, owner_id, state, created_at, last_activity,
			max_participants, idle_timeout_ns, expire_timeout_ns, recovery_timeout_ns,
			visibility, metadata, version, deleted_at
		FROM rooms
		WHERE id = $1
	`
	if !includeDeleted {
		queryStr += " AND deleted_at IS NULL"
	}

	row := r.exec.QueryRowContext(ctx, queryStr, id)

	var room repository.RoomEntity
	var metaBytes []byte
	err := row.Scan(
		&room.ID, &room.Name, &room.OwnerID, &room.State, &room.CreatedAt, &room.LastActivity,
		&room.MaxParticipants, &room.IdleTimeoutNs, &room.ExpireTimeoutNs, &room.RecoveryTimeoutNs,
		&room.Visibility, &metaBytes, &room.Version, &room.DeletedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, repository.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if len(metaBytes) > 0 {
		if err := json.Unmarshal(metaBytes, &room.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal room metadata: %w", err)
		}
	}

	return &room, nil
}

func (r *RoomRepository) Delete(ctx context.Context, id string) error {
	queryStr := "UPDATE rooms SET deleted_at = $1 WHERE id = $2 AND deleted_at IS NULL"
	res, err := r.exec.ExecContext(ctx, queryStr, time.Now(), id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return repository.ErrNotFound
	}
	return nil
}

func (r *RoomRepository) Restore(ctx context.Context, id string) error {
	queryStr := "UPDATE rooms SET deleted_at = NULL WHERE id = $1 AND deleted_at IS NOT NULL"
	res, err := r.exec.ExecContext(ctx, queryStr, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return repository.ErrNotFound
	}
	return nil
}

func (r *RoomRepository) List(ctx context.Context, params query.Params) ([]*repository.RoomEntity, error) {
	baseQuery := `
		SELECT
			id, name, owner_id, state, created_at, last_activity,
			max_participants, idle_timeout_ns, expire_timeout_ns, recovery_timeout_ns,
			visibility, metadata, version, deleted_at
		FROM rooms
	`
	// Always enforce soft delete filter unless requested otherwise
	hasDeletedFilter := false
	for _, f := range params.Filters {
		if f.Field == "deleted_at" {
			hasDeletedFilter = true
			break
		}
	}
	if !hasDeletedFilter {
		params.Filters = append(params.Filters, query.Filter{
			Field:    "deleted_at",
			Operator: query.OpIsNull,
		})
	}

	fullQuery, args := query.BuildQuery(baseQuery, params, 1)

	rows, err := r.exec.QueryContext(ctx, fullQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rooms []*repository.RoomEntity
	for rows.Next() {
		var room repository.RoomEntity
		var metaBytes []byte
		err := rows.Scan(
			&room.ID, &room.Name, &room.OwnerID, &room.State, &room.CreatedAt, &room.LastActivity,
			&room.MaxParticipants, &room.IdleTimeoutNs, &room.ExpireTimeoutNs, &room.RecoveryTimeoutNs,
			&room.Visibility, &metaBytes, &room.Version, &room.DeletedAt,
		)
		if err != nil {
			return nil, err
		}

		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &room.Metadata); err != nil {
				return nil, fmt.Errorf("failed to unmarshal room metadata: %w", err)
			}
		}
		rooms = append(rooms, &room)
	}

	return rooms, nil
}

func (r *RoomRepository) AddParticipant(ctx context.Context, p *repository.ParticipantEntity) error {
	metaJSON, err := json.Marshal(p.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal participant metadata: %w", err)
	}

	queryStr := `
		INSERT INTO participants (
			room_id, user_id, role, session_id, conn_id, joined_at, last_seen, connected, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (room_id, user_id) DO UPDATE SET
			role = EXCLUDED.role, session_id = EXCLUDED.session_id, conn_id = EXCLUDED.conn_id,
			last_seen = EXCLUDED.last_seen, connected = EXCLUDED.connected, metadata = EXCLUDED.metadata
	`

	_, err = r.exec.ExecContext(ctx, queryStr,
		p.RoomID, p.UserID, p.Role, p.SessionID, p.ConnID, p.JoinedAt, p.LastSeen, p.Connected, metaJSON,
	)
	return err
}

func (r *RoomRepository) RemoveParticipant(ctx context.Context, roomID, userID string) error {
	queryStr := "DELETE FROM participants WHERE room_id = $1 AND user_id = $2"
	res, err := r.exec.ExecContext(ctx, queryStr, roomID, userID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return repository.ErrNotFound
	}
	return nil
}

func (r *RoomRepository) GetParticipants(ctx context.Context, roomID string) ([]*repository.ParticipantEntity, error) {
	queryStr := `
		SELECT room_id, user_id, role, session_id, conn_id, joined_at, last_seen, connected, metadata
		FROM participants
		WHERE room_id = $1
	`

	rows, err := r.exec.QueryContext(ctx, queryStr, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var participants []*repository.ParticipantEntity
	for rows.Next() {
		var p repository.ParticipantEntity
		var metaBytes []byte
		err := rows.Scan(
			&p.RoomID, &p.UserID, &p.Role, &p.SessionID, &p.ConnID, &p.JoinedAt, &p.LastSeen, &p.Connected, &metaBytes,
		)
		if err != nil {
			return nil, err
		}

		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &p.Metadata); err != nil {
				return nil, fmt.Errorf("failed to unmarshal participant metadata: %w", err)
			}
		}
		participants = append(participants, &p)
	}

	return participants, nil
}
