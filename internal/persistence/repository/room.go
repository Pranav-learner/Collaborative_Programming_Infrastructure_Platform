package repository

import (
	"context"
	"time"

	"cpip/internal/persistence/query"
)

// RoomEntity maps the database rooms table.
type RoomEntity struct {
	ID                 string
	Name               string
	OwnerID            string
	State              string
	CreatedAt          time.Time
	LastActivity       time.Time
	MaxParticipants    int
	IdleTimeoutNs      int64
	ExpireTimeoutNs    int64
	RecoveryTimeoutNs  int64
	Visibility         string
	Metadata           map[string]any
	Version            int64
	DeletedAt          *time.Time
}

// ParticipantEntity maps the database participants table.
type ParticipantEntity struct {
	RoomID    string
	UserID    string
	Role      string
	SessionID string
	ConnID    string
	JoinedAt  time.Time
	LastSeen  time.Time
	Connected bool
	Metadata  map[string]any
}

// RoomRepository outlines repository methods for Room and Participant operations.
type RoomRepository interface {
	Create(ctx context.Context, room *RoomEntity) error
	Update(ctx context.Context, room *RoomEntity) error
	GetByID(ctx context.Context, id string, includeDeleted bool) (*RoomEntity, error)
	Delete(ctx context.Context, id string) error
	Restore(ctx context.Context, id string) error
	List(ctx context.Context, params query.Params) ([]*RoomEntity, error)

	AddParticipant(ctx context.Context, p *ParticipantEntity) error
	RemoveParticipant(ctx context.Context, roomID, userID string) error
	GetParticipants(ctx context.Context, roomID string) ([]*ParticipantEntity, error)
}
