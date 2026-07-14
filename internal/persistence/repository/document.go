package repository

import (
	"context"
	"time"
)

// DocumentEntity maps the database documents table.
type DocumentEntity struct {
	ID        string
	RoomID    string
	Content   []byte
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

// DocumentRepository outlines repository methods for Document operations.
type DocumentRepository interface {
	Create(ctx context.Context, doc *DocumentEntity) error
	Update(ctx context.Context, doc *DocumentEntity) error
	GetByID(ctx context.Context, id string) (*DocumentEntity, error)
	GetByRoomID(ctx context.Context, roomID string) (*DocumentEntity, error)
	Delete(ctx context.Context, id string) error
}
