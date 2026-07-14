package repository

import (
	"context"
	"time"
)

// UserSessionEntity maps the database user_sessions table.
type UserSessionEntity struct {
	ID        string
	UserID    string
	Token     string
	ExpiresAt time.Time
	CreatedAt time.Time
	Version   int64
}

// UserSessionRepository outlines repository methods for user sessions.
type UserSessionRepository interface {
	Create(ctx context.Context, sess *UserSessionEntity) error
	Update(ctx context.Context, sess *UserSessionEntity) error
	GetByID(ctx context.Context, id string) (*UserSessionEntity, error)
	GetByToken(ctx context.Context, token string) (*UserSessionEntity, error)
	Delete(ctx context.Context, id string) error
}
