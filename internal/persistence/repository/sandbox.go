package repository

import (
	"context"
	"time"
)

// SandboxEntity maps the database sandboxes table.
type SandboxEntity struct {
	ID        string
	RuntimeID string
	Status    string
	IP        string
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   int64
	DeletedAt *time.Time
}

// SandboxRepository outlines repository methods for Sandbox instances.
type SandboxRepository interface {
	Create(ctx context.Context, sb *SandboxEntity) error
	Update(ctx context.Context, sb *SandboxEntity) error
	GetByID(ctx context.Context, id string) (*SandboxEntity, error)
	Delete(ctx context.Context, id string) error
}
