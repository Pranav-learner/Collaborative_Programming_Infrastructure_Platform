package repository

import (
	"context"
	"time"
)

// ArtifactMetadataEntity maps the database artifact_metadata table.
type ArtifactMetadataEntity struct {
	ID        string
	Name      string
	Type      string
	Path      string
	Size      int64
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ArtifactMetadataRepository outlines repository methods for artifact metadata.
type ArtifactMetadataRepository interface {
	Create(ctx context.Context, art *ArtifactMetadataEntity) error
	Update(ctx context.Context, art *ArtifactMetadataEntity) error
	GetByID(ctx context.Context, id string) (*ArtifactMetadataEntity, error)
	Delete(ctx context.Context, id string) error
}
