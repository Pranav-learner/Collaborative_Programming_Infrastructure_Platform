// Package cursor handles coordinate validation and attributes for participant cursors.
package cursor

import (
	"errors"

	"cpip/internal/presence/types"
)

var (
	ErrInvalidLine = errors.New("cursor: line must be non-negative")
	ErrInvalidChar = errors.New("cursor: character column must be non-negative")
)

// Manager coordinates updates and validation of cursor positions.
type Manager struct{}

// New returns a cursor Manager.
func New() *Manager {
	return &Manager{}
}

// Update mutates the given cursor structure with new coordinates after validating them.
func (m *Manager) Update(c *types.Cursor, line, ch int, color string, filePath string, visible bool) error {
	if line < 0 {
		return ErrInvalidLine
	}
	if ch < 0 {
		return ErrInvalidChar
	}
	c.Line = line
	c.Ch = ch
	c.Visible = visible
	if color != "" {
		c.Color = color
	}
	c.FilePath = filePath
	return nil
}
