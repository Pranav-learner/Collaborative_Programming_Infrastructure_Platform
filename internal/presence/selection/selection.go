// Package selection manages range coordinates, direction, and clearing of selections.
package selection

import (
	"errors"

	"cpip/internal/presence/types"
)

var (
	ErrInvalidSelectionCoordinate = errors.New("selection: coordinates must be non-negative")
)

// Manager coordinates updates and validation of text selections.
type Manager struct{}

// New returns a selection Manager.
func New() *Manager {
	return &Manager{}
}

// Update validates coordinates, sets selection fields, and calculates direction.
func (m *Manager) Update(s *types.Selection, anchorLine, anchorCh, focusLine, focusCh int) error {
	if anchorLine < 0 || anchorCh < 0 || focusLine < 0 || focusCh < 0 {
		return ErrInvalidSelectionCoordinate
	}

	s.AnchorLine = anchorLine
	s.AnchorCh = anchorCh
	s.FocusLine = focusLine
	s.FocusCh = focusCh

	if focusLine > anchorLine {
		s.Direction = 1
	} else if focusLine < anchorLine {
		s.Direction = -1
	} else {
		if focusCh > anchorCh {
			s.Direction = 1
		} else if focusCh < anchorCh {
			s.Direction = -1
		} else {
			s.Direction = 0
		}
	}

	return nil
}

// Clear resets the selection fields to zero.
func (m *Manager) Clear(s *types.Selection) {
	s.AnchorLine = 0
	s.AnchorCh = 0
	s.FocusLine = 0
	s.FocusCh = 0
	s.Direction = 0
}
