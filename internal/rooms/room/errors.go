package room

import "errors"

// Domain errors returned by Room operations. Callers compare with errors.Is.
//
// Authorization failures surface as permissions.ErrPermissionDenied and illegal
// lifecycle moves as lifecycle.ErrInvalidTransition; those are defined in their
// own packages and wrapped, not redefined here.
var (
	// ErrRoomClosed means the room no longer accepts membership changes because
	// it is Closed or Destroyed.
	ErrRoomClosed = errors.New("room: closed")
	// ErrRoomFull means the room is at its configured participant capacity.
	ErrRoomFull = errors.New("room: at capacity")
	// ErrDuplicateParticipant means the user is already an actively-connected
	// member (a second concurrent join, not a reconnect).
	ErrDuplicateParticipant = errors.New("room: already a participant")
	// ErrParticipantNotFound means the referenced user is not a member.
	ErrParticipantNotFound = errors.New("room: participant not found")
	// ErrNotAParticipant means the acting principal is not a member and therefore
	// holds no role from which to authorize an action.
	ErrNotAParticipant = errors.New("room: actor is not a participant")
	// ErrCannotRemoveOwner means a kick targeted the owner without sufficient
	// (administrator) authority; ownership must be transferred first.
	ErrCannotRemoveOwner = errors.New("room: cannot remove the owner")
	// ErrInvalidRole means a role value outside the known set was supplied.
	ErrInvalidRole = errors.New("room: invalid role")
	// ErrAlreadyOwner means ownership was transferred to the current owner.
	ErrAlreadyOwner = errors.New("room: target is already the owner")
)
