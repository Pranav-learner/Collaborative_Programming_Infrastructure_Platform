// Package permissions defines the extensible authorization model for rooms.
//
// Authorization is expressed as two orthogonal concepts:
//
//   - Role — the standing a principal holds within a room (Observer, Participant,
//     Moderator, Owner, Administrator). Roles are totally ordered by a numeric
//     rank so that "at least role X" checks are a single comparison.
//   - Action — a distinct, auditable operation on a room (join, kick, transfer
//     ownership, close, ...).
//
// A Policy maps every Action to the minimum Role that may perform it. Policies
// are immutable values: a room may carry its own Policy (a per-room override of
// the platform default) and every check is a pure function of (role, action,
// policy) with no shared mutable state. This keeps authorization decisions
// race-free even when they run inside a room's lock.
//
// The model is deliberately extension-friendly: adding a role (e.g. a future
// "Auditor") or an action (e.g. "InviteGuest") is a matter of declaring a new
// constant and giving it a rank / requirement — no calling code changes.
package permissions

import (
	"errors"
	"fmt"
)

// ErrPermissionDenied is returned by Policy.Require when a role lacks the
// standing to perform an action. Callers compare with errors.Is.
var ErrPermissionDenied = errors.New("permissions: denied")

// Role is a principal's standing within a single room. Roles are ordered by
// Rank; a higher rank strictly dominates the authority of a lower one.
type Role uint8

const (
	// RoleObserver may watch a room (presence, document, execution output) but
	// not contribute edits. It is the least-privileged role.
	RoleObserver Role = iota
	// RoleParticipant is a full collaborator: it may edit and run code.
	RoleParticipant
	// RoleModerator (future) may additionally manage membership (kick) but not
	// dispose of the room or transfer ownership.
	RoleModerator
	// RoleOwner is the room's controlling principal: it may transfer ownership,
	// close the room, and modify its settings.
	RoleOwner
	// RoleAdministrator (future) is a platform-level operator that dominates
	// every room-scoped role, including Owner. It exists so staff/support tooling
	// can act on any room without being its owner.
	RoleAdministrator
)

// rank orders roles by authority. Administrator is deliberately ranked above
// Owner so a platform operator satisfies every owner-gated check.
func (r Role) rank() uint8 {
	switch r {
	case RoleObserver:
		return 1
	case RoleParticipant:
		return 2
	case RoleModerator:
		return 3
	case RoleOwner:
		return 4
	case RoleAdministrator:
		return 5
	default:
		return 0
	}
}

// AtLeast reports whether r has at least the authority of other.
func (r Role) AtLeast(other Role) bool { return r.rank() >= other.rank() }

// Valid reports whether r is a known role.
func (r Role) Valid() bool { return r.rank() != 0 }

func (r Role) String() string {
	switch r {
	case RoleObserver:
		return "observer"
	case RoleParticipant:
		return "participant"
	case RoleModerator:
		return "moderator"
	case RoleOwner:
		return "owner"
	case RoleAdministrator:
		return "administrator"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// Action is a distinct authorizable operation on a room.
type Action uint8

const (
	// ActionObserve is the right to read a room's state.
	ActionObserve Action = iota
	// ActionJoin is the right to become a member of a room.
	ActionJoin
	// ActionLeave is the right to voluntarily depart a room.
	ActionLeave
	// ActionKick is the right to forcibly remove another participant.
	ActionKick
	// ActionTransferOwnership is the right to hand ownership to another member.
	ActionTransferOwnership
	// ActionCloseRoom is the right to terminate a room.
	ActionCloseRoom
	// ActionModifySettings is the right to change a room's configuration.
	ActionModifySettings
)

func (a Action) String() string {
	switch a {
	case ActionObserve:
		return "observe"
	case ActionJoin:
		return "join"
	case ActionLeave:
		return "leave"
	case ActionKick:
		return "kick"
	case ActionTransferOwnership:
		return "transfer_ownership"
	case ActionCloseRoom:
		return "close_room"
	case ActionModifySettings:
		return "modify_settings"
	default:
		return fmt.Sprintf("action(%d)", uint8(a))
	}
}

// Policy maps each Action to the minimum Role required to perform it. It is an
// immutable value; derive a modified copy with With.
//
// The zero Policy denies destructive actions by default (an unset requirement is
// treated as RoleAdministrator, the most restrictive), so a mis-constructed
// Policy fails closed. Always build one from Default.
type Policy struct {
	// min holds the minimum role per action. A nil map is legal and treated as
	// "administrator required" for every action (fail-closed).
	min map[Action]Role
}

// Default returns the platform's baseline authorization policy.
func Default() Policy {
	return Policy{min: map[Action]Role{
		ActionObserve:           RoleObserver,
		ActionJoin:              RoleObserver,
		ActionLeave:             RoleObserver,
		ActionKick:              RoleModerator,
		ActionTransferOwnership: RoleOwner,
		ActionCloseRoom:         RoleOwner,
		ActionModifySettings:    RoleOwner,
	}}
}

// With returns a copy of p with the requirement for action set to min. The
// receiver is not modified, so Policies can be safely shared and specialized
// per room.
func (p Policy) With(action Action, min Role) Policy {
	next := make(map[Action]Role, len(p.min)+1)
	for k, v := range p.min {
		next[k] = v
	}
	next[action] = min
	return Policy{min: next}
}

// Requirement returns the minimum role for an action. Unset actions require
// RoleAdministrator (fail-closed).
func (p Policy) Requirement(action Action) Role {
	if r, ok := p.min[action]; ok {
		return r
	}
	return RoleAdministrator
}

// Allows reports whether a principal with the given role may perform action.
func (p Policy) Allows(role Role, action Action) bool {
	return role.AtLeast(p.Requirement(action))
}

// Require returns nil if role may perform action, else an error wrapping
// ErrPermissionDenied with a descriptive message.
func (p Policy) Require(role Role, action Action) error {
	if p.Allows(role, action) {
		return nil
	}
	return fmt.Errorf("%w: %s requires at least %s, have %s",
		ErrPermissionDenied, action, p.Requirement(action), role)
}
