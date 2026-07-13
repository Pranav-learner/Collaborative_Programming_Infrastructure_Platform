// Package room defines the runtime Room entity: the authoritative in-memory
// model of a single collaborative room and the atomic, authorized operations
// that mutate it.
//
// A Room owns its own sync.RWMutex and guards all of its state behind it. This
// is the cornerstone of the concurrency strategy: because each room carries an
// independent lock, operations on different rooms never contend, and the system
// scales to thousands of rooms without a global bottleneck (see the registry
// package for how rooms are sharded across the node).
//
// Every state-changing method performs validation, authorization, and mutation
// as a single critical section, so there is no check-then-act window a
// concurrent caller could exploit. Read methods return cloned value copies, so a
// caller can never reach in and mutate a room's internals by holding a reference
// to something the room returned.
//
// The Room is a pure domain object: it depends only on the leaf packages
// lifecycle and permissions. It does not emit events, record metrics, or persist
// itself — those cross-cutting concerns are orchestrated by the membership
// manager and the top-level room manager, which react to the results the Room
// returns. This keeps the entity testable in isolation and free of import
// cycles.
package room

import (
	"sync"
	"time"

	"cpip/internal/rooms/lifecycle"
	"cpip/internal/rooms/permissions"
)

// Visibility controls who may discover/enter a room. It is carried for future
// authorization layers (public rooms are listable/joinable by any authenticated
// user; private rooms require an invite/grant). The current module stores it but
// does not itself gate on it.
type Visibility uint8

const (
	// VisibilityPrivate rooms are reachable only by explicitly authorized users.
	VisibilityPrivate Visibility = iota
	// VisibilityPublic rooms are discoverable and joinable by any authenticated
	// user (subject to the join permission).
	VisibilityPublic
)

func (v Visibility) String() string {
	if v == VisibilityPublic {
		return "public"
	}
	return "private"
}

// Config is a room's tunable configuration. It is captured at creation (seeded
// from the node config, optionally overridden per room) and is immutable for the
// room's lifetime except through an authorized ModifySettings operation.
type Config struct {
	// MaxParticipants caps concurrent membership. Zero means unlimited.
	MaxParticipants int
	// IdleTimeout is how long without activity before the room becomes Idle.
	IdleTimeout time.Duration
	// ExpireTimeout is how long a room may remain Idle before it begins Expiring.
	ExpireTimeout time.Duration
	// RecoveryTimeout is how long a disconnected participant may remain a member
	// awaiting reconnection before eviction.
	RecoveryTimeout time.Duration
	// Visibility controls discoverability/joinability (future authorization).
	Visibility Visibility
}

// Params construct a Room.
type Params struct {
	ID       string
	Name     string
	OwnerID  string
	Config   Config
	Policy   permissions.Policy
	Metadata map[string]any
	// Owner connection binding, if the owner is connected at creation time. When
	// OwnerConnID is non-empty the owner is registered as Connected.
	OwnerSessionID string
	OwnerConnID    string
	// Now is the creation timestamp (injected for deterministic tests).
	Now time.Time
}

// Room is the runtime model of one collaborative room. All exported methods are
// safe for concurrent use.
type Room struct {
	machine lifecycle.Machine

	mu           sync.RWMutex
	id           string
	name         string
	ownerID      string
	state        lifecycle.State
	createdAt    time.Time
	lastActivity time.Time
	participants map[string]*Participant // keyed by UserID
	policy       permissions.Policy
	cfg          Config
	metadata     map[string]any

	// Reserved for later modules. These are deliberately typed as `any` and left
	// nil: the room owns their lifetime (guarded by mu) but stays decoupled from
	// their concrete types so this module does not depend on Yjs/CRDT, presence,
	// or execution packages.
	//
	//   document  — handle to the room's shared CRDT document (CRDT module).
	//   presence  — handle to the room's presence/awareness state (Presence module).
	//   execution — handle to the room's execution context/sandbox (Execution module).
	document  any
	presence  any
	execution any
}

// New constructs a Room with the owner registered as its first participant. The
// room starts in StateCreated; the caller (the room manager) is responsible for
// the initial transition (to Active if the owner is connected, else Waiting).
func New(p Params) *Room {
	owner := &Participant{
		UserID:    p.OwnerID,
		Role:      permissions.RoleOwner,
		SessionID: p.OwnerSessionID,
		ConnID:    p.OwnerConnID,
		JoinedAt:  p.Now,
		LastSeen:  p.Now,
		Connected: p.OwnerConnID != "",
	}
	r := &Room{
		machine:      lifecycle.NewMachine(),
		id:           p.ID,
		name:         p.Name,
		ownerID:      p.OwnerID,
		state:        lifecycle.StateCreated,
		createdAt:    p.Now,
		lastActivity: p.Now,
		participants: map[string]*Participant{p.OwnerID: owner},
		policy:       p.Policy,
		cfg:          p.Config,
	}
	if p.Metadata != nil {
		r.metadata = make(map[string]any, len(p.Metadata))
		for k, v := range p.Metadata {
			r.metadata[k] = v
		}
	}
	return r
}

// --- read accessors (all take the read lock or are immutable) ---

// ID returns the room's immutable identifier.
func (r *Room) ID() string { return r.id }

// Name returns the room's display name.
func (r *Room) Name() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.name
}

// OwnerID returns the current owner's user id.
func (r *Room) OwnerID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ownerID
}

// State returns the room's current lifecycle state.
func (r *Room) State() lifecycle.State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// CreatedAt returns the room's creation time.
func (r *Room) CreatedAt() time.Time { return r.createdAt }

// LastActivity returns the time of the most recent activity.
func (r *Room) LastActivity() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastActivity
}

// Config returns the room's configuration snapshot.
func (r *Room) Config() Config {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg
}

// ParticipantCount returns the number of members (connected or awaiting
// recovery).
func (r *Room) ParticipantCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.participants)
}

// ConnectedCount returns the number of members that currently have a live
// connection.
func (r *Room) ConnectedCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.connectedCountLocked()
}

// Participant returns a copy of the named member, or false if absent.
func (r *Room) Participant(userID string) (Participant, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.participants[userID]
	if !ok {
		return Participant{}, false
	}
	return p.clone(), true
}

// Participants returns cloned copies of all members.
func (r *Room) Participants() []Participant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Participant, 0, len(r.participants))
	for _, p := range r.participants {
		out = append(out, p.clone())
	}
	return out
}

// View returns an immutable, self-contained snapshot of the room suitable for
// handing to external consumers (the public API, persistence, presence seeding).
func (r *Room) View() View {
	r.mu.RLock()
	defer r.mu.RUnlock()
	parts := make([]Participant, 0, len(r.participants))
	for _, p := range r.participants {
		parts = append(parts, p.clone())
	}
	var meta map[string]any
	if r.metadata != nil {
		meta = make(map[string]any, len(r.metadata))
		for k, v := range r.metadata {
			meta[k] = v
		}
	}
	return View{
		ID:           r.id,
		Name:         r.name,
		OwnerID:      r.ownerID,
		State:        r.state,
		CreatedAt:    r.createdAt,
		LastActivity: r.lastActivity,
		Participants: parts,
		Config:       r.cfg,
		Metadata:     meta,
	}
}

// --- authorization ---

// Authorize checks whether actor may perform action in this room, using the
// room's policy and the actor's current role. It returns ErrNotAParticipant if
// the actor is not a member, or a permissions.ErrPermissionDenied-wrapped error
// if the role is insufficient.
func (r *Room) Authorize(actorID string, action permissions.Action) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.authorizeLocked(actorID, action)
}

func (r *Room) authorizeLocked(actorID string, action permissions.Action) error {
	actor, ok := r.participants[actorID]
	if !ok {
		return ErrNotAParticipant
	}
	return r.policy.Require(actor.Role, action)
}

// --- mutations ---

// JoinRequest describes a prospective membership.
type JoinRequest struct {
	UserID    string
	Role      permissions.Role // requested role; typically RoleParticipant or RoleObserver
	SessionID string
	ConnID    string
	Metadata  map[string]any
}

// JoinResult reports the outcome of a Join.
type JoinResult struct {
	Participant   Participant
	Reconnected   bool // true when an existing disconnected member reconnected
	StateChanged  bool
	PreviousState lifecycle.State
	NewState      lifecycle.State
}

// Join admits a participant, atomically enforcing the closed/full/duplicate
// invariants. If the user is an existing but disconnected member, Join is a
// reconnect: it restores the live binding and keeps the prior role
// (Reconnected == true). Joining a room drives it toward Active.
func (r *Room) Join(req JoinRequest, now time.Time) (JoinResult, error) {
	if !req.Role.Valid() {
		return JoinResult{}, ErrInvalidRole
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == lifecycle.StateClosed || r.state == lifecycle.StateDestroyed {
		return JoinResult{}, ErrRoomClosed
	}

	prev := r.state
	if existing, ok := r.participants[req.UserID]; ok {
		if existing.Connected {
			return JoinResult{}, ErrDuplicateParticipant
		}
		// Reconnect: restore the live binding, preserve role and JoinedAt.
		existing.Connected = true
		existing.SessionID = req.SessionID
		existing.ConnID = req.ConnID
		existing.LastSeen = now
		if req.Metadata != nil {
			existing.Metadata = req.Metadata
		}
		r.lastActivity = now
		changed := r.driveActiveLocked(now)
		return JoinResult{
			Participant:   existing.clone(),
			Reconnected:   true,
			StateChanged:  changed,
			PreviousState: prev,
			NewState:      r.state,
		}, nil
	}

	if r.cfg.MaxParticipants > 0 && len(r.participants) >= r.cfg.MaxParticipants {
		return JoinResult{}, ErrRoomFull
	}

	p := &Participant{
		UserID:    req.UserID,
		Role:      req.Role,
		SessionID: req.SessionID,
		ConnID:    req.ConnID,
		JoinedAt:  now,
		LastSeen:  now,
		Connected: true,
		Metadata:  req.Metadata,
	}
	r.participants[req.UserID] = p
	r.lastActivity = now
	changed := r.driveActiveLocked(now)
	return JoinResult{
		Participant:   p.clone(),
		StateChanged:  changed,
		PreviousState: prev,
		NewState:      r.state,
	}, nil
}

// LeaveResult reports the outcome of a Leave/Remove.
type LeaveResult struct {
	Participant   Participant
	OwnerLeft     bool // the departed member was the owner
	Empty         bool // no members remain
	StateChanged  bool
	PreviousState lifecycle.State
	NewState      lifecycle.State
}

// Leave removes a member voluntarily. It returns ErrParticipantNotFound if the
// user is not a member. When the last member leaves, the room drifts back toward
// Waiting; the OwnerLeft flag lets the caller apply ownership-succession policy.
func (r *Room) Leave(userID string, now time.Time) (LeaveResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.removeLocked(userID, now)
}

// Remove forcibly removes target on behalf of actor (a "kick"), enforcing the
// kick permission under the lock. The owner cannot be removed this way unless
// the actor holds administrator authority; transfer ownership first.
func (r *Room) Remove(actorID, targetID string, now time.Time) (LeaveResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.authorizeLocked(actorID, permissions.ActionKick); err != nil {
		return LeaveResult{}, err
	}
	if targetID == r.ownerID {
		actor := r.participants[actorID]
		if actor == nil || !actor.Role.AtLeast(permissions.RoleAdministrator) {
			return LeaveResult{}, ErrCannotRemoveOwner
		}
	}
	return r.removeLocked(targetID, now)
}

// removeLocked deletes a participant and applies the resulting state drift. The
// caller must hold the write lock.
func (r *Room) removeLocked(userID string, now time.Time) (LeaveResult, error) {
	p, ok := r.participants[userID]
	if !ok {
		return LeaveResult{}, ErrParticipantNotFound
	}
	prev := r.state
	delete(r.participants, userID)
	r.lastActivity = now

	empty := len(r.participants) == 0
	changed := false
	// If nobody is connected anymore, the room drifts back to Waiting (from an
	// active/idle state) to await participants; the janitor handles expiry.
	if r.connectedCountLocked() == 0 &&
		(r.state == lifecycle.StateActive || r.state == lifecycle.StateIdle) {
		changed = r.transitionLocked(lifecycle.StateWaiting, now) == nil
	}
	return LeaveResult{
		Participant:   p.clone(),
		OwnerLeft:     userID == r.ownerID,
		Empty:         empty,
		StateChanged:  changed,
		PreviousState: prev,
		NewState:      r.state,
	}, nil
}

// TransferResult reports the outcome of an ownership transfer.
type TransferResult struct {
	PreviousOwner string
	NewOwner      string
}

// TransferOwnership hands ownership from actor to newOwnerID, enforcing the
// transfer permission under the lock. The new owner must already be a member.
// The previous owner is demoted to participant.
func (r *Room) TransferOwnership(actorID, newOwnerID string, now time.Time) (TransferResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.authorizeLocked(actorID, permissions.ActionTransferOwnership); err != nil {
		return TransferResult{}, err
	}
	if newOwnerID == r.ownerID {
		return TransferResult{}, ErrAlreadyOwner
	}
	newOwner, ok := r.participants[newOwnerID]
	if !ok {
		return TransferResult{}, ErrParticipantNotFound
	}
	prevOwnerID := r.ownerID
	if cur, ok := r.participants[prevOwnerID]; ok {
		cur.Role = permissions.RoleParticipant
	}
	newOwner.Role = permissions.RoleOwner
	r.ownerID = newOwnerID
	r.lastActivity = now
	return TransferResult{PreviousOwner: prevOwnerID, NewOwner: newOwnerID}, nil
}

// SetConnected updates a member's live-connection state. It is used by the
// manager on disconnect (connected=false, opening the recovery window) and by
// presence. It returns the updated participant, whether the participant existed,
// and whether the room's connected count just dropped to zero (a hint the caller
// may use to begin idle/recovery handling).
func (r *Room) SetConnected(userID string, connected bool, connID, sessionID string, now time.Time) (Participant, bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.participants[userID]
	if !ok {
		return Participant{}, false, false
	}
	p.Connected = connected
	p.LastSeen = now
	if connected {
		p.ConnID = connID
		p.SessionID = sessionID
		r.lastActivity = now
	} else {
		p.ConnID = ""
	}
	nowEmpty := !connected && r.connectedCountLocked() == 0
	return p.clone(), true, nowEmpty
}

// Touch records activity at time now, refreshing LastActivity and, if the room
// had drifted to Idle/Expiring, pulling it back to Active. It returns whether
// the state changed.
func (r *Room) Touch(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastActivity = now
	return r.driveActiveLocked(now)
}

// Transition applies an explicit lifecycle transition, validating it against the
// state machine. It is used by the janitor (Active→Idle→Expiring→Closed) and by
// explicit close. Self-transitions are no-ops that still refresh activity for
// forward moves.
func (r *Room) Transition(to lifecycle.State, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.transitionLocked(to, now)
}

func (r *Room) transitionLocked(to lifecycle.State, now time.Time) error {
	if err := r.machine.Validate(r.state, to); err != nil {
		return err
	}
	if r.state == to {
		return nil
	}
	r.state = to
	if to == lifecycle.StateActive {
		r.lastActivity = now
	}
	return nil
}

// driveActiveLocked pulls a room that has a reason to be active (a join, a
// reconnect, activity) into StateActive from any state that legally reaches it.
// Returns whether the state changed. The caller must hold the write lock.
func (r *Room) driveActiveLocked(now time.Time) bool {
	if r.state == lifecycle.StateActive {
		return false
	}
	if r.machine.CanTransition(r.state, lifecycle.StateActive) {
		r.state = lifecycle.StateActive
		r.lastActivity = now
		return true
	}
	return false
}

func (r *Room) connectedCountLocked() int {
	n := 0
	for _, p := range r.participants {
		if p.Connected {
			n++
		}
	}
	return n
}

// --- reserved future-module accessors (guarded by the room lock) ---

// SetDocumentRef attaches the room's shared-document handle (CRDT module). The
// room stores it opaquely.
func (r *Room) SetDocumentRef(doc any) {
	r.mu.Lock()
	r.document = doc
	r.mu.Unlock()
}

// DocumentRef returns the room's shared-document handle, or nil.
func (r *Room) DocumentRef() any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.document
}

// SetPresenceRef attaches the room's presence-state handle (Presence module).
func (r *Room) SetPresenceRef(pres any) {
	r.mu.Lock()
	r.presence = pres
	r.mu.Unlock()
}

// PresenceRef returns the room's presence-state handle, or nil.
func (r *Room) PresenceRef() any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.presence
}

// SetExecutionRef attaches the room's execution-context handle (Execution module).
func (r *Room) SetExecutionRef(exec any) {
	r.mu.Lock()
	r.execution = exec
	r.mu.Unlock()
}

// ExecutionRef returns the room's execution-context handle, or nil.
func (r *Room) ExecutionRef() any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.execution
}
