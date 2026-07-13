package job

import "testing"

func TestHappyPathTransitions(t *testing.T) {
	path := []State{
		StatePending, StateValidated, StateQueued, StateDispatched,
		StateRunning, StateStreaming, StateCompleted, StateArchived,
	}
	for i := 0; i+1 < len(path); i++ {
		if !CanTransition(path[i], path[i+1]) {
			t.Errorf("expected %s → %s to be legal", path[i], path[i+1])
		}
	}
}

func TestIllegalTransitions(t *testing.T) {
	illegal := [][2]State{
		{StatePending, StateRunning},    // cannot skip validation/queue
		{StateCompleted, StateRunning},  // terminal outcome cannot resume
		{StateArchived, StatePending},   // archived is terminal
		{StateQueued, StateStreaming},   // must dispatch+run first
		{StateCancelled, StateRetrying}, // cancelled is not retryable
		{StateCompleted, StateFailed},   // outcomes are exclusive
	}
	for _, p := range illegal {
		if CanTransition(p[0], p[1]) {
			t.Errorf("expected %s → %s to be illegal", p[0], p[1])
		}
	}
}

func TestSelfTransitionRejected(t *testing.T) {
	if CanTransition(StateRunning, StateRunning) {
		t.Error("self-transition should be rejected by the state machine")
	}
}

func TestRetryPath(t *testing.T) {
	if !CanTransition(StateFailed, StateRetrying) || !CanTransition(StateTimedOut, StateRetrying) {
		t.Fatal("failed/timed-out must be retryable")
	}
	if !CanTransition(StateRetrying, StateQueued) {
		t.Fatal("retrying must re-enter the queue")
	}
}

func TestStatePredicates(t *testing.T) {
	if !StateArchived.IsTerminal() {
		t.Error("archived must be terminal")
	}
	if StateRunning.IsTerminal() {
		t.Error("running must not be terminal")
	}
	finished := []State{StateCompleted, StateFailed, StateTimedOut, StateCancelled, StateArchived}
	for _, s := range finished {
		if !s.IsFinished() {
			t.Errorf("%s must be finished", s)
		}
		if s.IsActive() {
			t.Errorf("%s must not be active", s)
		}
	}
	if !StateQueued.IsActive() {
		t.Error("queued must be active")
	}
	if !StateFailed.CanRetry() || StateCompleted.CanRetry() {
		t.Error("only failed/timed-out are retryable")
	}
}

func TestOutcomeFor(t *testing.T) {
	cases := map[State]Outcome{
		StateCompleted: OutcomeSuccess,
		StateFailed:    OutcomeFailure,
		StateTimedOut:  OutcomeTimeout,
		StateCancelled: OutcomeCancelled,
		StateRunning:   OutcomeNone,
	}
	for s, want := range cases {
		if got := OutcomeFor(s); got != want {
			t.Errorf("OutcomeFor(%s) = %s, want %s", s, got, want)
		}
	}
}

func TestStateStringStable(t *testing.T) {
	if StateTimedOut.String() != "timed_out" || StatePending.String() != "pending" {
		t.Error("state strings changed unexpectedly")
	}
}
