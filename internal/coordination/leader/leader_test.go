package leader_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/keys"
	"cpip/internal/coordination/leader"
	"cpip/internal/coordination/logger"
)

func testCfg() config.Leader {
	return config.Leader{Lease: 200 * time.Millisecond, RenewInterval: 40 * time.Millisecond, RetryInterval: 20 * time.Millisecond}
}

func eventually(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func mkManager(be backend.Backend, kb keys.Builder, id string) *leader.Manager {
	return leader.New(leader.Params{
		Backend: be, Keys: kb, Config: testCfg(), CandidateID: id,
		Events: events.NewBus(), Logger: logger.New(nil),
	})
}

func TestSingleLeaderElectedAmongCandidates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	be := backend.NewMemory()
	kb := keys.New("test", "c1")

	a := mkManager(be, kb, "A")
	b := mkManager(be, kb, "B")
	cc := mkManager(be, kb, "C")
	ea := a.Campaign(ctx, "s")
	eb := b.Campaign(ctx, "s")
	ec := cc.Campaign(ctx, "s")
	defer a.StopAll()
	defer b.StopAll()
	defer cc.StopAll()

	countLeaders := func() int {
		n := 0
		for _, e := range []*leader.Election{ea, eb, ec} {
			if e.IsLeader() {
				n++
			}
		}
		return n
	}
	eventually(t, 2*time.Second, func() bool { return countLeaders() == 1 })
	// Hold: leadership must remain unique across several renew cycles.
	for i := 0; i < 10; i++ {
		if countLeaders() != 1 {
			t.Fatalf("expected exactly one leader, got %d", countLeaders())
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func TestLeadershipFailoverOnResign(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	be := backend.NewMemory()
	kb := keys.New("test", "c2")

	a := mkManager(be, kb, "A")
	b := mkManager(be, kb, "B")
	ea := a.Campaign(ctx, "s")
	eb := b.Campaign(ctx, "s")
	defer a.StopAll()
	defer b.StopAll()

	eventually(t, 2*time.Second, func() bool { return ea.IsLeader() || eb.IsLeader() })

	var leaderE, followerE *leader.Election
	if ea.IsLeader() {
		leaderE, followerE = ea, eb
	} else {
		leaderE, followerE = eb, ea
	}
	if err := leaderE.Resign(ctx); err != nil {
		t.Fatalf("resign: %v", err)
	}
	// The follower should take over.
	eventually(t, 2*time.Second, func() bool { return followerE.IsLeader() })
	if leaderE.IsLeader() {
		t.Fatalf("resigned election should no longer be leader")
	}
}

func TestLeadershipTransfer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	be := backend.NewMemory()
	kb := keys.New("test", "c3")

	a := mkManager(be, kb, "A")
	b := mkManager(be, kb, "B")
	ea := a.Campaign(ctx, "s")
	eb := b.Campaign(ctx, "s")
	defer a.StopAll()
	defer b.StopAll()

	eventually(t, 2*time.Second, func() bool { return ea.IsLeader() || eb.IsLeader() })
	var leaderE, otherE *leader.Election
	var otherID string
	if ea.IsLeader() {
		leaderE, otherE, otherID = ea, eb, "B"
	} else {
		leaderE, otherE, otherID = eb, ea, "A"
	}
	if err := leaderE.Transfer(ctx, otherID); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	eventually(t, 2*time.Second, func() bool { return otherE.IsLeader() })
}

func TestLeadershipLossDetectionCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	be := backend.NewMemory()
	kb := keys.New("test", "c4")

	a := mkManager(be, kb, "A")
	ea := a.Campaign(ctx, "s")
	defer a.StopAll()

	var lost atomic.Bool
	ea.OnLost(func(context.Context) { lost.Store(true) })
	eventually(t, 2*time.Second, func() bool { return ea.IsLeader() })

	// Forcibly delete the lease key out from under the leader; the next renew
	// fails and loss must be detected.
	_, _ = be.Delete(ctx, kb.LeaderKey("s"))
	// Another candidate grabs it so renew's CAS definitively fails.
	_, _ = be.SetNX(ctx, kb.LeaderKey("s"), "someone-else", time.Minute)

	eventually(t, 2*time.Second, func() bool { return lost.Load() && !ea.IsLeader() })
}
