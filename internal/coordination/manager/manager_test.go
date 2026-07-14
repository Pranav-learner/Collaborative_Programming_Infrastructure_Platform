package manager_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cpip/internal/coordination/config"
	"cpip/internal/coordination/discovery"
	"cpip/internal/coordination/leader"
	"cpip/internal/coordination/locks"
	"cpip/internal/coordination/manager"
	"cpip/internal/coordination/types"
)

func testConfig(nodeID string) config.Config {
	cfg := config.Default()
	cfg.Node = config.NodeIdentity{
		ID: nodeID, Name: nodeID, Address: "local://" + nodeID, Role: types.RoleCoordinator,
		Capabilities: []string{"coordination"},
	}
	cfg.Heartbeat.Interval = 20 * time.Millisecond
	cfg.Heartbeat.Timeout = 40 * time.Millisecond
	cfg.Heartbeat.Expiry = 120 * time.Millisecond
	cfg.Heartbeat.MonitorInterval = 15 * time.Millisecond
	cfg.Discovery.RefreshInterval = 25 * time.Millisecond
	return cfg
}

func newCoordinator(t *testing.T, nodeID string) *manager.Coordinator {
	t.Helper()
	c, err := manager.New(manager.Params{Config: testConfig(nodeID)})
	if err != nil {
		t.Fatalf("manager.New: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

func worker(id string, caps ...string) *types.Node {
	return &types.Node{
		ID: id, Name: id, Address: "local://" + id, Role: types.RoleWorker,
		Capabilities: caps, Load: types.Load{Capacity: 10},
	}
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

func TestSelfJoinsClusterOnStart(t *testing.T) {
	c := newCoordinator(t, "self")
	self := c.Self()
	if self.Status != types.StatusActive {
		t.Fatalf("self should be Active after start, got %s", self.Status)
	}
	if c.Cluster().Size() != 1 {
		t.Fatalf("cluster should contain just self, got %d", c.Cluster().Size())
	}
}

func TestRegisterAndDiscover(t *testing.T) {
	c := newCoordinator(t, "self")
	ctx := context.Background()

	_, err := c.Register(ctx, worker("w1", "python", "docker"))
	if err != nil {
		t.Fatalf("register w1: %v", err)
	}
	_, _ = c.Register(ctx, worker("w2", "python"))
	_, _ = c.Register(ctx, worker("w3", "go"))

	// Discover python-capable workers.
	nodes, err := c.Discover(ctx, discovery.Query{Role: types.RoleWorker, Capabilities: []string{"python"}, RequireSchedulable: true})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 python workers, got %d", len(nodes))
	}
	// Discovery of a missing capability yields ErrNoCandidates.
	if _, err := c.Discover(ctx, discovery.Query{Capabilities: []string{"gpu"}}); !errors.Is(err, types.ErrNoCandidates) {
		t.Fatalf("expected no candidates, got %v", err)
	}
}

func TestLoadAwareDiscoveryRanksLeastLoaded(t *testing.T) {
	c := newCoordinator(t, "self")
	ctx := context.Background()

	busy := worker("busy", "python")
	busy.Load = types.Load{Capacity: 10, ActiveJobs: 9}
	idle := worker("idle", "python")
	idle.Load = types.Load{Capacity: 10, ActiveJobs: 1}
	_, _ = c.Register(ctx, busy)
	_, _ = c.Register(ctx, idle)

	best, err := c.Discovery().LeastLoaded(ctx, types.RoleWorker)
	if err != nil {
		t.Fatal(err)
	}
	if best.ID != "idle" {
		t.Fatalf("least-loaded should be 'idle', got %s", best.ID)
	}
}

func TestReconnectBumpsIncarnation(t *testing.T) {
	c := newCoordinator(t, "self")
	ctx := context.Background()

	first, _ := c.Register(ctx, worker("w1", "python"))
	if first.Incarnation != 1 {
		t.Fatalf("first incarnation = %d, want 1", first.Incarnation)
	}
	second, _ := c.Register(ctx, worker("w1", "python"))
	if second.Incarnation != 2 {
		t.Fatalf("reconnect incarnation = %d, want 2", second.Incarnation)
	}
	if second.Stats.Reconnects != 1 {
		t.Fatalf("reconnect count = %d, want 1", second.Stats.Reconnects)
	}
}

func TestHeartbeatExpiryEvictsDeadNode(t *testing.T) {
	c := newCoordinator(t, "self")
	ctx := context.Background()

	_, _ = c.Register(ctx, worker("ghost", "python"))
	// 'ghost' never heartbeats; the monitor must evict it after Expiry, while
	// 'self' (which auto-beats) survives.
	eventually(t, 2*time.Second, func() bool {
		_, err := c.Cluster().Node(ctx, "ghost")
		return errors.Is(err, types.ErrNodeNotFound)
	})
	if _, err := c.Cluster().Node(ctx, "self"); err != nil {
		t.Fatalf("self should still be a member: %v", err)
	}
}

func TestGracefulLeave(t *testing.T) {
	c := newCoordinator(t, "self")
	ctx := context.Background()
	_, _ = c.Register(ctx, worker("w1"))
	if err := c.Deregister(ctx, "w1"); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	if _, err := c.Cluster().Node(ctx, "w1"); !errors.Is(err, types.ErrNodeNotFound) {
		t.Fatalf("w1 should be gone after leave, got %v", err)
	}
}

func TestLeaderElectionThroughCoordinator(t *testing.T) {
	c := newCoordinator(t, "self")
	ctx := context.Background()
	c.Campaign(ctx, leader.DefaultScope)
	eventually(t, 2*time.Second, func() bool { return c.IsLeader(leader.DefaultScope) })

	id, err := c.LeaderID(ctx, leader.DefaultScope)
	if err != nil {
		t.Fatalf("leader id: %v", err)
	}
	if id != "self" {
		t.Fatalf("leader should be 'self', got %s", id)
	}
}

func TestDistributedLockThroughCoordinator(t *testing.T) {
	c := newCoordinator(t, "self")
	ctx := context.Background()
	l, err := c.AcquireLock(ctx, "critical", &locks.Options{Lease: time.Second})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := c.Locks().TryAcquire(ctx, "critical", nil); !errors.Is(err, types.ErrLockNotAcquired) {
		t.Fatalf("expected contention, got %v", err)
	}
	_ = l.Release(ctx)
}

func TestConcurrentRegistration(t *testing.T) {
	c := newCoordinator(t, "self")
	ctx := context.Background()

	const n = 200
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.Register(ctx, worker(fmt.Sprintf("w%d", i), "python"))
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent register: %v", err)
		}
	}
	// self + n workers, and none lost to a concurrent sync/refresh.
	eventually(t, 2*time.Second, func() bool { return c.Cluster().Size() == n+1 })
}

func TestConcurrentDiscovery(t *testing.T) {
	c := newCoordinator(t, "self")
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_, _ = c.Register(ctx, worker(fmt.Sprintf("w%d", i), "python"))
	}

	const readers = 100
	var wg sync.WaitGroup
	errCh := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodes, err := c.Discover(ctx, discovery.Query{Capabilities: []string{"python"}})
			if err != nil {
				errCh <- err
				return
			}
			if len(nodes) == 0 {
				errCh <- errors.New("no nodes discovered")
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent discovery: %v", err)
		}
	}
}

func TestConcurrentLocksAndRegistration(t *testing.T) {
	// Stress: registration, discovery, and lock traffic all at once.
	c := newCoordinator(t, "self")
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 3 {
			case 0:
				_, _ = c.Register(ctx, worker(fmt.Sprintf("s%d", i), "python"))
			case 1:
				_, _ = c.Discover(ctx, discovery.Query{Role: types.RoleWorker})
			case 2:
				if l, err := c.Locks().TryAcquire(ctx, fmt.Sprintf("r%d", i%5), &locks.Options{Lease: 200 * time.Millisecond}); err == nil {
					_ = l.Release(ctx)
				}
			}
		}(i)
	}
	wg.Wait()
	if !c.HealthReport(ctx).BackendUp {
		t.Fatalf("backend should be healthy after stress")
	}
}
