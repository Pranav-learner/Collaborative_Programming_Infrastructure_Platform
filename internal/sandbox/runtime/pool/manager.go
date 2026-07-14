package pool

import (
	"fmt"
	"sync"
	"sync/atomic"

	"cpip/internal/sandbox/runtime"
)

// RuntimeInstance encapsulates a single adapter instance with usage tracking.
type RuntimeInstance struct {
	InstanceID string
	RuntimeID  string
	Adapter    runtime.RuntimeAdapter
	leases     int64
}

// Lease increments lease count.
func (i *RuntimeInstance) Lease() {
	atomic.AddInt64(&i.leases, 1)
}

// Release decrements lease count.
func (i *RuntimeInstance) Release() {
	if atomic.AddInt64(&i.leases, -1) < 0 {
		atomic.StoreInt64(&i.leases, 0)
	}
}

// Leases returns active lease count.
func (i *RuntimeInstance) Leases() int64 {
	return atomic.LoadInt64(&i.leases)
}

// RuntimePool manages collections of adapters for load distribution.
type RuntimePool struct {
	mu        sync.RWMutex
	instances map[string][]*RuntimeInstance
}

// NewRuntimePool instantiates a RuntimePool.
func NewRuntimePool() *RuntimePool {
	return &RuntimePool{
		instances: make(map[string][]*RuntimeInstance),
	}
}

// AddInstance adds a runtime adapter instance to the pool.
func (p *RuntimePool) AddInstance(runtimeID string, instanceID string, adapter runtime.RuntimeAdapter) {
	p.mu.Lock()
	defer p.mu.Unlock()

	inst := &RuntimeInstance{
		InstanceID: instanceID,
		RuntimeID:  runtimeID,
		Adapter:    adapter,
	}
	p.instances[runtimeID] = append(p.instances[runtimeID], inst)
}

// Acquire chooses the least-loaded instance for the specified runtime.
func (p *RuntimePool) Acquire(runtimeID string) (runtime.RuntimeAdapter, string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	instances, ok := p.instances[runtimeID]
	if !ok || len(instances) == 0 {
		return nil, "", fmt.Errorf("no instances available in pool for runtime %s", runtimeID)
	}

	// Least-connection load balancing
	var selected *RuntimeInstance = instances[0]
	minLeases := selected.Leases()

	for _, inst := range instances {
		leases := inst.Leases()
		if leases < minLeases {
			minLeases = leases
			selected = inst
		}
	}

	selected.Lease()
	return selected.Adapter, selected.InstanceID, nil
}

// Release releases a leased instance by its ID.
func (p *RuntimePool) Release(runtimeID string, instanceID string) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	instances, ok := p.instances[runtimeID]
	if !ok {
		return
	}

	for _, inst := range instances {
		if inst.InstanceID == instanceID {
			inst.Release()
			break
		}
	}
}

// GetStats returns usage statistics for all instances in the pool.
func (p *RuntimePool) GetStats() map[string]int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[string]int64)
	for rID, instances := range p.instances {
		var totalLeases int64
		for _, inst := range instances {
			totalLeases += inst.Leases()
		}
		stats[rID] = totalLeases
	}
	return stats
}
