package profiles

import (
	"errors"
	"sync"
	"time"

	"cpip/internal/languages/config"
)

// ProfileRegistry manages predefined execution and resource profiles.
type ProfileRegistry struct {
	mu        sync.RWMutex
	execs     map[string]ExecutionProfile
	resources map[string]ResourceProfile
}

// ExecutionProfile represents resource limits configurations for executions.
type ExecutionProfile struct {
	Name        string        `json:"name"`
	Timeout     time.Duration `json:"timeout"`
	MemoryLimit int64         `json:"memory_limit"`
	CPULimit    int           `json:"cpu_limit"`
	FileLimit   int64         `json:"file_limit"`
	OutputLimit int64         `json:"output_limit"`
}

// ResourceProfile maps directly to container resource policies.
type ResourceProfile struct {
	Name          string        `json:"name"`
	CPUMillicores int           `json:"cpu_millicores"`
	MemoryBytes   int64         `json:"memory_bytes"`
	PidsLimit     int           `json:"pids_limit"`
	TmpfsBytes    int64         `json:"tmpfs_bytes"`
	WallTimeout   time.Duration `json:"wall_timeout"`
}

// NewProfileRegistry initializes a profile registry seeded with standard defaults.
func NewProfileRegistry(cfg config.Config) *ProfileRegistry {
	r := &ProfileRegistry{
		execs:     make(map[string]ExecutionProfile),
		resources: make(map[string]ResourceProfile),
	}

	// Seed Execution Profiles
	r.RegisterExecution(ExecutionProfile{
		Name:        "default",
		Timeout:     cfg.ProfileDefaults.Timeout,
		MemoryLimit: cfg.ProfileDefaults.MemoryLimit,
		CPULimit:    cfg.ProfileDefaults.CPULimit,
		FileLimit:   cfg.ProfileDefaults.FileLimit,
		OutputLimit: cfg.ProfileDefaults.OutputLimit,
	})

	r.RegisterExecution(ExecutionProfile{
		Name:        "cpu_intensive",
		Timeout:     30 * time.Second,
		MemoryLimit: 512 * 1024 * 1024,
		CPULimit:    4000,
		FileLimit:   50 * 1024 * 1024,
		OutputLimit: 2 * 1024 * 1024,
	})

	r.RegisterExecution(ExecutionProfile{
		Name:        "memory_intensive",
		Timeout:     15 * time.Second,
		MemoryLimit: 1024 * 1024 * 1024,
		CPULimit:    1000,
		FileLimit:   50 * 1024 * 1024,
		OutputLimit: 2 * 1024 * 1024,
	})

	r.RegisterExecution(ExecutionProfile{
		Name:        "interactive",
		Timeout:     5 * time.Second,
		MemoryLimit: 256 * 1024 * 1024,
		CPULimit:    1000,
		FileLimit:   5 * 1024 * 1024,
		OutputLimit: 512 * 1024,
	})

	r.RegisterExecution(ExecutionProfile{
		Name:        "sandbox_ready",
		Timeout:     10 * time.Second,
		MemoryLimit: 128 * 1024 * 1024,
		CPULimit:    500,
		FileLimit:   1 * 1024 * 1024,
		OutputLimit: 256 * 1024,
	})

	// Seed Resource Profiles
	r.RegisterResource(ResourceProfile{
		Name:          "small",
		CPUMillicores: cfg.ResourceDefaults.Small.CPUMillicores,
		MemoryBytes:   cfg.ResourceDefaults.Small.MemoryBytes,
		PidsLimit:     cfg.ResourceDefaults.Small.PidsLimit,
		TmpfsBytes:    cfg.ResourceDefaults.Small.TmpfsBytes,
		WallTimeout:   cfg.ResourceDefaults.Small.WallTimeout,
	})

	r.RegisterResource(ResourceProfile{
		Name:          "medium",
		CPUMillicores: cfg.ResourceDefaults.Medium.CPUMillicores,
		MemoryBytes:   cfg.ResourceDefaults.Medium.MemoryBytes,
		PidsLimit:     cfg.ResourceDefaults.Medium.PidsLimit,
		TmpfsBytes:    cfg.ResourceDefaults.Medium.TmpfsBytes,
		WallTimeout:   cfg.ResourceDefaults.Medium.WallTimeout,
	})

	r.RegisterResource(ResourceProfile{
		Name:          "large",
		CPUMillicores: cfg.ResourceDefaults.Large.CPUMillicores,
		MemoryBytes:   cfg.ResourceDefaults.Large.MemoryBytes,
		PidsLimit:     cfg.ResourceDefaults.Large.PidsLimit,
		TmpfsBytes:    cfg.ResourceDefaults.Large.TmpfsBytes,
		WallTimeout:   cfg.ResourceDefaults.Large.WallTimeout,
	})

	return r
}

func (r *ProfileRegistry) RegisterExecution(ep ExecutionProfile) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.execs[ep.Name] = ep
}

func (r *ProfileRegistry) RegisterResource(rp ResourceProfile) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resources[rp.Name] = rp
}

func (r *ProfileRegistry) GetExecution(name string) (ExecutionProfile, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.execs[name]
	if !ok {
		return ExecutionProfile{}, errors.New("execution profile not found")
	}
	return p, nil
}

func (r *ProfileRegistry) GetResource(name string) (ResourceProfile, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.resources[name]
	if !ok {
		return ResourceProfile{}, errors.New("resource profile not found")
	}
	return p, nil
}
