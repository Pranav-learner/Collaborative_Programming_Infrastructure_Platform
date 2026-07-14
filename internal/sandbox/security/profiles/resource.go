package profiles

import (
	"time"
)

type ResourceProfileID string

const (
	ProfileTiny   ResourceProfileID = "Tiny"
	ProfileSmall  ResourceProfileID = "Small"
	ProfileMedium ResourceProfileID = "Medium"
	ProfileLarge  ResourceProfileID = "Large"
	ProfileCustom ResourceProfileID = "Custom"
)

type ResourceProfile struct {
	ID                    ResourceProfileID `json:"id"`
	CPULimitShares        int64             `json:"cpu_limit_shares"`       // Docker cpu-shares (1024 = 1 core)
	CPUQuotaMicroseconds  int64             `json:"cpu_quota_microseconds"` // Docker cpu-quota (microseconds per period)
	MemoryLimitBytes      int64             `json:"memory_limit_bytes"`
	SwapLimitBytes        int64             `json:"swap_limit_bytes"`
	ExecutionTimeout      time.Duration     `json:"execution_timeout"`
	FileSizeLimitBytes    int64             `json:"file_size_limit_bytes"`
	OutputLimitBytes      int64             `json:"output_limit_bytes"`
	DiskLimitBytes        int64             `json:"disk_limit_bytes"`
	NetworkAllowed        bool              `json:"network_allowed"`
	ProcessLimit          int64             `json:"process_limit"`
	OpenFileLimit         int64             `json:"open_file_limit"`
	TempStorageLimitBytes int64             `json:"temp_storage_limit_bytes"`
	GPUSupport            bool              `json:"gpu_support"` // Reserved for future GPU support
}

func GetDefaultResourceProfile(id ResourceProfileID) ResourceProfile {
	switch id {
	case ProfileTiny:
		return ResourceProfile{
			ID:                    ProfileTiny,
			CPULimitShares:        256, // 0.25 core
			CPUQuotaMicroseconds:  25000,
			MemoryLimitBytes:      64 * 1024 * 1024, // 64MB
			SwapLimitBytes:        0,
			ExecutionTimeout:      2 * time.Second,
			FileSizeLimitBytes:    1 * 1024 * 1024,
			OutputLimitBytes:      512 * 1024,
			DiskLimitBytes:        10 * 1024 * 1024,
			NetworkAllowed:        false,
			ProcessLimit:          10,
			OpenFileLimit:         32,
			TempStorageLimitBytes: 5 * 1024 * 1024,
			GPUSupport:            false,
		}
	case ProfileSmall:
		return ResourceProfile{
			ID:                    ProfileSmall,
			CPULimitShares:        512, // 0.5 core
			CPUQuotaMicroseconds:  50000,
			MemoryLimitBytes:      128 * 1024 * 1024, // 128MB
			SwapLimitBytes:        0,
			ExecutionTimeout:      5 * time.Second,
			FileSizeLimitBytes:    5 * 1024 * 1024,
			OutputLimitBytes:      1 * 1024 * 1024,
			DiskLimitBytes:        50 * 1024 * 1024,
			NetworkAllowed:        false,
			ProcessLimit:          20,
			OpenFileLimit:         64,
			TempStorageLimitBytes: 10 * 1024 * 1024,
			GPUSupport:            false,
		}
	case ProfileMedium:
		return ResourceProfile{
			ID:                    ProfileMedium,
			CPULimitShares:        1024, // 1 core
			CPUQuotaMicroseconds:  100000,
			MemoryLimitBytes:      512 * 1024 * 1024, // 512MB
			SwapLimitBytes:        0,
			ExecutionTimeout:      15 * time.Second,
			FileSizeLimitBytes:    20 * 1024 * 1024,
			OutputLimitBytes:      5 * 1024 * 1024,
			DiskLimitBytes:        200 * 1024 * 1024,
			NetworkAllowed:        true,
			ProcessLimit:          50,
			OpenFileLimit:         128,
			TempStorageLimitBytes: 50 * 1024 * 1024,
			GPUSupport:            false,
		}
	case ProfileLarge:
		return ResourceProfile{
			ID:                    ProfileLarge,
			CPULimitShares:        2048, // 2 cores
			CPUQuotaMicroseconds:  200000,
			MemoryLimitBytes:      2048 * 1024 * 1024, // 2GB
			SwapLimitBytes:        512 * 1024 * 1024,  // 512MB
			ExecutionTimeout:      60 * time.Second,
			FileSizeLimitBytes:    100 * 1024 * 1024,
			OutputLimitBytes:      20 * 1024 * 1024,
			DiskLimitBytes:        1024 * 1024 * 1024, // 1GB
			NetworkAllowed:        true,
			ProcessLimit:          100,
			OpenFileLimit:         256,
			TempStorageLimitBytes: 250 * 1024 * 1024,
			GPUSupport:            false,
		}
	default:
		// Return Small as default fallback
		return GetDefaultResourceProfile(ProfileSmall)
	}
}
