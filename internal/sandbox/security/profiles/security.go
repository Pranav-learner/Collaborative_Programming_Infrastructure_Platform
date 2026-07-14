package profiles

type SecurityProfileID string

const (
	ProfileDefault      SecurityProfileID = "Default"
	ProfileReadOnly     SecurityProfileID = "ReadOnly"
	ProfileRestricted   SecurityProfileID = "Restricted"
	ProfileHighSecurity SecurityProfileID = "HighSecurity"
	ProfileInteractive  SecurityProfileID = "Interactive"
)

type FilesystemPolicy struct {
	ReadOnlyRoot      bool     `json:"read_only_root"`
	WritableWorkspace bool     `json:"writable_workspace"`
	AllowedMountPaths []string `json:"allowed_mount_paths"`
	TempDirsMaxSizeMB int64    `json:"temp_dirs_max_size_mb"`
}

type NetworkPolicy struct {
	Mode               string   `json:"mode"` // "none", "bridge", "isolated"
	AllowedOutboundIPs []string `json:"allowed_outbound_ips,omitempty"`
}

type CapabilityPolicy struct {
	DropCapabilities  []string `json:"drop_capabilities"`
	AllowCapabilities []string `json:"allow_capabilities"`
}

type UserPolicy struct {
	RunAsNonRoot bool `json:"run_as_non_root"`
	UID          int  `json:"uid"`
	GID          int  `json:"gid"`
}

type EnvironmentPolicy struct {
	BlockedVariables []string `json:"blocked_variables"`
	AllowedVariables []string `json:"allowed_variables"`
}

type SecurityProfile struct {
	ID                    SecurityProfileID `json:"id"`
	Filesystem            FilesystemPolicy  `json:"filesystem"`
	Network               NetworkPolicy     `json:"network"`
	Capabilities          CapabilityPolicy  `json:"capabilities"`
	User                  UserPolicy        `json:"user"`
	Environment           EnvironmentPolicy `json:"environment"`
	ExecutionRestrictions []string          `json:"execution_restrictions,omitempty"`
}

func GetDefaultSecurityProfile(id SecurityProfileID) SecurityProfile {
	switch id {
	case ProfileDefault:
		return SecurityProfile{
			ID: ProfileDefault,
			Filesystem: FilesystemPolicy{
				ReadOnlyRoot:      true,
				WritableWorkspace: true,
				AllowedMountPaths: []string{},
				TempDirsMaxSizeMB: 64,
			},
			Network: NetworkPolicy{
				Mode: "bridge",
			},
			Capabilities: CapabilityPolicy{
				DropCapabilities: []string{"ALL"},
			},
			User: UserPolicy{
				RunAsNonRoot: true,
				UID:          1000,
				GID:          1000,
			},
			Environment: EnvironmentPolicy{
				BlockedVariables: []string{"SECRET_", "AWS_", "KUBERNETES_", "DOCKER_"},
				AllowedVariables: []string{"PATH", "LANG", "HOME", "USER"},
			},
		}
	case ProfileReadOnly:
		return SecurityProfile{
			ID: ProfileReadOnly,
			Filesystem: FilesystemPolicy{
				ReadOnlyRoot:      true,
				WritableWorkspace: false,
				AllowedMountPaths: []string{},
				TempDirsMaxSizeMB: 0,
			},
			Network: NetworkPolicy{
				Mode: "none",
			},
			Capabilities: CapabilityPolicy{
				DropCapabilities: []string{"ALL"},
			},
			User: UserPolicy{
				RunAsNonRoot: true,
				UID:          1001,
				GID:          1001,
			},
			Environment: EnvironmentPolicy{
				BlockedVariables: []string{"SECRET_", "AWS_", "KUBERNETES_", "DOCKER_"},
				AllowedVariables: []string{"PATH", "LANG"},
			},
		}
	case ProfileRestricted:
		return SecurityProfile{
			ID: ProfileRestricted,
			Filesystem: FilesystemPolicy{
				ReadOnlyRoot:      true,
				WritableWorkspace: true,
				AllowedMountPaths: []string{},
				TempDirsMaxSizeMB: 16,
			},
			Network: NetworkPolicy{
				Mode: "none",
			},
			Capabilities: CapabilityPolicy{
				DropCapabilities: []string{"ALL"},
			},
			User: UserPolicy{
				RunAsNonRoot: true,
				UID:          1002,
				GID:          1002,
			},
			Environment: EnvironmentPolicy{
				BlockedVariables: []string{"SECRET_", "AWS_", "KUBERNETES_", "DOCKER_", "ENV_"},
				AllowedVariables: []string{"PATH"},
			},
		}
	case ProfileHighSecurity:
		return SecurityProfile{
			ID: ProfileHighSecurity,
			Filesystem: FilesystemPolicy{
				ReadOnlyRoot:      true,
				WritableWorkspace: true,
				AllowedMountPaths: []string{},
				TempDirsMaxSizeMB: 4,
			},
			Network: NetworkPolicy{
				Mode: "none",
			},
			Capabilities: CapabilityPolicy{
				DropCapabilities: []string{"ALL", "CAP_SYS_ADMIN", "CAP_NET_ADMIN"},
			},
			User: UserPolicy{
				RunAsNonRoot: true,
				UID:          2000,
				GID:          2000,
			},
			Environment: EnvironmentPolicy{
				BlockedVariables: []string{"*"}, // Block all except explicitly allowed
				AllowedVariables: []string{"PATH"},
			},
		}
	case ProfileInteractive:
		return SecurityProfile{
			ID: ProfileInteractive,
			Filesystem: FilesystemPolicy{
				ReadOnlyRoot:      false, // Needs write access for interactive tools
				WritableWorkspace: true,
				AllowedMountPaths: []string{},
				TempDirsMaxSizeMB: 128,
			},
			Network: NetworkPolicy{
				Mode: "bridge",
			},
			Capabilities: CapabilityPolicy{
				DropCapabilities: []string{"CAP_SYS_ADMIN", "CAP_NET_ADMIN"},
			},
			User: UserPolicy{
				RunAsNonRoot: true,
				UID:          1000,
				GID:          1000,
			},
			Environment: EnvironmentPolicy{
				BlockedVariables: []string{"SECRET_", "AWS_"},
				AllowedVariables: []string{"PATH", "LANG", "HOME", "USER", "TERM"},
			},
		}
	default:
		return GetDefaultSecurityProfile(ProfileDefault)
	}
}
