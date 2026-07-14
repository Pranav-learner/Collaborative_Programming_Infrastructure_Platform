package features

// Feature represents a runtime capability flag.
type Feature string

const (
	SupportsNetworking           Feature = "SupportsNetworking"
	SupportsTTY                  Feature = "SupportsTTY"
	SupportsVolumes              Feature = "SupportsVolumes"
	SupportsSnapshots            Feature = "SupportsSnapshots"
	SupportsGPU                  Feature = "SupportsGPU"
	SupportsRootless             Feature = "SupportsRootless"
	SupportsCheckpointRestore    Feature = "SupportsCheckpointRestore"
	SupportsInteractiveExecution Feature = "SupportsInteractiveExecution"
	SupportsCustomImages         Feature = "SupportsCustomImages"
	SupportsReadOnlyRootFS       Feature = "SupportsReadOnlyRootFS"
)
