package adapters

import (
	"cpip/internal/sandbox/runtime"
)

// Registry of adapter creation helpers or common configurations can reside here.
type AdapterCreator func() (runtime.RuntimeAdapter, error)
