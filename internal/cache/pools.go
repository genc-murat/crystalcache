package cache

import (
	"strings"
	"sync"
)

var (
	// Builder pool
	builderPool = sync.Pool{
		New: func() interface{} {
			return new(strings.Builder)
		},
	}

	// String maps pool - for simple string maps
	stringMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]string)
		},
	}

	// Bool maps pool - for sets
	boolMapSetPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]map[string]bool)
		},
	}

	// String slice pool
	stringSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]string, 0, 32)
		},
	}

	// Hash maps pool
	hashMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]map[string]string)
		},
	}
)
