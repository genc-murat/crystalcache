package cache

import (
	"strings"
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

var (
	// Existing pools
	builderPool = sync.Pool{
		New: func() interface{} {
			return new(strings.Builder)
		},
	}

	stringMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]string)
		},
	}

	// String slice pool
	stringSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]string, 0, 32)
		},
	}

	// Bool maps pool - for sets
	boolMapSetPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]map[string]bool)
		},
	}

	// Lists pool
	listMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string][]string)
		},
	}

	stringListPool = sync.Pool{
		New: func() interface{} {
			return make([]string, 0, 32)
		},
	}

	// Hash pools
	hashMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]map[string]string)
		},
	}

	// Set pools
	setMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]map[string]bool)
		},
	}

	// Sorted Set pools
	zsetMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]map[string]float64)
		},
	}

	zsetMemberPool = sync.Pool{
		New: func() interface{} {
			return make([]models.ZSetMember, 0, 32)
		},
	}

	// HyperLogLog pool
	hllMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]*models.HyperLogLog)
		},
	}
)
