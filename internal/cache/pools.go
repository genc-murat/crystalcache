package cache

import (
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

var (
	stringMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]string)
		},
	}

	hashMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]map[string]string)
		},
	}

	stringListPool = sync.Pool{
		New: func() interface{} {
			return make([]string, 0)
		},
	}

	boolMapSetPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]map[string]bool)
		},
	}

	zsetMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]map[string]float64)
		},
	}

	zsetMemberPool = sync.Pool{
		New: func() interface{} {
			return make([]models.ZSetMember, 0)
		},
	}

	syncMapPool = sync.Pool{
		New: func() interface{} {
			return &sync.Map{}
		},
	}

	stringSlicePool = &sync.Pool{
		New: func() interface{} {
			return make([]string, 0, 64)
		},
	}
)
