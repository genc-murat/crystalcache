package cache

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type LRU2Adapter struct {
	cache *models.LRU2Cache
}

func NewLRU2Adapter(capacity int) *LRU2Adapter {
	return &LRU2Adapter{
		cache: models.NewLRU2Cache(capacity, 5), // 5 saniye coroutine
	}
}

func (a *LRU2Adapter) Set(key string, value interface{}) error {
	size := 1 // Basit boyut hesabı, geliştirilebilir
	if str, ok := value.(string); ok {
		size = len(str)
	}

	if !a.cache.Set(key, value, size) {
		return fmt.Errorf("failed to set key: %s", key)
	}
	return nil
}

func (a *LRU2Adapter) Get(key string) (interface{}, bool) {
	return a.cache.Get(key)
}

func (a *LRU2Adapter) Remove(key string) bool {
	return a.cache.Remove(key)
}

func (a *LRU2Adapter) Clear() {
	a.cache.Clear()
}

func (a *LRU2Adapter) Stats() models.LRU2Stats {
	return a.cache.Stats()
}
