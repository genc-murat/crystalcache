package cache

import (
	"fmt"
	"sync/atomic"
)

func (c *MemoryCache) DelType(typeName string) (int64, error) {
	var deletedCount int64

	switch typeName {
	case "string":
		c.sets.Range(func(key, _ interface{}) bool {
			c.sets.Delete(key)
			c.expires.Delete(key)
			atomic.AddInt64(&deletedCount, 1)
			c.incrementKeyVersion(key.(string))
			return true
		})

	case "hash":
		c.hsets.Range(func(key, _ interface{}) bool {
			c.hsets.Delete(key)
			atomic.AddInt64(&deletedCount, 1)
			c.incrementKeyVersion(key.(string))
			return true
		})

	case "list":
		c.lists.Range(func(key, _ interface{}) bool {
			c.lists.Delete(key)
			atomic.AddInt64(&deletedCount, 1)
			c.incrementKeyVersion(key.(string))
			return true
		})

	case "set":
		c.sets_.Range(func(key, _ interface{}) bool {
			c.sets_.Delete(key)
			atomic.AddInt64(&deletedCount, 1)
			c.incrementKeyVersion(key.(string))
			return true
		})

	case "zset":
		c.zsets.Range(func(key, _ interface{}) bool {
			c.zsets.Delete(key)
			atomic.AddInt64(&deletedCount, 1)
			c.incrementKeyVersion(key.(string))
			return true
		})

	case "json":
		c.jsonData.Range(func(key, _ interface{}) bool {
			c.jsonData.Delete(key)
			atomic.AddInt64(&deletedCount, 1)
			c.incrementKeyVersion(key.(string))
			return true
		})

	case "stream":
		c.streams.Range(func(key, _ interface{}) bool {
			c.streams.Delete(key)
			atomic.AddInt64(&deletedCount, 1)
			c.incrementKeyVersion(key.(string))
			return true
		})

	case "bitmap":
		c.bitmaps.Range(func(key, _ interface{}) bool {
			c.bitmaps.Delete(key)
			atomic.AddInt64(&deletedCount, 1)
			c.incrementKeyVersion(key.(string))
			return true
		})

	default:
		return 0, fmt.Errorf("ERR unknown type '%s'. Must be one of: string, hash, list, set, zset, json, stream, bitmap", typeName)
	}

	// Update stats
	if c.stats != nil {
		atomic.AddInt64(&c.stats.cmdCount, 1)
	}

	return deletedCount, nil
}
