package cache

import (
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/genc-murat/crystalcache/internal/core/models"
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

func (c *MemoryCache) KeyCount(typeName string) (int64, error) {
	var count int64

	switch typeName {
	case "string":
		c.sets.Range(func(key, _ interface{}) bool {
			count++
			return true
		})

	case "hash":
		c.hsets.Range(func(key, _ interface{}) bool {
			count++
			return true
		})

	case "list":
		c.lists.Range(func(key, _ interface{}) bool {
			count++
			return true
		})

	case "set":
		c.sets_.Range(func(key, _ interface{}) bool {
			count++
			return true
		})

	case "zset":
		c.zsets.Range(func(key, _ interface{}) bool {
			count++
			return true
		})

	case "json":
		c.jsonData.Range(func(key, _ interface{}) bool {
			count++
			return true
		})

	case "stream":
		c.streams.Range(func(key, _ interface{}) bool {
			count++
			return true
		})

	case "bitmap":
		c.bitmaps.Range(func(key, _ interface{}) bool {
			count++
			return true
		})

	default:
		return 0, fmt.Errorf("ERR unknown type '%s'. Must be one of: string, hash, list, set, zset, json, stream, bitmap", typeName)
	}

	return count, nil
}

func (c *MemoryCache) MemoryUsage(key string) (*models.MemoryUsageInfo, error) {
	info := &models.MemoryUsageInfo{
		PointerSize: strconv.IntSize / 8,
	}

	// Base overhead for key storage
	info.OverheadBytes = int64(len(key) + info.PointerSize) // Key string + pointer overhead

	var valueSize int64

	switch c.Type(key) {
	case "string":
		if val, exists := c.Get(key); exists {
			valueSize = int64(len(val))
		} else {
			return nil, fmt.Errorf("ERR key not found")
		}

	case "hash":
		if hashMap := c.HGetAll(key); hashMap != nil {
			for k, v := range hashMap {
				valueSize += int64(len(k) + len(v))
			}
			info.OverheadBytes += int64(len(hashMap) * info.PointerSize) // Overhead for hash entries
		}

	case "list":
		if values, err := c.LRange(key, 0, -1); err == nil {
			for _, v := range values {
				valueSize += int64(len(v))
			}
			info.OverheadBytes += int64(len(values) * info.PointerSize) // Overhead for list nodes
		}

	case "set":
		if members, err := c.SMembers(key); err == nil {
			for _, m := range members {
				valueSize += int64(len(m))
			}
			info.OverheadBytes += int64(len(members) * info.PointerSize) // Overhead for set entries
		}

	case "zset":
		if members := c.ZRange(key, 0, -1); len(members) > 0 {
			for _, m := range members {
				valueSize += int64(len(m)) + 8 // 8 bytes for score (float64)
			}
			info.OverheadBytes += int64(len(members) * (info.PointerSize + 8)) // Overhead for sorted set entries
		}

	case "json":
		if val, exists := c.GetJSON(key); exists {
			// Estimate JSON size using string representation
			valueSize = int64(len(fmt.Sprintf("%v", val)))
		}

	case "stream":
		if length := c.XLEN(key); length > 0 {
			// Estimate based on average entry size
			valueSize = length * 128                                 // Assume average entry size of 128 bytes
			info.OverheadBytes += length * int64(info.PointerSize*2) // Stream entry overhead
		}

	case "bitmap":
		if val, err := c.BitCount(key, 0, -1); err == nil {
			valueSize = (val + 7) / 8 // Convert bits to bytes, rounding up
		}

	case "none":
		return nil, fmt.Errorf("ERR key not found")
	}

	// Calculate allocator overhead (approximately 16 bytes per allocation)
	info.AllocatorOverhead = (info.OverheadBytes + valueSize) / 16

	// Calculate aligned size (rounded up to nearest 8 bytes)
	info.AlignedBytes = ((info.OverheadBytes + valueSize + 7) / 8) * 8

	info.ValueBytes = valueSize
	info.TotalBytes = info.AlignedBytes + info.AllocatorOverhead

	return info, nil
}
