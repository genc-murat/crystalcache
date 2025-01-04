package cache

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// DelType deletes all entries of the specified type from the memory cache.
// It supports the following types: string, hash, list, set, zset, json, stream, and bitmap.
// For each deleted entry, it increments the key version and updates the deletion count.
//
// Parameters:
//   - typeName: The type of entries to delete. Must be one of: string, hash, list, set, zset, json, stream, bitmap.
//
// Returns:
//   - int64: The number of deleted entries.
//   - error: An error if the typeName is unknown.
//
// Example usage:
//
//	deletedCount, err := cache.DelType("string")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Deleted %d entries of type 'string'\n", deletedCount)
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

// KeyCount returns the number of keys of a specified type in the memory cache.
// The typeName parameter specifies the type of keys to count and must be one of:
// "string", "hash", "list", "set", "zset", "json", "stream", or "bitmap".
// If an unknown type is provided, an error is returned.
//
// Parameters:
//   - typeName: A string representing the type of keys to count.
//
// Returns:
//   - int64: The number of keys of the specified type.
//   - error: An error if the typeName is unknown.
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

// MemoryUsage calculates the memory usage of a given key in the memory cache.
// It returns a MemoryUsageInfo struct containing detailed memory usage information
// and an error if the key is not found or if there is an issue calculating the memory usage.
//
// Parameters:
//   - key: The key for which to calculate memory usage.
//
// Returns:
//   - *models.MemoryUsageInfo: A struct containing memory usage details such as
//     PointerSize, OverheadBytes, ValueBytes, AllocatorOverhead, AlignedBytes, and TotalBytes.
//   - error: An error if the key is not found or if there is an issue calculating the memory usage.
//
// The function calculates the base overhead for key storage, determines the type of the key,
// and calculates the memory usage based on the key type. It supports various key types including
// string, hash, list, set, zset, json, stream, and bitmap. The function also approximates
// allocator overhead and aligns the total memory usage to 8-byte boundaries.
func (c *MemoryCache) MemoryUsage(key string) (*models.MemoryUsageInfo, error) {
	info := &models.MemoryUsageInfo{
		PointerSize: int(unsafe.Sizeof(uintptr(0))),
	}

	info.OverheadBytes = int64(len(key))

	keyType := c.Type(key)
	if keyType == "none" {
		return nil, fmt.Errorf("ERR key not found")
	}

	var valueSize int64
	var err error

	switch keyType {
	case "string":
		valueSize, err = c.memoryUsageString(key)
	case "hash":
		valueSize, err = c.memoryUsageHash(key)
	case "list":
		valueSize, err = c.memoryUsageList(key)
	case "set":
		valueSize, err = c.memoryUsageSet(key)
	case "zset":
		valueSize, err = c.memoryUsageZSet(key)
	case "json":
		valueSize, err = c.memoryUsageJSON(key)
	case "stream":
		valueSize, err = c.memoryUsageStream(key)
	case "bitmap":
		valueSize, err = c.memoryUsageBitmap(key)
	default:
		return nil, fmt.Errorf("unexpected type: %s", keyType)
	}

	if err != nil {
		return nil, err
	}

	info.ValueBytes = valueSize
	info.AllocatorOverhead = (info.OverheadBytes + info.ValueBytes + 15) / 16 // Approximation
	info.AlignedBytes = ((info.OverheadBytes + info.ValueBytes + 7) / 8) * 8
	info.TotalBytes = info.AlignedBytes + info.AllocatorOverhead

	return info, nil
}

func (c *MemoryCache) memoryUsageString(key string) (int64, error) {
	if val, exists := c.Get(key); exists {
		return int64(len(val)), nil
	}
	return 0, fmt.Errorf("string key not found")
}

func (c *MemoryCache) memoryUsageHash(key string) (int64, error) {
	var valueSize int64
	if hashMap := c.HGetAll(key); hashMap != nil {
		for k, v := range hashMap {
			valueSize += int64(len(k) + len(v))
		}
		return valueSize, nil
	}
	return 0, fmt.Errorf("hash key not found")
}

func (c *MemoryCache) memoryUsageList(key string) (int64, error) {
	var valueSize int64
	if values, err := c.LRange(key, 0, -1); err == nil {
		for _, v := range values {
			valueSize += int64(len(v))
		}
		return valueSize, nil
	}
	return 0, fmt.Errorf("list key not found")
}

func (c *MemoryCache) memoryUsageSet(key string) (int64, error) {
	var valueSize int64
	if members, err := c.SMembers(key); err == nil {
		for _, m := range members {
			valueSize += int64(len(m))
		}
		return valueSize, nil
	}
	return 0, fmt.Errorf("set key not found")
}

func (c *MemoryCache) memoryUsageZSet(key string) (int64, error) {
	var valueSize int64
	if members := c.ZRange(key, 0, -1); len(members) > 0 {
		for _, m := range members {
			valueSize += int64(len(m)) + 8 // 8 bytes for score (float64)
		}
		return valueSize, nil
	}
	return 0, fmt.Errorf("zset key not found")
}

func (c *MemoryCache) memoryUsageJSON(key string) (int64, error) {
	if val, exists := c.GetJSON(key); exists {
		return int64(len(fmt.Sprintf("%v", val))), nil
	}
	return 0, fmt.Errorf("json key not found")
}

func (c *MemoryCache) memoryUsageStream(key string) (int64, error) {
	if length := c.XLEN(key); length > 0 {
		return length * 128, nil // Assume average entry size of 128 bytes
	}
	return 0, fmt.Errorf("stream key not found")
}

func (c *MemoryCache) memoryUsageBitmap(key string) (int64, error) {
	if val, err := c.BitCount(key, 0, -1); err == nil {
		return (val + 7) / 8, nil
	}
	return 0, fmt.Errorf("bitmap key not found")
}
