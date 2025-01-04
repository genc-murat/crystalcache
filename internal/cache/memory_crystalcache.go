package cache

import (
	"fmt"
	"strconv"
	"sync/atomic"

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
// and an error if the key is not found or any other issue occurs.
//
// The memory usage is calculated based on the type of the value associated with the key:
// - "string": The size of the string value.
// - "hash": The combined size of all keys and values in the hash map.
// - "list": The combined size of all elements in the list.
// - "set": The combined size of all members in the set.
// - "zset": The combined size of all members in the sorted set, including the score.
// - "json": The estimated size of the JSON value as a string representation.
// - "stream": The estimated size based on the average entry size.
// - "bitmap": The size of the bitmap in bytes.
// - "none": Returns an error indicating the key is not found.
//
// The function also calculates additional overheads:
// - Pointer overhead for key storage.
// - Overhead for hash entries, list nodes, set entries, and sorted set entries.
// - Allocator overhead (approximately 16 bytes per allocation).
// - Aligned size (rounded up to the nearest 8 bytes).
//
// Parameters:
// - key: The key for which memory usage is to be calculated.
//
// Returns:
// - *models.MemoryUsageInfo: A struct containing detailed memory usage information.
// - error: An error if the key is not found or any other issue occurs.
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
