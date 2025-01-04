package cache

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

// Set stores a key-value pair in the memory cache. It adds the key to the bloom filter,
// stores the value in the cache, and increments the version of the key.
//
// Parameters:
//
//	key: The key to be stored in the cache.
//	value: The value to be associated with the key.
//
// Returns:
//
//	error: Returns nil if the operation is successful.
func (c *MemoryCache) Set(key string, value string) error {
	c.bloomFilter.Add([]byte(key))
	c.sets.Store(key, value)
	c.incrementKeyVersion(key)
	return nil
}

// Get retrieves the value associated with the given key from the memory cache.
// It first checks if the key is likely to be present using a bloom filter.
// If the key is not present in the bloom filter, it returns an empty string and false.
// If the key is present, it checks if the key has expired. If the key has expired,
// it deletes the key from the cache and returns an empty string and false.
// If the key is not expired and is present in the cache, it returns the value and true.
//
// Parameters:
//   - key: The key to retrieve the value for.
//
// Returns:
//   - string: The value associated with the key, or an empty string if the key is not found or has expired.
//   - bool: True if the key is found and not expired, false otherwise.
func (c *MemoryCache) Get(key string) (string, bool) {
	if !c.bloomFilter.Contains([]byte(key)) {
		return "", false
	}

	if expireTime, ok := c.expires.Load(key); ok {
		if expTime, ok := expireTime.(time.Time); ok && time.Now().After(expTime) {
			c.sets.Delete(key)
			c.expires.Delete(key)
			return "", false
		}
	}

	if value, ok := c.sets.Load(key); ok {
		return value.(string), true
	}
	return "", false
}

// PTTL returns the remaining time to live (TTL) of a key in milliseconds.
// If the key does not exist, it returns -2.
// If the key exists but has no expiration, it returns -1.
// If the key has expired, it returns -2 and schedules the key for deletion.
func (c *MemoryCache) PTTL(key string) int64 {
	// Check if key exists
	if _, exists := c.sets.Load(key); !exists {
		return -2
	}

	// Check expiration
	expireTimeI, hasExpire := c.expires.Load(key)
	if !hasExpire {
		return -1
	}

	expireTime := expireTimeI.(time.Time)
	ttlMs := time.Until(expireTime).Milliseconds()
	if ttlMs < 0 {
		// Key has expired, clean it up
		go func() {
			c.sets.Delete(key)
			c.expires.Delete(key)
			if c.stats != nil {
				atomic.AddInt64(&c.stats.expiredKeys, 1)
			}
		}()
		return -2
	}

	return ttlMs
}

// Incr increments the integer value stored at the given key by 1.
// If the key does not exist, it sets the value to 1.
// If the value is not an integer, it returns an error.
// It returns the new value and any error encountered.
func (c *MemoryCache) Incr(key string) (int, error) {
	for {
		val, exists := c.sets.Load(key)
		if !exists {
			if c.sets.CompareAndSwap(key, nil, "1") {
				return 1, nil
			}
			continue
		}

		num, err := strconv.Atoi(val.(string))
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer")
		}

		num++
		if c.sets.CompareAndSwap(key, val, strconv.Itoa(num)) {
			return num, nil
		}
	}
}

// Del removes the specified key from all internal data structures of the MemoryCache.
// It returns a boolean indicating whether the key was found and deleted, and an error if any occurred.
//
// The method checks and deletes the key from the following internal data structures:
// - sets
// - sets_
// - hsets
// - lists
// - jsonData
// - zsets
// - geoData
// - suggestions
// - cms
// - cuckooFilters
// - hlls
// - tdigests
// - bfilters
// - topks
// - timeSeries
//
// If the key is found and deleted from any of these structures, the key version is incremented.
func (c *MemoryCache) Del(key string) (bool, error) {
	deleted := false

	if _, ok := c.sets.LoadAndDelete(key); ok {
		c.expires.Delete(key)
		deleted = true
	}

	if _, ok := c.sets_.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.hsets.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.lists.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.jsonData.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.zsets.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.geoData.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.suggestions.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.cms.LoadAndDelete(key); ok {
		deleted = true
	}

	if deleted {
		c.incrementKeyVersion(key)
	}

	if _, ok := c.cuckooFilters.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.hlls.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.tdigests.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.bfilters.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.topks.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.timeSeries.LoadAndDelete(key); ok {
		deleted = true
	}

	return deleted, nil
}

// MGetType retrieves the type of each key in the provided list of keys.
// It returns a map where the keys are the provided keys and the values are their corresponding types.
//
// Parameters:
//
//	keys - A slice of strings representing the keys to retrieve types for.
//
// Returns:
//
//	A map where each key is a string from the provided keys slice and the value is the type of that key as a string.
func (c *MemoryCache) MGetType(keys []string) map[string]string {
	results := make(map[string]string, len(keys))

	// Process each key
	for _, key := range keys {
		results[key] = c.Type(key)
	}

	return results
}
