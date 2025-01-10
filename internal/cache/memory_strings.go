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
	c.strings.Store(key, value)
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
			c.strings.Delete(key)
			c.expires.Delete(key)
			return "", false
		}
	}

	if value, ok := c.strings.Load(key); ok {
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
	if _, exists := c.strings.Load(key); !exists {
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
			c.strings.Delete(key)
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
		val, exists := c.strings.Load(key)
		if !exists {
			if c.strings.CompareAndSwap(key, nil, "1") {
				return 1, nil
			}
			continue
		}

		num, err := strconv.Atoi(val.(string))
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer")
		}

		num++
		if c.strings.CompareAndSwap(key, val, strconv.Itoa(num)) {
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

	if _, ok := c.strings.LoadAndDelete(key); ok {
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

// PExpireAt sets an expiration time for a given key in the cache. The expiration time is specified
// as a Unix timestamp in milliseconds. When the expiration time is reached, the key will be
// automatically deleted from the cache.
//
// Parameters:
//   - key: The key for which the expiration time is to be set.
//   - timestampMs: The expiration time as a Unix timestamp in milliseconds.
//
// Returns:
//   - error: An error if the operation fails, otherwise nil.
func (c *MemoryCache) PExpireAt(key string, timestampMs int64) error {
	// Convert millisecond timestamp to time.Time
	expireTime := time.Unix(0, timestampMs*int64(time.Millisecond))

	// Store expiration time
	c.expires.Store(key, expireTime)

	// Start a background goroutine to handle expiration
	go func() {
		timer := time.NewTimer(time.Until(expireTime))
		defer timer.Stop()

		<-timer.C

		// Check if the key still exists with the same expiration time
		if expTime, exists := c.expires.Load(key); exists {
			if expTime.(time.Time).Equal(expireTime) {
				c.Del(key)
				// Increment expired keys counter
				if c.stats != nil {
					atomic.AddInt64(&c.stats.expiredKeys, 1)
				}
			}
		}
	}()

	return nil
}

// SEExpire sets the expiration time for a given key in the memory cache based on a specified condition.
//
// Parameters:
//   - key: The key for which the expiration time is to be set.
//   - seconds: The number of seconds after which the key should expire.
//   - condition: The condition under which the expiration time should be set.
//     Possible values are:
//   - "NX": Set the expiration time only if the key does not already have an expiration time.
//   - "XX": Set the expiration time only if the key already has an expiration time.
//   - "GT": Set the expiration time only if the new expiration time is greater than the current expiration time.
//   - "LT": Set the expiration time only if the new expiration time is less than the current expiration time.
//   - "": Always set the expiration time regardless of any existing expiration time.
//
// Returns:
//   - bool: True if the expiration time was set, false otherwise.
//   - error: An error if the condition is invalid or any other issue occurs.
//
// The function also starts a background goroutine to monitor the expiration time and delete the key when it expires.
func (c *MemoryCache) SEExpire(key string, seconds int, condition string) (bool, error) {
	// Check if key exists
	if !c.Exists(key) {
		return false, nil
	}

	// Calculate new expiration time
	newExpireTime := time.Now().Add(time.Duration(seconds) * time.Second)

	// Get current expiration time if exists
	currentExpireTimeI, hasExpire := c.expires.Load(key)

	switch condition {
	case "NX":
		// Set only if there's no existing expiration
		if hasExpire {
			return false, nil
		}
	case "XX":
		// Set only if there's an existing expiration
		if !hasExpire {
			return false, nil
		}
	case "GT", "LT":
		if !hasExpire {
			return false, nil
		}
		currentExpireTime := currentExpireTimeI.(time.Time)
		if condition == "GT" {
			// Set only if new expiry is greater than current
			if !newExpireTime.After(currentExpireTime) {
				return false, nil
			}
		} else { // "LT"
			// Set only if new expiry is less than current
			if !newExpireTime.Before(currentExpireTime) {
				return false, nil
			}
		}
	case "":
		// No condition, always set
	default:
		return false, fmt.Errorf("ERR invalid condition: %s", condition)
	}

	// Store expiration time
	c.expires.Store(key, newExpireTime)

	// Start background expiration monitoring
	go func() {
		timer := time.NewTimer(time.Until(newExpireTime))
		defer timer.Stop()

		<-timer.C

		// Check if the key still exists with the same expiration time
		if expTime, exists := c.expires.Load(key); exists {
			if expTime.(time.Time).Equal(newExpireTime) {
				c.Del(key)
				// Increment expired keys counter
				if c.stats != nil {
					atomic.AddInt64(&c.stats.expiredKeys, 1)
				}
			}
		}
	}()

	return true, nil
}

func (c *MemoryCache) Unlink(key string) (bool, error) {
	// Check if key exists first
	if !c.Exists(key) {
		return false, nil
	}

	// Delete from the appropriate map based on key type
	go func() {
		switch c.Type(key) {
		case "string":
			c.strings.Delete(key)
		case "hash":
			c.hsets.Delete(key)
		case "list":
			c.lists.Delete(key)
		case "set":
			c.sets_.Delete(key)
		case "zset":
			c.zsets.Delete(key)
		case "json":
			c.jsonData.Delete(key)
		case "stream":
			c.streams.Delete(key)
		case "bitmap":
			c.bitmaps.Delete(key)
		case "geo":
			c.geoData.Delete(key)
		case "suggestion":
			c.suggestions.Delete(key)
		case "cms":
			c.cms.Delete(key)
		case "cuckoo":
			c.cuckooFilters.Delete(key)
		case "hll":
			c.hlls.Delete(key)
		case "tdigest":
			c.tdigests.Delete(key)
		case "bf":
			c.bfilters.Delete(key)
		case "topk":
			c.topks.Delete(key)
		}

		// Also delete expiration time if exists
		c.expires.Delete(key)

		// Increment key version to maintain consistency
		c.incrementKeyVersion(key)
	}()

	// Return immediately since actual deletion happens asynchronously
	return true, nil
}

func (c *MemoryCache) defragStrings() {
	c.strings = c.defragSyncMap(c.strings)
}
