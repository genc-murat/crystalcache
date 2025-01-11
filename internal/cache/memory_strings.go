package cache

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
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

	if value, exists := c.strings.Load(key); exists {
		c.lastAccessed.Store(key, time.Now())
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

func (c *MemoryCache) RenameNX(oldKey, newKey string) (bool, error) {
	// Check if source key exists
	if !c.Exists(oldKey) {
		return false, fmt.Errorf("ERR no such key")
	}

	// Check if destination key exists
	if c.Exists(newKey) {
		return false, nil
	}

	// Get key type and value
	keyType := c.Type(oldKey)
	var value interface{}
	var exists bool

	switch keyType {
	case "string":
		value, exists = c.strings.LoadAndDelete(oldKey)
		if exists {
			c.strings.Store(newKey, value)
		}
	case "hash":
		value, exists = c.hsets.LoadAndDelete(oldKey)
		if exists {
			c.hsets.Store(newKey, value)
		}
	case "list":
		value, exists = c.lists.LoadAndDelete(oldKey)
		if exists {
			c.lists.Store(newKey, value)
		}
	case "set":
		value, exists = c.sets_.LoadAndDelete(oldKey)
		if exists {
			c.sets_.Store(newKey, value)
		}
	case "zset":
		value, exists = c.zsets.LoadAndDelete(oldKey)
		if exists {
			c.zsets.Store(newKey, value)
		}
	case "json":
		value, exists = c.jsonData.LoadAndDelete(oldKey)
		if exists {
			c.jsonData.Store(newKey, value)
		}
	case "stream":
		value, exists = c.streams.LoadAndDelete(oldKey)
		if exists {
			c.streams.Store(newKey, value)
		}
	case "bitmap":
		value, exists = c.bitmaps.LoadAndDelete(oldKey)
		if exists {
			c.bitmaps.Store(newKey, value)
		}
	default:
		return false, fmt.Errorf("ERR unsupported key type")
	}

	// Handle expiration time if exists
	if expTime, hasExp := c.expires.LoadAndDelete(oldKey); hasExp {
		c.expires.Store(newKey, expTime)
	}

	// Update key versions
	c.incrementKeyVersion(oldKey)
	c.incrementKeyVersion(newKey)

	return true, nil
}

func (c *MemoryCache) Copy(source, destination string, replace bool) (bool, error) {
	// Check if source exists
	if !c.Exists(source) {
		return false, nil
	}

	// Check if destination exists when replace is false
	if !replace && c.Exists(destination) {
		return false, nil
	}

	// Get source key type and value
	keyType := c.Type(source)
	var success bool

	switch keyType {
	case "string":
		if value, exists := c.strings.Load(source); exists {
			c.strings.Store(destination, value)
			success = true
		}

	case "hash":
		if value, exists := c.hsets.Load(source); exists {
			// Deep copy the hash map
			originalMap := value.(*sync.Map)
			newMap := &sync.Map{}
			originalMap.Range(func(k, v interface{}) bool {
				newMap.Store(k, v)
				return true
			})
			c.hsets.Store(destination, newMap)
			success = true
		}

	case "list":
		if value, exists := c.lists.Load(source); exists {
			// Deep copy the list
			originalList := value.(*[]string)
			newList := make([]string, len(*originalList))
			copy(newList, *originalList)
			c.lists.Store(destination, &newList)
			success = true
		}

	case "set":
		if value, exists := c.sets_.Load(source); exists {
			// Deep copy the set
			originalSet := value.(*sync.Map)
			newSet := &sync.Map{}
			originalSet.Range(func(k, v interface{}) bool {
				newSet.Store(k, v)
				return true
			})
			c.sets_.Store(destination, newSet)
			success = true
		}

	case "zset":
		if value, exists := c.zsets.Load(source); exists {
			// Deep copy the sorted set
			originalZSet := value.(*sync.Map)
			newZSet := &sync.Map{}
			originalZSet.Range(func(k, v interface{}) bool {
				newZSet.Store(k, v)
				return true
			})
			c.zsets.Store(destination, newZSet)
			success = true
		}

	case "json":
		if value, exists := c.jsonData.Load(source); exists {
			c.jsonData.Store(destination, deepCopyJSON(value))
			success = true
		}

	case "stream":
		if value, exists := c.streams.Load(source); exists {
			// Deep copy the stream
			originalStream := value.(*sync.Map)
			newStream := &sync.Map{}
			originalStream.Range(func(k, v interface{}) bool {
				entry := v.(*models.StreamEntry)
				newEntry := &models.StreamEntry{
					ID:     entry.ID,
					Fields: make(map[string]string),
				}
				for k, v := range entry.Fields {
					newEntry.Fields[k] = v
				}
				newStream.Store(k, newEntry)
				return true
			})
			c.streams.Store(destination, newStream)
			success = true
		}

	case "bitmap":
		if value, exists := c.bitmaps.Load(source); exists {
			// Deep copy the bitmap
			originalBitmap := value.([]byte)
			newBitmap := make([]byte, len(originalBitmap))
			copy(newBitmap, originalBitmap)
			c.bitmaps.Store(destination, newBitmap)
			success = true
		}
	}

	// Copy expiration if exists
	if success {
		if expTime, hasExp := c.expires.Load(source); hasExp {
			c.expires.Store(destination, expTime)
		}

		// Update key version
		c.incrementKeyVersion(destination)
	}

	return success, nil
}

// Helper function to deep copy JSON values
func deepCopyJSON(value interface{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		newMap := make(map[string]interface{})
		for k, val := range v {
			newMap[k] = deepCopyJSON(val)
		}
		return newMap
	case []interface{}:
		newSlice := make([]interface{}, len(v))
		for i, val := range v {
			newSlice[i] = deepCopyJSON(val)
		}
		return newSlice
	default:
		return v
	}
}

func (c *MemoryCache) Persist(key string) (bool, error) {
	// Check if key exists
	if !c.Exists(key) {
		return false, nil
	}

	// Check if key has an expiration time
	if _, exists := c.expires.Load(key); !exists {
		return false, nil
	}

	// Remove expiration time
	c.expires.Delete(key)

	// Update key version to maintain consistency
	c.incrementKeyVersion(key)

	return true, nil
}

func (c *MemoryCache) Touch(keys ...string) (int, error) {
	count := 0
	now := time.Now()

	for _, key := range keys {
		if c.Exists(key) {
			c.lastAccessed.Store(key, now)
			count++
		}
	}

	return count, nil
}

func (c *MemoryCache) defragStrings() {
	c.strings = c.defragSyncMap(c.strings)
}
