package cache

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/genc-murat/crystalcache/pkg/utils/pattern"
)

// HSet sets the value for a given key in a hash map stored in memory cache.
// If the hash map does not exist, it creates a new one.
// It also increments the version of the key.
//
// Parameters:
//
//	hash - the name of the hash map
//	key - the key within the hash map
//	value - the value to be set for the key
//
// Returns:
//
//	error - an error if the operation fails, otherwise nil
func (c *MemoryCache) HSet(hash string, key string, value string) error {
	var hashMap sync.Map
	actual, _ := c.hsets.LoadOrStore(hash, &hashMap)
	actualMap := actual.(*sync.Map)
	actualMap.Store(key, value)
	c.incrementKeyVersion(hash)
	return nil
}

// HGet retrieves the value associated with the given key from the hash map
// identified by the specified hash. It returns the value and a boolean
// indicating whether the key was found in the hash map.
//
// Parameters:
//
//	hash - The identifier for the hash map.
//	key  - The key whose associated value is to be returned.
//
// Returns:
//
//	string - The value associated with the specified key.
//	bool   - True if the key was found in the hash map, otherwise false.
func (c *MemoryCache) HGet(hash string, key string) (string, bool) {
	if hashMapI, ok := c.hsets.Load(hash); ok {
		hashMap := hashMapI.(*sync.Map)
		if value, ok := hashMap.Load(key); ok {
			return value.(string), true
		}
	}
	return "", false
}

// HGetAll retrieves all key-value pairs from the hash map stored in the memory cache
// under the specified hash key. It returns a map where the keys and values are strings.
//
// Parameters:
//   - hash: The key of the hash map to retrieve.
//
// Returns:
//   - A map containing all key-value pairs from the specified hash map.
func (c *MemoryCache) HGetAll(hash string) map[string]string {
	result := make(map[string]string)
	if hashMapI, ok := c.hsets.Load(hash); ok {
		hashMap := hashMapI.(*sync.Map)
		hashMap.Range(func(key, value interface{}) bool {
			result[key.(string)] = value.(string)
			return true
		})
	}
	return result
}

// HScan iterates over the fields of a hash stored in memory, returning a slice of
// field-value pairs that match the given pattern. The iteration starts from the
// specified cursor position and returns up to the specified count of field-value pairs.
// It returns the resulting slice and the next cursor position to continue the iteration.
//
// Parameters:
//   - hash: The key of the hash to scan.
//   - cursor: The position to start scanning from.
//   - matchPattern: The pattern to match fields against.
//   - count: The maximum number of field-value pairs to return.
//
// Returns:
//   - []string: A slice of field-value pairs that match the pattern.
//   - int: The next cursor position to continue the iteration.
func (c *MemoryCache) HScan(hash string, cursor int, matchPattern string, count int) ([]string, int) {
	hashMapI, exists := c.hsets.Load(hash)
	if !exists {
		return []string{}, 0
	}
	hashMap := hashMapI.(*sync.Map)

	// Get fields slice from pool
	fields := stringSlicePool.Get().([]string)
	fields = fields[:0] // Reset slice keeping capacity

	// Collect matching fields
	hashMap.Range(func(key, _ interface{}) bool {
		field := key.(string)
		if pattern.Match(matchPattern, field) {
			fields = append(fields, field)
		}
		return true
	})
	sort.Strings(fields)

	// Check cursor bounds
	if cursor >= len(fields) {
		stringSlicePool.Put(fields)
		return []string{}, 0
	}

	// Get result slice from pool
	result := stringSlicePool.Get().([]string)
	result = result[:0]

	// Collect results with field-value pairs
	nextCursor := cursor
	for i := cursor; i < len(fields) && len(result) < count*2; i++ {
		field := fields[i]
		if value, ok := hashMap.Load(field); ok {
			result = append(result, field, value.(string))
		}
		nextCursor = i + 1
	}

	// Reset cursor if we've reached the end
	if nextCursor >= len(fields) {
		nextCursor = 0
	}

	// Create final result
	finalResult := make([]string, len(result))
	copy(finalResult, result)

	// Return slices to pool
	stringSlicePool.Put(fields)
	stringSlicePool.Put(result)

	return finalResult, nextCursor
}

// HDel deletes a field from a hash in the memory cache.
// It returns true if the field was successfully deleted, and false if the field did not exist.
// If the hash becomes empty after the deletion, it is removed from the cache.
// The method also increments the version of the key.
//
// Parameters:
//   - hash: The key of the hash from which the field should be deleted.
//   - field: The field to delete from the hash.
//
// Returns:
//   - bool: True if the field was deleted, false if the field did not exist.
//   - error: An error if something went wrong during the deletion.
func (c *MemoryCache) HDel(hash string, field string) (bool, error) {
	hashMapI, exists := c.hsets.Load(hash)
	if !exists {
		return false, nil
	}

	hashMap := hashMapI.(*sync.Map)
	if _, exists := hashMap.LoadAndDelete(field); !exists {
		return false, nil
	}

	// Check if hash is empty after deletion
	empty := true
	hashMap.Range(func(_, _ interface{}) bool {
		empty = false
		return false // Stop iteration at first key
	})

	// If hash is empty, remove it completely
	if empty {
		c.hsets.Delete(hash)
		// Return the empty sync.Map to a pool if you maintain one
		syncMapPool.Put(hashMap)
	}

	c.incrementKeyVersion(hash)
	return true, nil
}

// HIncrBy increments the integer value of a hash field by the given increment.
// If the field does not exist, it is set to 0 before performing the operation.
// If the field contains a value that is not an integer, an error is returned.
//
// Parameters:
//   - key: The key of the hash.
//   - field: The field within the hash to increment.
//   - increment: The value to increment the field by.
//
// Returns:
//   - int64: The new value of the field after the increment.
//   - error: An error if the field value is not an integer.
func (c *MemoryCache) HIncrBy(key, field string, increment int64) (int64, error) {
	var hashMap sync.Map
	actual, _ := c.hsets.LoadOrStore(key, &hashMap)
	actualMap := actual.(*sync.Map)

	for {
		// Get current value
		currentI, _ := actualMap.LoadOrStore(field, "0")
		current := currentI.(string)

		// Convert current value to int64
		currentVal, err := strconv.ParseInt(current, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not an integer")
		}

		// Calculate new value
		newVal := currentVal + increment

		// Try to store new value
		if actualMap.CompareAndSwap(field, current, strconv.FormatInt(newVal, 10)) {
			c.incrementKeyVersion(key)
			return newVal, nil
		}
	}
}

// HIncrByFloat increments the float64 value of a hash field by the given increment.
// If the field does not exist, it is set to 0 before performing the operation.
// The function returns the new value of the field after the increment.
//
// Parameters:
//   - key: The key of the hash.
//   - field: The field within the hash to increment.
//   - increment: The value to increment the field by.
//
// Returns:
//   - float64: The new value of the field after the increment.
//   - error: An error if the current value of the field is not a valid float64.
//
// Errors:
//   - Returns an error if the current value of the field is not a valid float64.
func (c *MemoryCache) HIncrByFloat(key, field string, increment float64) (float64, error) {
	var hashMap sync.Map
	actual, _ := c.hsets.LoadOrStore(key, &hashMap)
	actualMap := actual.(*sync.Map)

	for {
		// Get current value
		currentI, _ := actualMap.LoadOrStore(field, "0")
		current := currentI.(string)

		// Convert current value to float64
		currentVal, err := strconv.ParseFloat(current, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not a float")
		}

		// Calculate new value
		newVal := currentVal + increment

		// Format new value with maximum precision
		newValStr := strconv.FormatFloat(newVal, 'f', -1, 64)

		// Try to store new value
		if actualMap.CompareAndSwap(field, current, newValStr) {
			c.incrementKeyVersion(key)
			return newVal, nil
		}
	}
}

// HDelIf deletes a field from a hash stored at key only if the field's current value matches the expected value.
// It returns true if the field was deleted, and false otherwise. If the hash becomes empty after the deletion,
// the hash itself is removed from the cache.
//
// Parameters:
//   - key: The key of the hash.
//   - field: The field within the hash to delete.
//   - expectedValue: The value that the field must have for the deletion to occur.
//
// Returns:
//   - bool: True if the field was deleted, false otherwise.
//   - error: An error if any occurred during the operation.
func (c *MemoryCache) HDelIf(key string, field string, expectedValue string) (bool, error) {
	// Get the hash map
	hashI, exists := c.hsets.Load(key)
	if !exists {
		return false, nil
	}

	hash := hashI.(*sync.Map)

	// Get current value and check condition
	actualValueI, exists := hash.Load(field)
	if !exists {
		return false, nil
	}

	actualValue := actualValueI.(string)
	if actualValue != expectedValue {
		return false, nil
	}

	// Delete the field only if condition is met
	hash.Delete(field)

	// Check if hash is now empty
	empty := true
	hash.Range(func(_, _ interface{}) bool {
		empty = false
		return false
	})

	// If hash is empty, remove it entirely
	if empty {
		c.hsets.Delete(key)
	}

	c.incrementKeyVersion(key)
	return true, nil
}

// HIncrByFloatIf increments the float value of a hash field by the given increment
// if the current value matches the expected value.
//
// Parameters:
//   - key: The key of the hash.
//   - field: The field within the hash to increment.
//   - increment: The amount to increment the field's value by.
//   - expectedValue: The expected current value of the field.
//
// Returns:
//   - float64: The new value of the field after incrementing.
//   - bool: A boolean indicating whether the increment was performed.
//   - error: An error if the current value is not a valid float or any other issue occurs.
//
// If the field does not exist or the current value does not match the expected value,
// the function returns 0, false, nil.
func (c *MemoryCache) HIncrByFloatIf(key string, field string, increment float64, expectedValue string) (float64, bool, error) {
	// Get or create hash
	hashI, _ := c.hsets.LoadOrStore(key, &sync.Map{})
	hash := hashI.(*sync.Map)

	// Get current value and check condition
	currentValueI, exists := hash.Load(field)
	if !exists {
		return 0, false, nil
	}

	currentStr := currentValueI.(string)
	if currentStr != expectedValue {
		return 0, false, nil
	}

	// Parse current value as float
	currentValue, err := strconv.ParseFloat(currentStr, 64)
	if err != nil {
		return 0, false, fmt.Errorf("ERR hash value is not a valid float")
	}

	// Calculate new value
	newValue := currentValue + increment

	// Convert new value to string with high precision
	newStr := strconv.FormatFloat(newValue, 'f', -1, 64)

	// Store new value
	hash.Store(field, newStr)
	c.incrementKeyVersion(key)

	return newValue, true, nil
}

// HScanMatch scans the hash map for keys matching the given pattern, starting from the specified cursor position.
// It returns a slice of matching key-value pairs and the next cursor position.
//
// Parameters:
//   - hash: The key of the hash map to scan.
//   - cursor: The position to start scanning from. If the cursor is out of range, it will start from the beginning.
//   - matchPattern: The pattern to match keys against.
//   - count: The maximum number of key-value pairs to return. If count is less than or equal to 0, a default value of 10 is used.
//
// Returns:
//   - []string: A slice containing the matching key-value pairs.
//   - int: The next cursor position for subsequent scans.
func (c *MemoryCache) HScanMatch(hash string, cursor int, matchPattern string, count int) ([]string, int) {
	hashI, exists := c.hsets.Load(hash)
	if !exists {
		return []string{}, 0
	}

	hashMap := hashI.(*sync.Map)
	var result []string
	var keys []string
	matcher := c.patternMatcher
	nextCursor := 0

	// Collect and filter keys based on the pattern
	hashMap.Range(func(key, value interface{}) bool {
		keyStr := key.(string)
		if matcher.MatchCached(matchPattern, keyStr) {
			keys = append(keys, keyStr)
		}
		return true
	})

	sort.Strings(keys)

	// Handle cursor and count
	if cursor < 0 || cursor >= len(keys) {
		cursor = 0
	}
	if count <= 0 {
		count = 10 // Default count
	}

	// Calculate the end index for the current scan
	end := cursor + count
	if end > len(keys) {
		end = len(keys)
	}

	// Append matching key-value pairs to the result
	for i := cursor; i < end; i++ {
		key := keys[i]
		if value, ok := hashMap.Load(key); ok {
			result = append(result, key, value.(string))
		}
	}

	// Update the next cursor
	if end < len(keys) {
		nextCursor = end
	}

	return result, nextCursor
}
