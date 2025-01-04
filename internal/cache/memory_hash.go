package cache

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/genc-murat/crystalcache/pkg/utils/pattern"
)

func (c *MemoryCache) HSet(hash string, key string, value string) error {
	var hashMap sync.Map
	actual, _ := c.hsets.LoadOrStore(hash, &hashMap)
	actualMap := actual.(*sync.Map)
	actualMap.Store(key, value)
	c.incrementKeyVersion(hash)
	return nil
}

func (c *MemoryCache) HGet(hash string, key string) (string, bool) {
	if hashMapI, ok := c.hsets.Load(hash); ok {
		hashMap := hashMapI.(*sync.Map)
		if value, ok := hashMap.Load(key); ok {
			return value.(string), true
		}
	}
	return "", false
}

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

// HScan implements Redis HSCAN command with optimized pattern matching
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

// HIncrBy increments the integer value of a hash field by the given increment
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

// HIncrByFloat increments the float value of a hash field by the given increment
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
