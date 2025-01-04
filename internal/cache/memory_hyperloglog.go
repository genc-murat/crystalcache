package cache

import (
	"fmt"
	"hash/fnv"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// PFAdd adds the specified elements to the HyperLogLog data structure associated with the given key.
// It returns a boolean indicating whether the HyperLogLog was modified and an error if any occurred.
//
// Parameters:
//   - key: The key associated with the HyperLogLog data structure.
//   - elements: The elements to be added to the HyperLogLog.
//
// Returns:
//   - bool: True if the HyperLogLog was modified, false otherwise.
//   - error: An error if any occurred during the operation.
func (c *MemoryCache) PFAdd(key string, elements ...string) (bool, error) {
	hllI, _ := c.hlls.LoadOrStore(key, models.NewHyperLogLog())
	hll := hllI.(*models.HyperLogLog)

	modified := false
	for _, element := range elements {
		// Create FNV-64a hash
		h := fnv.New64a()
		h.Write([]byte(element))
		hashValue := h.Sum64()

		if hll.Add(hashValue) {
			modified = true
		}
	}

	if modified {
		c.incrementKeyVersion(key)
	}

	return modified, nil
}

// PFCount returns the approximate cardinality of the set(s) stored at the given key(s).
// If a single key is provided, it returns the cardinality of the set stored at that key.
// If multiple keys are provided, it merges the sets stored at those keys and returns the cardinality of the merged set.
// If no keys are provided, it returns an error indicating that at least one key is required.
//
// Parameters:
//
//	keys - One or more keys identifying the sets to be counted.
//
// Returns:
//
//	int64 - The approximate cardinality of the set(s).
//	error - An error if no keys are provided or if any other error occurs.
func (c *MemoryCache) PFCount(keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, fmt.Errorf("at least one key is required")
	}

	if len(keys) == 1 {
		// Single key case
		if hllI, exists := c.hlls.Load(keys[0]); exists {
			hll := hllI.(*models.HyperLogLog)
			return int64(hll.Count()), nil
		}
		return 0, nil
	}

	// Multiple keys case - merge all HLLs
	merged := models.NewHyperLogLog()
	for _, key := range keys {
		if hllI, exists := c.hlls.Load(key); exists {
			hll := hllI.(*models.HyperLogLog)
			merged.Merge(hll)
		}
	}

	return int64(merged.Count()), nil
}

// PFMerge merges multiple HyperLogLog structures into a destination HyperLogLog.
// It takes a destination key and one or more source keys. If no source keys are provided,
// it returns an error. The function creates or retrieves the destination HyperLogLog,
// then merges all source HyperLogLogs into the destination. Finally, it increments the
// version of the destination key.
//
// Parameters:
//   - destKey: The key for the destination HyperLogLog.
//   - sourceKeys: One or more keys for the source HyperLogLogs.
//
// Returns:
//   - error: An error if no source keys are provided, otherwise nil.
func (c *MemoryCache) PFMerge(destKey string, sourceKeys ...string) error {
	if len(sourceKeys) == 0 {
		return fmt.Errorf("at least one source key is required")
	}

	// Create or get destination HLL
	destHLLI, _ := c.hlls.LoadOrStore(destKey, models.NewHyperLogLog())
	destHLL := destHLLI.(*models.HyperLogLog)

	// Merge all source HLLs
	for _, sourceKey := range sourceKeys {
		if sourceHLLI, exists := c.hlls.Load(sourceKey); exists {
			sourceHLL := sourceHLLI.(*models.HyperLogLog)
			destHLL.Merge(sourceHLL)
		}
	}

	c.incrementKeyVersion(destKey)
	return nil
}

// PFDebug retrieves the debug information of a HyperLogLog associated with the given key.
// It returns a map containing the debug information and an error if the key does not exist.
//
// Parameters:
//   - key: The key associated with the HyperLogLog.
//
// Returns:
//   - map[string]interface{}: A map containing the debug information of the HyperLogLog.
//   - error: An error if the key does not exist.
func (c *MemoryCache) PFDebug(key string) (map[string]interface{}, error) {
	hllI, exists := c.hlls.Load(key)
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	hll := hllI.(*models.HyperLogLog)
	return hll.Debug(), nil
}

// PFSelfTest performs a self-test on the HyperLogLog (HLL) implementation.
// It creates a test HLL, adds a set of test data to it, and verifies that
// the estimated count is within an expected range.
//
// Returns an error if the self-test fails, indicating that the estimated
// count is outside the expected range.
func (c *MemoryCache) PFSelfTest() error {
	// Create a test HLL
	hll := models.NewHyperLogLog()

	// Test basic operations
	testData := []string{"a", "b", "a", "c", "d", "a"}
	for _, elem := range testData {
		// Create FNV-64a hash
		h := fnv.New64a()
		h.Write([]byte(elem))
		hashValue := h.Sum64()

		hll.Add(hashValue)
	}

	// Verify count is within expected range
	count := hll.Count()
	if count < 3 || count > 5 {
		return fmt.Errorf("self test failed: count %d outside expected range [3,5]", count)
	}

	return nil
}

// defragHLL defragments the HyperLogLog (HLL) data structures stored in the memory cache.
// It optimizes the memory usage by consolidating fragmented HLL data structures.
func (c *MemoryCache) defragHLL() {
	c.hlls = c.defragSyncMap(c.hlls)
}
