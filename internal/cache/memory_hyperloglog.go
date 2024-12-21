package cache

import (
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// PFAdd adds elements to a HyperLogLog
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

// PFCount returns the estimated cardinality
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

// PFMerge merges multiple HyperLogLogs into a destination key
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

// PFDebug returns debug information about a HyperLogLog
func (c *MemoryCache) PFDebug(key string) (map[string]interface{}, error) {
	hllI, exists := c.hlls.Load(key)
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	hll := hllI.(*models.HyperLogLog)
	return hll.Debug(), nil
}

// PFSelfTest runs internal consistency checks
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

func (c *MemoryCache) defragHLL() {
	newHLLs := &sync.Map{}

	c.hlls.Range(func(key, valueI interface{}) bool {
		hll := valueI.(*models.HyperLogLog)
		newHLLs.Store(key, hll)
		return true
	})

	c.hlls = newHLLs
}
