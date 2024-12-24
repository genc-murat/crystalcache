package cache

import (
	"encoding/binary"
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// BFAdd adds an item to a Bloom Filter
func (c *MemoryCache) BFAdd(key string, item string) (bool, error) {
	// Create a new filter if it doesn't exist
	filterI, _ := c.bfilters.LoadOrStore(key, models.NewBloomFilter(models.BloomFilterConfig{
		ExpectedItems:     1000000, // Default capacity
		FalsePositiveRate: 0.01,    // Default error rate
	}))
	filter := filterI.(*models.BloomFilter)

	// Check if item already exists
	exists := filter.Contains([]byte(item))
	if !exists {
		filter.Add([]byte(item))
		c.incrementKeyVersion(key)
	}

	return !exists, nil
}

// BFExists checks whether an item exists in a Bloom Filter
func (c *MemoryCache) BFExists(key string, item string) (bool, error) {
	filterI, exists := c.bfilters.Load(key)
	if !exists {
		return false, nil
	}

	filter := filterI.(*models.BloomFilter)
	return filter.Contains([]byte(item)), nil
}

// BFReserve creates a new Bloom Filter with custom parameters
func (c *MemoryCache) BFReserve(key string, errorRate float64, capacity uint) error {
	if errorRate <= 0 || errorRate >= 1 {
		return fmt.Errorf("ERR error rate should be between 0 and 1")
	}
	if capacity == 0 {
		return fmt.Errorf("ERR capacity must be positive")
	}

	// Create new filter with specified parameters
	filter := models.NewBloomFilter(models.BloomFilterConfig{
		ExpectedItems:     capacity,
		FalsePositiveRate: errorRate,
	})

	// Store the filter
	c.bfilters.Store(key, filter)
	c.incrementKeyVersion(key)

	return nil
}

// BFMAdd adds multiple items to a Bloom Filter
func (c *MemoryCache) BFMAdd(key string, items []string) ([]bool, error) {
	// Create or get filter
	filterI, _ := c.bfilters.LoadOrStore(key, models.NewBloomFilter(models.BloomFilterConfig{
		ExpectedItems:     1000000,
		FalsePositiveRate: 0.01,
	}))
	filter := filterI.(*models.BloomFilter)

	results := make([]bool, len(items))
	modified := false

	// Add each item and track if it was newly added
	for i, item := range items {
		exists := filter.Contains([]byte(item))
		results[i] = !exists
		if !exists {
			filter.Add([]byte(item))
			modified = true
		}
	}

	if modified {
		c.incrementKeyVersion(key)
	}

	return results, nil
}

// BFMExists checks for multiple items in a Bloom Filter
func (c *MemoryCache) BFMExists(key string, items []string) ([]bool, error) {
	filterI, exists := c.bfilters.Load(key)
	if !exists {
		results := make([]bool, len(items))
		return results, nil
	}

	filter := filterI.(*models.BloomFilter)
	results := make([]bool, len(items))

	for i, item := range items {
		results[i] = filter.Contains([]byte(item))
	}

	return results, nil
}

// BFInfo returns information about a Bloom Filter
func (c *MemoryCache) BFInfo(key string) (map[string]interface{}, error) {
	filterI, exists := c.bfilters.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR no such key")
	}

	filter := filterI.(*models.BloomFilter)
	stats := filter.Stats()

	return map[string]interface{}{
		"Size":              stats.Size,
		"HashCount":         stats.HashCount,
		"Count":             stats.Count,
		"BitsetSize":        stats.BitsetSize,
		"SetBits":           stats.SetBits,
		"FalsePositiveRate": stats.FalsePositiveRate,
		"MemoryUsage":       stats.MemoryUsage,
	}, nil
}

// BFCard returns the cardinality of a Bloom Filter
func (c *MemoryCache) BFCard(key string) (uint, error) {
	filterI, exists := c.bfilters.Load(key)
	if !exists {
		return 0, nil
	}

	filter := filterI.(*models.BloomFilter)
	return filter.ApproximateCount(), nil
}

// BFScanDump begins an incremental save of the bloom filter
func (c *MemoryCache) BFScanDump(key string, iterator int) (int, []byte, error) {
	filterI, exists := c.bfilters.Load(key)
	if !exists {
		return 0, nil, fmt.Errorf("ERR no such key")
	}

	filter := filterI.(*models.BloomFilter)
	stats := filter.Stats()

	// Encode filter configuration and state
	// Format: [Size(8)][HashCount(8)][Count(8)][BitsetSize(8)]
	data := make([]byte, 32)
	binary.BigEndian.PutUint64(data[0:8], uint64(stats.Size))
	binary.BigEndian.PutUint64(data[8:16], uint64(stats.HashCount))
	binary.BigEndian.PutUint64(data[16:24], uint64(stats.Count))
	binary.BigEndian.PutUint64(data[24:32], uint64(stats.BitsetSize))

	// Since we can't access the bitset directly, we can only save the configuration
	// The filter will need to be rebuilt by re-adding items
	if iterator == 0 {
		return 1, data, nil
	}

	return 0, nil, nil
}

// BFLoadChunk restores a filter previously saved using SCANDUMP
func (c *MemoryCache) BFLoadChunk(key string, iterator int, data []byte) error {
	if iterator != 0 {
		return fmt.Errorf("ERR invalid iterator value")
	}

	if len(data) < 32 {
		return fmt.Errorf("ERR invalid data format")
	}

	// Decode filter configuration
	size := uint(binary.BigEndian.Uint64(data[0:8]))
	// Create a new filter with decoded configuration
	// Note: We'll need to approximate the configuration parameters to achieve
	// similar size and hash count
	expectedItems := size / 10 // Rough approximation
	falsePositiveRate := 0.01  // Default rate

	filter := models.NewBloomFilter(models.BloomFilterConfig{
		ExpectedItems:     expectedItems,
		FalsePositiveRate: falsePositiveRate,
	})

	c.bfilters.Store(key, filter)
	c.incrementKeyVersion(key)

	return nil
}

func (c *MemoryCache) defragBloomFilters() {
	c.bfilters = c.defragSyncMap(c.bfilters)
}
