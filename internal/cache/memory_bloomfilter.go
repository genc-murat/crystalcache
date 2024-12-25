package cache

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// BFAdd adds an item to a Bloom Filter
func (c *MemoryCache) BFAdd(key string, item string) (bool, error) {
	filterI, _ := c.bfilters.LoadOrStore(key, models.NewBloomFilter(models.BloomFilterConfig{
		ExpectedItems:     1000000, // Default capacity
		FalsePositiveRate: 0.01,    // Default error rate
	}))
	filter := filterI.(*models.BloomFilter)

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

	filter := models.NewBloomFilter(models.BloomFilterConfig{
		ExpectedItems:     capacity,
		FalsePositiveRate: errorRate,
	})

	c.bfilters.Store(key, filter)
	c.incrementKeyVersion(key)

	return nil
}

// BFMAdd adds multiple items to a Bloom Filter
func (c *MemoryCache) BFMAdd(key string, items []string) ([]bool, error) {
	filterI, _ := c.bfilters.LoadOrStore(key, models.NewBloomFilter(models.BloomFilterConfig{
		ExpectedItems:     1000000,
		FalsePositiveRate: 0.01,
	}))
	filter := filterI.(*models.BloomFilter)

	results := make([]bool, len(items))
	modified := false

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
		"Count":             atomic.LoadUint64(&stats.Count),
		"BitsetSize":        stats.BitsetSize,
		"SetBits":           atomic.LoadUint64(&stats.SetBits),
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

// BFScanDump begins an incremental save of the bloom filter's state.
// For Bloom Filters, a full dump is typically more practical than incremental.
// This implementation returns the entire filter's data on the first call (iterator 0).
func (c *MemoryCache) BFScanDump(key string, iterator int) (int, []byte, error) {
	filterI, exists := c.bfilters.Load(key)
	if !exists {
		return 0, nil, fmt.Errorf("ERR no such key")
	}

	if iterator != 0 {
		return 0, nil, nil // No more chunks to dump
	}

	filter := filterI.(*models.BloomFilter)

	// Serialize the Bloom Filter's configuration and bitset
	config := filter.GetConfig()
	bitSetData, err := filter.SerializeBitSet()
	if err != nil {
		return 0, nil, fmt.Errorf("ERR failed to serialize bitset: %w", err)
	}

	// Format: [ExpectedItems(8)][FalsePositiveRate(8)][BitSetDataLength(8)][BitSetData]
	data := make([]byte, 8+8+8+len(bitSetData))
	binary.BigEndian.PutUint64(data[0:8], uint64(config.ExpectedItems))
	binary.LittleEndian.PutUint64(data[8:16], math.Float64bits(config.FalsePositiveRate))
	binary.BigEndian.PutUint64(data[16:24], uint64(len(bitSetData)))
	copy(data[24:], bitSetData)

	return 1, data, nil
}

// BFLoadChunk restores a filter previously saved using SCANDUMP.
// This implementation expects a single chunk containing the entire filter data.
func (c *MemoryCache) BFLoadChunk(key string, iterator int, data []byte) error {
	if iterator != 1 {
		return fmt.Errorf("ERR invalid iterator value for BFLoadChunk, expected 1, got %d", iterator)
	}

	if len(data) < 24 {
		return fmt.Errorf("ERR invalid data format for Bloom Filter chunk")
	}

	expectedItems := binary.BigEndian.Uint64(data[0:8])
	falsePositiveRate := math.Float64frombits(binary.LittleEndian.Uint64(data[8:16]))
	bitSetDataLength := binary.BigEndian.Uint64(data[16:24])

	if len(data) < 24+int(bitSetDataLength) {
		return fmt.Errorf("ERR incomplete data for Bloom Filter bitset")
	}

	bitSetData := data[24 : 24+bitSetDataLength]

	config := models.BloomFilterConfig{
		ExpectedItems:     uint(expectedItems),
		FalsePositiveRate: falsePositiveRate,
	}
	filter := models.NewBloomFilter(config)

	if err := filter.DeserializeBitSet(bitSetData); err != nil {
		return fmt.Errorf("ERR failed to deserialize bitset: %w", err)
	}

	c.bfilters.Store(key, filter)
	c.incrementKeyVersion(key)

	return nil
}

// BFInsert creates a new Bloom Filter and adds items in one operation
func (c *MemoryCache) BFInsert(key string, errorRate float64, capacity uint, items []string) ([]bool, error) {
	if errorRate <= 0 || errorRate >= 1 {
		return nil, fmt.Errorf("ERR error rate should be between 0 and 1")
	}
	if capacity == 0 {
		return nil, fmt.Errorf("ERR capacity must be positive")
	}

	filter := models.NewBloomFilter(models.BloomFilterConfig{
		ExpectedItems:     capacity,
		FalsePositiveRate: errorRate,
	})

	results := make([]bool, len(items))
	for i, item := range items {
		exists := filter.Contains([]byte(item))
		results[i] = !exists
		filter.Add([]byte(item))
	}

	c.bfilters.Store(key, filter)
	c.incrementKeyVersion(key)

	return results, nil
}

func (c *MemoryCache) defragBloomFilters() {
	c.bfilters = c.defragSyncMap(c.bfilters)
}
