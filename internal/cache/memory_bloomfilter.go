package cache

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// BFAdd adds an item to the Bloom filter associated with the given key.
// If the item is not already in the filter, it adds the item and increments the key version.
//
// Parameters:
//   - key: The key associated with the Bloom filter.
//   - item: The item to be added to the Bloom filter.
//
// Returns:
//   - bool: True if the item was not already in the filter, false otherwise.
//   - error: An error if any occurred during the operation.
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

// BFExists checks if the given item exists in the Bloom filter associated with the specified key.
// It returns true if the item exists, false otherwise. If the key does not exist in the cache,
// it returns false and no error.
//
// Parameters:
//   - key: The key associated with the Bloom filter.
//   - item: The item to check for existence in the Bloom filter.
//
// Returns:
//   - bool: True if the item exists in the Bloom filter, false otherwise.
//   - error: An error if there is an issue with the operation.
func (c *MemoryCache) BFExists(key string, item string) (bool, error) {
	filterI, exists := c.bfilters.Load(key)
	if !exists {
		return false, nil
	}

	filter := filterI.(*models.BloomFilter)
	return filter.Contains([]byte(item)), nil
}

// BFReserve reserves a Bloom filter for the given key with the specified error rate and capacity.
// It returns an error if the error rate is not between 0 and 1, or if the capacity is zero.
//
// Parameters:
//   - key: The key for which the Bloom filter is reserved.
//   - errorRate: The desired false positive rate for the Bloom filter. Must be between 0 and 1.
//   - capacity: The expected number of items to be stored in the Bloom filter. Must be positive.
//
// Returns:
//   - error: An error if the error rate is out of bounds or if the capacity is zero, otherwise nil.
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

// BFMAdd adds a list of items to the Bloom filter associated with the given key in the memory cache.
// It returns a slice of booleans indicating whether each item was newly added (true) or already existed (false).
// If any item is newly added, the key version is incremented.
//
// Parameters:
//   - key: The key associated with the Bloom filter in the memory cache.
//   - items: A slice of strings representing the items to be added to the Bloom filter.
//
// Returns:
//   - []bool: A slice of booleans indicating the result for each item (true if newly added, false if already existed).
//   - error: An error if any occurs during the operation.
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

// BFMExists checks if the given items exist in the Bloom filter associated with the specified key.
// It returns a slice of booleans indicating the existence of each item and an error if any occurs.
//
// Parameters:
//   - key: The key associated with the Bloom filter.
//   - items: A slice of strings representing the items to check for existence in the Bloom filter.
//
// Returns:
//   - A slice of booleans where each boolean corresponds to the existence of the respective item in the Bloom filter.
//   - An error if any occurs during the operation.
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

// BFInfo retrieves information about a Bloom filter associated with the given key.
// It returns a map containing various statistics about the Bloom filter, such as
// its size, hash count, element count, bitset size, number of set bits, false positive
// rate, and memory usage.
//
// Parameters:
//   - key: The key associated with the Bloom filter.
//
// Returns:
//   - map[string]interface{}: A map containing the Bloom filter statistics.
//   - error: An error if the key does not exist or any other issue occurs.
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

// BFCard returns the approximate count of items in the Bloom filter associated with the given key.
// If the Bloom filter does not exist, it returns 0 and no error.
//
// Parameters:
//   - key: The key associated with the Bloom filter.
//
// Returns:
//   - uint: The approximate count of items in the Bloom filter.
//   - error: An error if there is an issue retrieving the Bloom filter.
func (c *MemoryCache) BFCard(key string) (uint, error) {
	filterI, exists := c.bfilters.Load(key)
	if !exists {
		return 0, nil
	}

	filter := filterI.(*models.BloomFilter)
	return filter.ApproximateCount(), nil
}

// BFScanDump serializes and returns the configuration and bitset of a Bloom Filter
// associated with the given key in the memory cache. The function returns an iterator
// for further chunks (always 1 if successful, 0 otherwise), the serialized data, and
// an error if any.
//
// The serialized data format is as follows:
// [ExpectedItems(8 bytes)][FalsePositiveRate(8 bytes)][BitSetDataLength(8 bytes)][BitSetData]
//
// Parameters:
//   - key: The key associated with the Bloom Filter in the memory cache.
//   - iterator: The iterator for scanning (should be 0 for the initial call).
//
// Returns:
//   - int: The next iterator value (1 if successful, 0 otherwise).
//   - []byte: The serialized Bloom Filter data.
//   - error: An error if the key does not exist or if serialization fails.
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

// BFLoadChunk loads a chunk of data into a Bloom Filter in the memory cache.
//
// Parameters:
//   - key: The key associated with the Bloom Filter.
//   - iterator: The iterator value, which must be 1 for this function to proceed.
//   - data: The byte slice containing the Bloom Filter data.
//
// Returns:
//   - error: An error if the iterator is not 1, if the data format is invalid,
//     if the data is incomplete, or if deserialization of the bitset fails.
//
// The data byte slice is expected to have the following format:
//   - The first 8 bytes represent the expected number of items (uint64, big-endian).
//   - The next 8 bytes represent the false positive rate (float64, little-endian).
//   - The next 8 bytes represent the length of the bitset data (uint64, big-endian).
//   - The remaining bytes are the bitset data.
//
// The function deserializes the bitset data and stores the Bloom Filter in the memory cache.
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

// BFInsert inserts a list of items into a Bloom filter associated with the given key.
// It returns a slice of booleans indicating whether each item was already present in the filter,
// and an error if the error rate is not between 0 and 1 or if the capacity is zero.
//
// Parameters:
//   - key: A string representing the key associated with the Bloom filter.
//   - errorRate: A float64 representing the desired false positive rate of the Bloom filter.
//   - capacity: An unsigned integer representing the expected number of items to be inserted into the filter.
//   - items: A slice of strings representing the items to be inserted into the Bloom filter.
//
// Returns:
//   - A slice of booleans where each boolean indicates whether the corresponding item was already present in the filter.
//   - An error if the error rate is not between 0 and 1 or if the capacity is zero.
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

// defragBloomFilters defragments the bloom filters in the memory cache.
// It calls the defragSyncMap method to perform the defragmentation and
// updates the bloom filters map with the defragmented version.
func (c *MemoryCache) defragBloomFilters() {
	c.bfilters = c.defragSyncMap(c.bfilters)
}
