package cache

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// CFReserve reserves a new Cuckoo filter for the given key with the specified capacity.
// It creates a new Cuckoo filter using the provided capacity and stores it in the cache.
// The key version is incremented after storing the filter.
//
// Parameters:
//
//	key - the key for which the Cuckoo filter is reserved
//	capacity - the capacity of the new Cuckoo filter
//
// Returns:
//
//	error - if there is an error during the reservation process
func (c *MemoryCache) CFReserve(key string, capacity uint64) error {
	filter := models.NewCuckooFilter(capacity)
	c.cuckooFilters.Store(key, filter)
	c.incrementKeyVersion(key)
	return nil
}

// CFAdd adds an item to the Cuckoo filter associated with the given key.
// If the filter does not exist, it returns false and an error.
// If the item is successfully added, it increments the version of the key.
//
// Parameters:
//   - key: The key associated with the Cuckoo filter.
//   - item: The item to be added to the Cuckoo filter.
//
// Returns:
//   - bool: True if the item was successfully added, false otherwise.
//   - error: An error if the filter does not exist.
func (c *MemoryCache) CFAdd(key string, item string) (bool, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return false, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	success := filter.Add(item)
	if success {
		c.incrementKeyVersion(key)
	}
	return success, nil
}

// CFAddNX attempts to add an item to the cuckoo filter associated with the given key,
// only if the item does not already exist in the filter. If the filter does not exist,
// it returns an error.
//
// Parameters:
//   - key: The key associated with the cuckoo filter.
//   - item: The item to be added to the cuckoo filter.
//
// Returns:
//   - bool: True if the item was successfully added, false if the item already exists.
//   - error: An error if the filter does not exist.
func (c *MemoryCache) CFAddNX(key string, item string) (bool, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return false, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	success := filter.AddNX(item)
	if success {
		c.incrementKeyVersion(key)
	}
	return success, nil
}

// CFInsert inserts a list of items into the cuckoo filter associated with the given key.
// If the key does not exist, a new cuckoo filter is created with a default capacity.
// It returns a slice of booleans indicating whether each item was successfully inserted,
// and an error if any occurred during the process.
//
// Parameters:
//   - key: The key associated with the cuckoo filter.
//   - items: A slice of strings representing the items to be inserted.
//
// Returns:
//   - []bool: A slice of booleans where each value indicates if the corresponding item was successfully inserted.
//   - error: An error if any occurred during the insertion process.
func (c *MemoryCache) CFInsert(key string, items []string) ([]bool, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		// Create new filter with default capacity
		filter := models.NewCuckooFilter(uint64(len(items) * 2))
		c.cuckooFilters.Store(key, filter)
		filterI = filter
	}

	filter := filterI.(*models.CuckooFilter)
	results := make([]bool, len(items))
	changed := false

	for i, item := range items {
		results[i] = filter.Add(item)
		if results[i] {
			changed = true
		}
	}

	if changed {
		c.incrementKeyVersion(key)
	}
	return results, nil
}

// CFInsertNX inserts the given items into the cuckoo filter associated with the specified key,
// only if they do not already exist in the filter. If the key does not have an associated filter,
// a new filter is created with a default capacity.
//
// Parameters:
//   - key: The key associated with the cuckoo filter.
//   - items: A slice of strings representing the items to be inserted.
//
// Returns:
//   - A slice of booleans indicating whether each item was successfully inserted (true) or already existed (false).
//   - An error, if any occurred during the operation.
//
// If any item is successfully inserted, the version of the key is incremented.
func (c *MemoryCache) CFInsertNX(key string, items []string) ([]bool, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		// Create new filter with default capacity
		filter := models.NewCuckooFilter(uint64(len(items) * 2))
		c.cuckooFilters.Store(key, filter)
		filterI = filter
	}

	filter := filterI.(*models.CuckooFilter)
	results := make([]bool, len(items))
	changed := false

	for i, item := range items {
		results[i] = filter.AddNX(item)
		if results[i] {
			changed = true
		}
	}

	if changed {
		c.incrementKeyVersion(key)
	}
	return results, nil
}

// CFDel deletes an item from the cuckoo filter associated with the given key.
// If the filter does not exist, it returns false and an error.
// If the item is successfully deleted, it increments the version of the key.
//
// Parameters:
//
//	key: The key associated with the cuckoo filter.
//	item: The item to be deleted from the cuckoo filter.
//
// Returns:
//
//	bool: True if the item was successfully deleted, false otherwise.
//	error: An error if the filter does not exist.
func (c *MemoryCache) CFDel(key string, item string) (bool, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return false, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	deleted := filter.Delete(item)
	if deleted {
		c.incrementKeyVersion(key)
	}
	return deleted, nil
}

// CFCount returns the count of the specified item in the cuckoo filter associated with the given key.
// If the filter does not exist, it returns an error.
//
// Parameters:
//
//	key: The key associated with the cuckoo filter.
//	item: The item to count in the cuckoo filter.
//
// Returns:
//
//	int: The count of the item in the cuckoo filter.
//	error: An error if the filter does not exist.
func (c *MemoryCache) CFCount(key string, item string) (int, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return 0, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	return filter.Count(item), nil
}

// CFExists checks if the given item exists in the Cuckoo filter associated with the specified key.
// It returns true if the item exists, false if it does not, and an error if the filter does not exist.
//
// Parameters:
//
//	key  - The key associated with the Cuckoo filter.
//	item - The item to check for existence in the filter.
//
// Returns:
//
//	bool - True if the item exists in the filter, false otherwise.
//	error - An error if the filter does not exist.
func (c *MemoryCache) CFExists(key string, item string) (bool, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return false, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	return filter.Exists(item), nil
}

// CFMExists checks the existence of multiple items in a Cuckoo filter associated with a given key.
// It returns a slice of booleans indicating the existence of each item and an error if the filter does not exist.
//
// Parameters:
//   - key: The key associated with the Cuckoo filter.
//   - items: A slice of strings representing the items to check for existence in the filter.
//
// Returns:
//   - A slice of booleans where each boolean corresponds to the existence of the respective item in the filter.
//   - An error if the filter associated with the key does not exist.
func (c *MemoryCache) CFMExists(key string, items []string) ([]bool, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return nil, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	results := make([]bool, len(items))

	for i, item := range items {
		results[i] = filter.Exists(item)
	}

	return results, nil
}

// CFInfo retrieves information about a Cuckoo filter associated with the given key.
// It returns a pointer to a CuckooInfo struct containing the filter's information,
// or an error if the filter does not exist.
//
// Parameters:
//   - key: The key associated with the Cuckoo filter.
//
// Returns:
//   - *models.CuckooInfo: A pointer to the CuckooInfo struct containing the filter's information.
//   - error: An error if the filter does not exist.
func (c *MemoryCache) CFInfo(key string) (*models.CuckooInfo, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return nil, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	info := filter.Info()
	return &info, nil
}

// CFScanDump scans and dumps the data from the Cuckoo filter associated with the given key.
// It returns the next iteration value, the dumped data as a byte slice, and an error if the filter does not exist.
//
// Parameters:
//   - key: The key associated with the Cuckoo filter.
//   - iter: The current iteration value.
//
// Returns:
//   - uint64: The next iteration value.
//   - []byte: The dumped data as a byte slice.
//   - error: An error if the filter does not exist.
func (c *MemoryCache) CFScanDump(key string, iter uint64) (uint64, []byte, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return 0, nil, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	nextIter, data := filter.ScanDump(iter)
	return nextIter, data, nil
}

// CFLoadChunk loads a chunk of data into the cuckoo filter associated with the given key.
// If the filter does not exist, it returns an error.
//
// Parameters:
//   - key: The key associated with the cuckoo filter.
//   - iter: The iteration or chunk index to load the data into.
//   - data: The byte slice containing the data to be loaded.
//
// Returns:
//   - error: An error if the filter does not exist or if there is an issue loading the chunk.
func (c *MemoryCache) CFLoadChunk(key string, iter uint64, data []byte) error {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	return filter.LoadChunk(iter, data)
}

// defragCuckooFilters defragments the cuckoo filters in the MemoryCache.
// It calls the defragSyncMap method to perform the defragmentation and
// updates the cuckooFilters field with the defragmented data.
func (c *MemoryCache) defragCuckooFilters() {
	c.cuckooFilters = c.defragSyncMap(c.cuckooFilters)
}
