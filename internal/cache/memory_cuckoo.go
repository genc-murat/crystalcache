package cache

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func (c *MemoryCache) CFReserve(key string, capacity uint64) error {
	filter := models.NewCuckooFilter(capacity)
	c.cuckooFilters.Store(key, filter)
	c.incrementKeyVersion(key)
	return nil
}

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

func (c *MemoryCache) CFCount(key string, item string) (int, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return 0, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	return filter.Count(item), nil
}

func (c *MemoryCache) CFExists(key string, item string) (bool, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return false, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	return filter.Exists(item), nil
}

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

func (c *MemoryCache) CFInfo(key string) (*models.CuckooInfo, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return nil, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	info := filter.Info()
	return &info, nil
}

func (c *MemoryCache) CFScanDump(key string, iter uint64) (uint64, []byte, error) {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return 0, nil, fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	nextIter, data := filter.ScanDump(iter)
	return nextIter, data, nil
}

func (c *MemoryCache) CFLoadChunk(key string, iter uint64, data []byte) error {
	filterI, exists := c.cuckooFilters.Load(key)
	if !exists {
		return fmt.Errorf("filter does not exist")
	}

	filter := filterI.(*models.CuckooFilter)
	return filter.LoadChunk(iter, data)
}

func (c *MemoryCache) defragCuckooFilters() {
	c.cuckooFilters = c.defragSyncMap(c.cuckooFilters)
}
