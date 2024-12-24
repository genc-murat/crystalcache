package cache

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// TOPKReserve initializes a TopK sketch
func (c *MemoryCache) TOPKReserve(key string, topk, capacity int, decay float64) error {
	if topk <= 0 || capacity <= 0 || decay < 0 || decay > 1 {
		return fmt.Errorf("ERR invalid parameters")
	}

	sketch := models.NewTopK(topk, capacity, decay)
	c.topks.Store(key, sketch)
	c.incrementKeyVersion(key)
	return nil
}

// TOPKAdd adds items to a TopK sketch
func (c *MemoryCache) TOPKAdd(key string, items ...string) ([]bool, error) {
	sketchI, exists := c.topks.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR key does not exist")
	}

	sketch := sketchI.(*models.TopK)
	results := sketch.Add(items...)
	c.incrementKeyVersion(key)
	return results, nil
}

// TOPKIncrBy increases counts of items in a TopK sketch
func (c *MemoryCache) TOPKIncrBy(key string, itemsWithCount map[string]int64) ([]bool, error) {
	sketchI, exists := c.topks.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR key does not exist")
	}

	sketch := sketchI.(*models.TopK)
	results := make([]bool, len(itemsWithCount))
	i := 0
	for item, count := range itemsWithCount {
		results[i] = sketch.IncrBy(item, count)
		i++
	}
	c.incrementKeyVersion(key)
	return results, nil
}

// TOPKQuery checks existence of items in a TopK sketch
func (c *MemoryCache) TOPKQuery(key string, items ...string) ([]bool, error) {
	sketchI, exists := c.topks.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR key does not exist")
	}

	sketch := sketchI.(*models.TopK)
	return sketch.Query(items...), nil
}

// TOPKCount returns counts of items in a TopK sketch
func (c *MemoryCache) TOPKCount(key string, items ...string) ([]int64, error) {
	sketchI, exists := c.topks.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR key does not exist")
	}

	sketch := sketchI.(*models.TopK)
	return sketch.Count(items...), nil
}

// TOPKList returns the full list of items in a TopK sketch
func (c *MemoryCache) TOPKList(key string) ([]struct {
	Item  string
	Count int64
}, error) {
	sketchI, exists := c.topks.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR key does not exist")
	}

	sketch := sketchI.(*models.TopK)
	return sketch.List(), nil
}

// TOPKInfo returns information about a TopK sketch
func (c *MemoryCache) TOPKInfo(key string) (map[string]interface{}, error) {
	sketchI, exists := c.topks.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR key does not exist")
	}

	sketch := sketchI.(*models.TopK)
	return sketch.Info(), nil
}

func (c *MemoryCache) defragTopK() {
	c.topks = c.defragSyncMap(c.topks)
}
