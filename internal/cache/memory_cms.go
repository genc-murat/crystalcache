package cache

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func (c *MemoryCache) CMSInitByDim(key string, width, depth uint) error {
	sketch := models.NewCountMinSketchByDim(width, depth)
	c.cms.Store(key, sketch)
	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) CMSInitByProb(key string, epsilon, delta float64) error {
	sketch := models.NewCountMinSketchByProb(epsilon, delta)
	c.cms.Store(key, sketch)
	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) CMSIncrBy(key string, items []string, increments []uint64) error {
	sketchI, exists := c.cms.Load(key)
	if !exists {
		return fmt.Errorf("key does not exist")
	}

	sketch := sketchI.(*models.CountMinSketch)
	if len(items) != len(increments) {
		return fmt.Errorf("items and increments must have same length")
	}

	for i, item := range items {
		sketch.Increment(item, increments[i])
	}

	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) CMSQuery(key string, items []string) ([]uint64, error) {
	sketchI, exists := c.cms.Load(key)
	if !exists {
		return nil, fmt.Errorf("key does not exist")
	}

	sketch := sketchI.(*models.CountMinSketch)
	counts := make([]uint64, len(items))

	for i, item := range items {
		counts[i] = sketch.Query(item)
	}

	return counts, nil
}

func (c *MemoryCache) CMSMerge(destination string, sources []string, weights []float64) error {
	if len(sources) != len(weights) {
		return fmt.Errorf("number of sources and weights must match")
	}

	// Load or create destination sketch
	var destSketch *models.CountMinSketch
	destSketchI, exists := c.cms.Load(destination)
	if !exists {
		// If destination doesn't exist, load the first source and use its dimensions
		sourceSketchI, exists := c.cms.Load(sources[0])
		if !exists {
			return fmt.Errorf("source key does not exist")
		}
		sourceSketch := sourceSketchI.(*models.CountMinSketch)
		destSketch = models.NewCountMinSketchByDim(sourceSketch.Width, sourceSketch.Depth)
		c.cms.Store(destination, destSketch)
	} else {
		destSketch = destSketchI.(*models.CountMinSketch)
	}

	// Merge each source with appropriate weight
	for i, sourceKey := range sources {
		sourceSketchI, exists := c.cms.Load(sourceKey)
		if !exists {
			return fmt.Errorf("source key %s does not exist", sourceKey)
		}
		sourceSketch := sourceSketchI.(*models.CountMinSketch)

		// Check compatibility
		if sourceSketch.Width != destSketch.Width || sourceSketch.Depth != destSketch.Depth {
			return fmt.Errorf("incompatible sketch dimensions")
		}

		// Merge with weight
		weight := weights[i]
		for d := uint(0); d < destSketch.Depth; d++ {
			for w := uint(0); w < destSketch.Width; w++ {
				destSketch.Matrix[d][w] += uint64(float64(sourceSketch.Matrix[d][w]) * weight)
			}
		}
		destSketch.Count += uint64(float64(sourceSketch.Count) * weight)
	}

	c.incrementKeyVersion(destination)
	return nil
}

func (c *MemoryCache) CMSInfo(key string) (map[string]interface{}, error) {
	sketchI, exists := c.cms.Load(key)
	if !exists {
		return nil, fmt.Errorf("key does not exist")
	}

	sketch := sketchI.(*models.CountMinSketch)
	return sketch.Info(), nil
}

func (c *MemoryCache) defragCMS() {
	c.cms = c.defragSyncMap(c.cms)
}
