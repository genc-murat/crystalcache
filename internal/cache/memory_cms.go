package cache

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// CMSInitByDim initializes a Count-Min Sketch (CMS) for the given key with specified width and depth.
// It creates a new CMS with the provided dimensions and stores it in the cache.
// The key version is incremented after storing the CMS.
//
// Parameters:
//   - key: A string representing the key for which the CMS is being initialized.
//   - width: An unsigned integer specifying the width of the CMS.
//   - depth: An unsigned integer specifying the depth of the CMS.
//
// Returns:
//   - error: An error if the initialization fails, otherwise nil.
func (c *MemoryCache) CMSInitByDim(key string, width, depth uint) error {
	sketch := models.NewCountMinSketchByDim(width, depth)
	c.cms.Store(key, sketch)
	c.incrementKeyVersion(key)
	return nil
}

// CMSInitByProb initializes a Count-Min Sketch (CMS) for the given key using the provided
// epsilon and delta values, which determine the accuracy and confidence of the sketch.
// The CMS is then stored in the memory cache and the key version is incremented.
//
// Parameters:
//   - key: The unique identifier for the CMS.
//   - epsilon: The error rate for the CMS.
//   - delta: The confidence level for the CMS.
//
// Returns:
//   - error: An error if the initialization fails, otherwise nil.
func (c *MemoryCache) CMSInitByProb(key string, epsilon, delta float64) error {
	sketch := models.NewCountMinSketchByProb(epsilon, delta)
	c.cms.Store(key, sketch)
	c.incrementKeyVersion(key)
	return nil
}

// CMSIncrBy increments the count of multiple items in a Count-Min Sketch associated with the given key.
// If the key does not exist in the cache, an error is returned.
// The lengths of the items and increments slices must be the same, otherwise an error is returned.
//
// Parameters:
//   - key: The key associated with the Count-Min Sketch in the cache.
//   - items: A slice of items whose counts need to be incremented.
//   - increments: A slice of increment values corresponding to each item.
//
// Returns:
//   - error: An error if the key does not exist or if the lengths of items and increments do not match.
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

// CMSQuery queries the Count-Min Sketch (CMS) for the given key and items.
// It returns a slice of counts corresponding to the frequency of each item in the CMS.
//
// Parameters:
//   - key: The key associated with the Count-Min Sketch in the cache.
//   - items: A slice of strings representing the items to query in the CMS.
//
// Returns:
//   - A slice of uint64 counts, where each count corresponds to the frequency of the respective item in the CMS.
//   - An error if the key does not exist in the cache.
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

// CMSMerge merges multiple Count-Min Sketches into a destination sketch with specified weights.
//
// Parameters:
//   - destination: The key for the destination Count-Min Sketch.
//   - sources: A slice of keys for the source Count-Min Sketches to be merged.
//   - weights: A slice of weights corresponding to each source sketch.
//
// Returns:
//   - error: An error if the number of sources and weights do not match, if a source key does not exist,
//     or if the sketches have incompatible dimensions.
//
// The function performs the following steps:
//  1. Checks if the number of sources matches the number of weights.
//  2. Loads or creates the destination sketch. If the destination does not exist, it uses the dimensions
//     of the first source sketch to create a new destination sketch.
//  3. Iterates over each source sketch, checks for existence and compatibility, and merges it into the
//     destination sketch using the specified weight.
//  4. Increments the version of the destination key.
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

// CMSInfo retrieves information about a Count-Min Sketch (CMS) associated with the given key.
// It returns a map containing the CMS information and an error if the key does not exist.
//
// Parameters:
//   - key: A string representing the key associated with the CMS.
//
// Returns:
//   - map[string]interface{}: A map containing the CMS information.
//   - error: An error if the key does not exist.
func (c *MemoryCache) CMSInfo(key string) (map[string]interface{}, error) {
	sketchI, exists := c.cms.Load(key)
	if !exists {
		return nil, fmt.Errorf("key does not exist")
	}

	sketch := sketchI.(*models.CountMinSketch)
	return sketch.Info(), nil
}

// defragCMS defragments the Count-Min Sketch (CMS) data structure used by the MemoryCache.
// It replaces the current CMS with a defragmented version by calling the defragSyncMap method.
func (c *MemoryCache) defragCMS() {
	c.cms = c.defragSyncMap(c.cms)
}
