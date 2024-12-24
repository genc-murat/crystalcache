package cache

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func (c *MemoryCache) TDigestCreate(key string, compression float64) error {
	if compression <= 0 {
		return fmt.Errorf("compression must be positive")
	}

	tdigest := models.NewTDigest(compression)
	c.tdigests.Store(key, tdigest)
	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) TDigestAdd(key string, values ...float64) error {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	for _, value := range values {
		tdigest.Add(value)
	}

	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) TDigestMerge(destKey string, sourceKeys []string, weights []float64) error {
	if len(sourceKeys) != len(weights) {
		return fmt.Errorf("number of sources and weights must match")
	}

	// Get or create destination T-Digest
	destTDigestI, exists := c.tdigests.Load(destKey)
	var destTDigest *models.TDigest
	if !exists {
		destTDigest = models.NewTDigest(100.0) // Default compression
		c.tdigests.Store(destKey, destTDigest)
	} else {
		destTDigest = destTDigestI.(*models.TDigest)
	}

	// Merge each source
	for _, sourceKey := range sourceKeys {
		sourceTDigestI, exists := c.tdigests.Load(sourceKey)
		if !exists {
			return fmt.Errorf("source key %s not found", sourceKey)
		}

		sourceTDigest := sourceTDigestI.(*models.TDigest)
		destTDigest.Merge(sourceTDigest)
	}

	c.incrementKeyVersion(destKey)
	return nil
}

func (c *MemoryCache) TDigestReset(key string) error {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	tdigest.Reset()
	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) TDigestQuantile(key string, quantiles ...float64) ([]float64, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	results := make([]float64, len(quantiles))
	for i, q := range quantiles {
		results[i] = tdigest.Quantile(q)
	}

	return results, nil
}

func (c *MemoryCache) TDigestMin(key string) (float64, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return 0, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	return tdigest.Min(), nil
}

func (c *MemoryCache) TDigestMax(key string) (float64, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return 0, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	return tdigest.Max(), nil
}

func (c *MemoryCache) TDigestInfo(key string) (map[string]interface{}, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	return tdigest.Info(), nil
}

func (c *MemoryCache) TDigestCDF(key string, values ...float64) ([]float64, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	results := make([]float64, len(values))
	for i, v := range values {
		results[i] = tdigest.CDF(v)
	}

	return results, nil
}

func (c *MemoryCache) TDigestTrimmedMean(key string, lowQuantile, highQuantile float64) (float64, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return 0, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	return tdigest.TrimmedMean(lowQuantile, highQuantile), nil
}

func (c *MemoryCache) defragTDigests() {
	c.tdigests = c.defragSyncMap(c.tdigests)
}
