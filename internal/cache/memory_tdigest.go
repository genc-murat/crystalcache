package cache

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// TDigestCreate creates a new t-digest with the specified compression factor and stores it in the cache with the given key.
// The compression factor must be a positive value.
//
// Parameters:
//
//	key - the key under which the t-digest will be stored
//	compression - the compression factor for the t-digest
//
// Returns:
//
//	error - an error if the compression factor is not positive, otherwise nil
func (c *MemoryCache) TDigestCreate(key string, compression float64) error {
	if compression <= 0 {
		return fmt.Errorf("compression must be positive")
	}

	tdigest := models.NewTDigest(compression)
	c.tdigests.Store(key, tdigest)
	c.incrementKeyVersion(key)
	return nil
}

// TDigestAdd adds one or more float64 values to the t-digest associated with the given key.
// If the key does not exist in the cache, an error is returned.
//
// Parameters:
//   - key: The key associated with the t-digest.
//   - values: One or more float64 values to be added to the t-digest.
//
// Returns:
//   - error: An error if the key does not exist, otherwise nil.
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

// TDigestMerge merges multiple T-Digests into a destination T-Digest within the memory cache.
// It takes a destination key, a slice of source keys, and a slice of weights as parameters.
// The number of source keys must match the number of weights.
//
// Parameters:
//   - destKey: The key for the destination T-Digest.
//   - sourceKeys: A slice of keys for the source T-Digests to be merged.
//   - weights: A slice of weights corresponding to each source T-Digest.
//
// Returns:
//   - error: An error if the number of source keys does not match the number of weights,
//     or if any source key is not found in the cache.
//
// The function retrieves or creates the destination T-Digest and merges each source T-Digest into it.
// After merging, it increments the version of the destination key.
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

// TDigestReset resets the TDigest associated with the given key in the memory cache.
// If the key does not exist, it returns an error indicating that the key was not found.
//
// Parameters:
//   - key: The key associated with the TDigest to be reset.
//
// Returns:
//   - error: An error if the key does not exist, otherwise nil.
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

// TDigestQuantile calculates the quantiles for the given key using the TDigest algorithm.
// It takes a key and a variadic number of quantiles as input and returns a slice of float64
// representing the quantile values and an error if the key does not exist.
//
// Parameters:
//   - key: The key associated with the TDigest.
//   - quantiles: A variadic number of float64 values representing the quantiles to be calculated.
//
// Returns:
//   - []float64: A slice of float64 values representing the calculated quantiles.
//   - error: An error if the key does not exist in the cache.
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

// TDigestMin retrieves the minimum value from the t-digest associated with the given key.
// If the key does not exist, it returns an error.
//
// Parameters:
//
//	key - The key associated with the t-digest.
//
// Returns:
//
//	float64 - The minimum value from the t-digest.
//	error - An error if the key does not exist.
func (c *MemoryCache) TDigestMin(key string) (float64, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return 0, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	return tdigest.Min(), nil
}

// TDigestMax retrieves the maximum value from the t-digest associated with the given key.
// If the key does not exist in the cache, it returns an error.
//
// Parameters:
//
//	key - The key associated with the t-digest.
//
// Returns:
//
//	float64 - The maximum value from the t-digest.
//	error - An error if the key is not found.
func (c *MemoryCache) TDigestMax(key string) (float64, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return 0, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	return tdigest.Max(), nil
}

// TDigestInfo retrieves information about a t-digest associated with the given key.
// It returns a map containing the t-digest information and an error if the key does not exist.
//
// Parameters:
//   - key: The key associated with the t-digest.
//
// Returns:
//   - map[string]interface{}: A map containing the t-digest information.
//   - error: An error if the key does not exist.
func (c *MemoryCache) TDigestInfo(key string) (map[string]interface{}, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return nil, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	return tdigest.Info(), nil
}

// TDigestCDF calculates the cumulative distribution function (CDF) values for the given
// key and a list of float64 values. It retrieves the TDigest associated with the key
// from the memory cache and computes the CDF for each value in the provided list.
// If the key does not exist in the cache, it returns an error.
//
// Parameters:
//   - key: A string representing the key to retrieve the TDigest from the cache.
//   - values: A variadic list of float64 values for which the CDF will be calculated.
//
// Returns:
//   - A slice of float64 values representing the CDF results for each input value.
//   - An error if the key does not exist in the cache.
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

// TDigestTrimmedMean calculates the trimmed mean of the values associated with the given key
// in the memory cache using the t-digest algorithm. The trimmed mean is computed by excluding
// the values below the specified lowQuantile and above the specified highQuantile.
//
// Parameters:
//   - key: The key associated with the t-digest in the memory cache.
//   - lowQuantile: The lower quantile threshold (0 <= lowQuantile <= 1).
//   - highQuantile: The upper quantile threshold (0 <= highQuantile <= 1).
//
// Returns:
//   - float64: The trimmed mean of the values within the specified quantile range.
//   - error: An error if the key is not found in the memory cache.
func (c *MemoryCache) TDigestTrimmedMean(key string, lowQuantile, highQuantile float64) (float64, error) {
	tdigestI, exists := c.tdigests.Load(key)
	if !exists {
		return 0, fmt.Errorf("key not found")
	}

	tdigest := tdigestI.(*models.TDigest)
	return tdigest.TrimmedMean(lowQuantile, highQuantile), nil
}

// defragTDigests defragments the t-digests stored in the MemoryCache.
// It consolidates the t-digests by calling the defragSyncMap method,
// which helps in optimizing memory usage and improving performance.
func (c *MemoryCache) defragTDigests() {
	defraggedTDigests := c.defragSyncMap(c.tdigests)
	if defraggedTDigests != c.tdigests {
		c.tdigests = defraggedTDigests
	}
}
