package models

import (
	"math"
	"sort"
)

// Centroid represents a cluster in the T-Digest
type Centroid struct {
	Mean   float64
	Weight float64
}

// TDigest represents a T-Digest data structure
type TDigest struct {
	compression float64
	count       float64
	min         float64
	max         float64
	Centroids   []Centroid
}

// NewTDigest creates a new T-Digest with given compression parameter
func NewTDigest(compression float64) *TDigest {
	return &TDigest{
		Centroids:   make([]Centroid, 0),
		compression: compression,
		min:         math.Inf(1),  // Positive infinity
		max:         math.Inf(-1), // Negative infinity
	}
}

// Add adds a new value to the T-Digest
func (td *TDigest) Add(value float64) {
	td.AddWeighted(value, 1.0)
}

// AddWeighted adds a new value with a specified weight
func (td *TDigest) AddWeighted(value, weight float64) {
	if weight <= 0 {
		return
	}

	if value < td.min {
		td.min = value
	}
	if value > td.max {
		td.max = value
	}

	td.count += weight

	// Find the closest centroid
	var closest *Centroid
	minDistance := math.Inf(1)

	for i := range td.Centroids {
		dist := math.Abs(td.Centroids[i].Mean - value)
		if dist < minDistance {
			minDistance = dist
			closest = &td.Centroids[i]
		}
	}

	if closest == nil {
		td.Centroids = append(td.Centroids, Centroid{Mean: value, Weight: weight})
	} else {
		// Merge with closest centroid
		totalWeight := closest.Weight + weight
		closest.Mean = (closest.Mean*closest.Weight + value*weight) / totalWeight
		closest.Weight = totalWeight
	}

	// Compress if needed
	if float64(len(td.Centroids)) > 20*td.compression {
		td.compress()
	}
}

// compress reduces the number of centroids
func (td *TDigest) compress() {
	if len(td.Centroids) <= 1 {
		return
	}

	// Sort centroids by mean
	sort.Slice(td.Centroids, func(i, j int) bool {
		return td.Centroids[i].Mean < td.Centroids[j].Mean
	})

	newCentroids := make([]Centroid, 0)
	current := td.Centroids[0]

	for i := 1; i < len(td.Centroids); i++ {
		if current.Weight+td.Centroids[i].Weight <= td.compression {
			// Merge centroids
			totalWeight := current.Weight + td.Centroids[i].Weight
			current.Mean = (current.Mean*current.Weight + td.Centroids[i].Mean*td.Centroids[i].Weight) / totalWeight
			current.Weight = totalWeight
		} else {
			newCentroids = append(newCentroids, current)
			current = td.Centroids[i]
		}
	}
	newCentroids = append(newCentroids, current)
	td.Centroids = newCentroids
}

// Quantile returns the approximate value at a given quantile
func (td *TDigest) Quantile(q float64) float64 {
	if q < 0 || q > 1 {
		return math.NaN()
	}

	if len(td.Centroids) == 0 {
		return math.NaN()
	}

	if q == 0 {
		return td.min
	}
	if q == 1 {
		return td.max
	}

	sort.Slice(td.Centroids, func(i, j int) bool {
		return td.Centroids[i].Mean < td.Centroids[j].Mean
	})

	targetWeight := q * td.count
	cumWeight := 0.0

	for i, c := range td.Centroids {
		cumWeight += c.Weight
		if cumWeight >= targetWeight {
			if i == 0 {
				return c.Mean
			}
			// Linear interpolation between centroids
			prev := td.Centroids[i-1]
			weightDelta := cumWeight - c.Weight - (cumWeight - targetWeight)
			return prev.Mean + (c.Mean-prev.Mean)*(weightDelta/c.Weight)
		}
	}

	return td.max
}

// CDF returns the approximate cumulative distribution function
func (td *TDigest) CDF(x float64) float64 {
	if len(td.Centroids) == 0 {
		return math.NaN()
	}

	if x <= td.min {
		return 0
	}
	if x >= td.max {
		return 1
	}

	sort.Slice(td.Centroids, func(i, j int) bool {
		return td.Centroids[i].Mean < td.Centroids[j].Mean
	})

	cumWeight := 0.0
	for _, c := range td.Centroids {
		if c.Mean <= x {
			cumWeight += c.Weight
		} else {
			break
		}
	}

	return cumWeight / td.count
}

// Count returns the total number of points
func (td *TDigest) Count() float64 {
	return td.count
}

// Min returns the minimum value
func (td *TDigest) Min() float64 {
	return td.min
}

// Max returns the maximum value
func (td *TDigest) Max() float64 {
	return td.max
}

// Reset resets the T-Digest to its initial state
func (td *TDigest) Reset() {
	td.Centroids = make([]Centroid, 0)
	td.count = 0
	td.min = math.Inf(1)
	td.max = math.Inf(-1)
}

// Merge merges another T-Digest into this one
func (td *TDigest) Merge(other *TDigest) {
	if other == nil || len(other.Centroids) == 0 {
		return
	}

	// Update min/max
	if other.min < td.min {
		td.min = other.min
	}
	if other.max > td.max {
		td.max = other.max
	}

	// Merge centroids
	for _, c := range other.Centroids {
		td.AddWeighted(c.Mean, c.Weight)
	}
}

// TrimmedMean calculates the mean excluding values outside the given quantiles
func (td *TDigest) TrimmedMean(lowQuantile, highQuantile float64) float64 {
	if lowQuantile >= highQuantile || lowQuantile < 0 || highQuantile > 1 {
		return math.NaN()
	}

	sort.Slice(td.Centroids, func(i, j int) bool {
		return td.Centroids[i].Mean < td.Centroids[j].Mean
	})

	lowValue := td.Quantile(lowQuantile)
	highValue := td.Quantile(highQuantile)

	sum := 0.0
	weight := 0.0

	for _, c := range td.Centroids {
		if c.Mean >= lowValue && c.Mean <= highValue {
			sum += c.Mean * c.Weight
			weight += c.Weight
		}
	}

	if weight == 0 {
		return math.NaN()
	}

	return sum / weight
}

// GetMemoryUsage returns an estimation of memory usage in bytes
func (td *TDigest) GetMemoryUsage() int64 {
	// Each centroid uses 16 bytes (2 float64s)
	return int64(len(td.Centroids) * 16)
}

// Info returns information about the T-Digest
func (td *TDigest) Info() map[string]interface{} {
	return map[string]interface{}{
		"compression":   td.compression,
		"count":         td.count,
		"min":           td.min,
		"max":           td.max,
		"num_centroids": len(td.Centroids),
		"memory_usage":  td.GetMemoryUsage(),
	}
}
