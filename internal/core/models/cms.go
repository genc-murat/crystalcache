package models

import (
	"errors"
	"hash/fnv"
	"math"
)

var (
	// Count-Min Sketch errors
	ErrIncompatibleSketches = errors.New("sketches have incompatible dimensions")
	ErrSketchNotFound       = errors.New("sketch not found")
	ErrInvalidDimensions    = errors.New("invalid sketch dimensions")
	ErrInvalidProbability   = errors.New("invalid probability parameters")
)

// CountMinSketch represents a count-min sketch data structure
type CountMinSketch struct {
	Width    uint
	Depth    uint
	Count    uint64
	Matrix   [][]uint64
	HashSeed []uint64
}

// NewCountMinSketchByDim initializes a new Count-Min Sketch with given dimensions
func NewCountMinSketchByDim(width, depth uint) *CountMinSketch {
	cms := &CountMinSketch{
		Width:    width,
		Depth:    depth,
		Matrix:   make([][]uint64, depth),
		HashSeed: make([]uint64, depth),
	}

	// Initialize matrix
	for i := uint(0); i < depth; i++ {
		cms.Matrix[i] = make([]uint64, width)
		cms.HashSeed[i] = uint64(i + 1) // Simple hash seed
	}

	return cms
}

// NewCountMinSketchByProb initializes a Count-Min Sketch based on error probability
func NewCountMinSketchByProb(epsilon, delta float64) *CountMinSketch {
	width := uint(math.Ceil(math.E / epsilon))
	depth := uint(math.Ceil(math.Log(1 / delta)))
	return NewCountMinSketchByDim(width, depth)
}

// Hash generates hash for a row
func (cms *CountMinSketch) hash(item string, seed uint64) uint {
	h := fnv.New64a()
	h.Write([]byte(item))
	hash := h.Sum64()
	return uint((hash ^ seed) % uint64(cms.Width))
}

// Increment adds increment to item's count
func (cms *CountMinSketch) Increment(item string, increment uint64) {
	cms.Count += increment
	for i := uint(0); i < cms.Depth; i++ {
		j := cms.hash(item, cms.HashSeed[i])
		cms.Matrix[i][j] += increment
	}
}

// Query estimates the count of an item
func (cms *CountMinSketch) Query(item string) uint64 {
	min := uint64(math.MaxUint64)
	for i := uint(0); i < cms.Depth; i++ {
		j := cms.hash(item, cms.HashSeed[i])
		if cms.Matrix[i][j] < min {
			min = cms.Matrix[i][j]
		}
	}
	return min
}

// Merge combines another sketch into this one
func (cms *CountMinSketch) Merge(other *CountMinSketch) error {
	if cms.Width != other.Width || cms.Depth != other.Depth {
		return ErrIncompatibleSketches
	}

	cms.Count += other.Count
	for i := uint(0); i < cms.Depth; i++ {
		for j := uint(0); j < cms.Width; j++ {
			cms.Matrix[i][j] += other.Matrix[i][j]
		}
	}

	return nil
}

// Info returns information about the sketch
func (cms *CountMinSketch) Info() map[string]interface{} {
	return map[string]interface{}{
		"width": cms.Width,
		"depth": cms.Depth,
		"count": cms.Count,
	}
}
