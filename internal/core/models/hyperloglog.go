package models

import (
	"math"
	"math/bits"
)

const (
	HLL_P                 = 14         // Precision bits
	HLL_M                 = 1 << HLL_P // Register count (16384)
	HLL_MAX_ENCODING_SIZE = 12000      // Maximum size of dense encoding
)

// HyperLogLog struct
type HyperLogLog struct {
	Registers []uint8 // Register array to store bucket values
	Encoding  byte    // dense: 1, sparse: 2
	Size      int64   // Approximate number of unique elements
}

func NewHyperLogLog() *HyperLogLog {
	return &HyperLogLog{
		Registers: make([]uint8, HLL_M),
		Encoding:  1, // Start with dense encoding
		Size:      0,
	}
}

// Cardinality tahmini için helper method
func (h *HyperLogLog) Estimate() float64 {
	sum := 0.0
	zeros := 0

	for _, val := range h.Registers {
		sum += 1.0 / float64(uint64(1)<<val)
		if val == 0 {
			zeros++
		}
	}

	// HyperLogLog algorithm formula
	estimate := 0.7213 / sum * float64(HLL_M) * float64(HLL_M)

	// Small range correction
	if estimate <= 2.5*float64(HLL_M) {
		if zeros > 0 {
			estimate = float64(HLL_M) * math.Log(float64(HLL_M)/float64(zeros))
		}
	}

	// Large range correction
	if estimate > float64(1<<32)/30.0 {
		estimate = -float64(1<<32) * math.Log(1.0-estimate/float64(1<<32))
	}

	return estimate
}

// Add method to add new element
func (h *HyperLogLog) Add(hash uint64) bool {
	idx := hash & (HLL_M - 1)
	zeros := uint8(bits.LeadingZeros64(hash>>HLL_P)) + 1

	if zeros > h.Registers[idx] {
		h.Registers[idx] = zeros
		h.Size = int64(h.Estimate() + 0.5)
		return true
	}

	return false
}

// Merge method to merge another HLL into this one
func (h *HyperLogLog) Merge(other *HyperLogLog) {
	for i := 0; i < HLL_M; i++ {
		if other.Registers[i] > h.Registers[i] {
			h.Registers[i] = other.Registers[i]
		}
	}
	h.Size = int64(h.Estimate() + 0.5)
}
