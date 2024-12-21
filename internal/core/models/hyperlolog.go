package models

import (
	"math"
	"math/bits"
)

const (
	hllP                       = 14                                   // Precision parameter
	hllM                       = 1 << hllP                            // Number of registers
	hllAlpha                   = 0.7213 / (1.0 + 1.079/float64(hllM)) // Alpha constant for bias correction
	thresholdForLinearCounting = float64(hllM * 5 / 2)                // Threshold for switching to linear counting
)

// HyperLogLog represents the HyperLogLog probabilistic data structure
type HyperLogLog struct {
	registers []byte
	size      uint64
}

// NewHyperLogLog creates a new HyperLogLog with default precision
func NewHyperLogLog() *HyperLogLog {
	return &HyperLogLog{
		registers: make([]byte, hllM),
		size:      0,
	}
}

// Add adds a value to the HyperLogLog
func (hll *HyperLogLog) Add(value uint64) bool {
	// Get the bucket index and pattern from the hash
	bucket := value & (hllM - 1)
	pattern := value >> hllP

	// Count trailing zeros + 1
	zeros := uint8(1)
	if pattern != 0 {
		zeros = uint8(bits.TrailingZeros64(pattern)) + 1
	}

	// Update register if new value is larger
	if zeros > hll.registers[bucket] {
		hll.registers[bucket] = zeros
		hll.size = 0 // Reset cached size
		return true
	}
	return false
}

// Count returns the estimated cardinality
func (hll *HyperLogLog) Count() uint64 {
	if hll.size > 0 {
		return hll.size
	}

	// Calculate harmonicMean
	sum := 0.0
	zeros := 0
	for _, val := range hll.registers {
		if val == 0 {
			zeros++
			continue
		}
		sum += math.Pow(2.0, -float64(val))
	}

	// Apply bias correction
	estimate := hllAlpha * float64(hllM*hllM) / sum

	// Linear counting for small cardinalities
	if zeros > 0 {
		linearEst := float64(hllM) * math.Log(float64(hllM)/float64(zeros))
		if linearEst <= thresholdForLinearCounting {
			estimate = linearEst
		}
	}

	hll.size = uint64(estimate)
	return hll.size
}

// Merge combines another HyperLogLog into this one
func (hll *HyperLogLog) Merge(other *HyperLogLog) {
	for i := 0; i < hllM; i++ {
		if other.registers[i] > hll.registers[i] {
			hll.registers[i] = other.registers[i]
			hll.size = 0 // Reset cached size
		}
	}
}

// Debug returns internal state for debugging
func (hll *HyperLogLog) Debug() map[string]interface{} {
	return map[string]interface{}{
		"encoding":    "raw",
		"size":        len(hll.registers),
		"regwidth":    8,
		"sparseness":  calculateSparseness(hll.registers),
		"nonZeroRegs": countNonZeroRegisters(hll.registers),
	}
}

// Helper functions
func calculateSparseness(registers []byte) float64 {
	nonZero := 0
	for _, reg := range registers {
		if reg != 0 {
			nonZero++
		}
	}
	return 1.0 - float64(nonZero)/float64(len(registers))
}

func countNonZeroRegisters(registers []byte) int {
	count := 0
	for _, reg := range registers {
		if reg != 0 {
			count++
		}
	}
	return count
}
