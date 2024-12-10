package bitmap

import (
	"math/bits"
)

type CountOps struct {
	basicOps *BasicOps
}

func NewCountOps(basicOps *BasicOps) *CountOps {
	return &CountOps{
		basicOps: basicOps,
	}
}

// BitCount returns the number of set bits (1) in the bitmap
func (c *CountOps) BitCount(key string, start, end int64) (int64, error) {
	bytes := c.basicOps.GetBitmap(key)
	if bytes == nil {
		return 0, nil
	}

	// Adjust negative indices
	if start < 0 {
		start = int64(len(bytes)) + start
	}
	if end < 0 {
		end = int64(len(bytes)) + end
	}

	// Boundary checks
	if start < 0 {
		start = 0
	}
	if end >= int64(len(bytes)) {
		end = int64(len(bytes)) - 1
	}

	// Count set bits in range
	var count int64
	for i := start; i <= end; i++ {
		count += int64(bits.OnesCount8(bytes[i]))
	}
	return count, nil
}

// BitPos finds the position of the first bit set to a given value (0 or 1)
func (c *CountOps) BitPos(key string, bit int, start, end int64, reverse bool) (int64, error) {
	bytes := c.basicOps.GetBitmap(key)
	if bytes == nil {
		if bit == 0 {
			return 0, nil // Empty bitmap is all zeros
		}
		return -1, nil
	}

	// Adjust negative indices
	if start < 0 {
		start = int64(len(bytes)) + start
	}
	if end < 0 {
		end = int64(len(bytes)) + end
	}

	// Boundary checks
	if start < 0 {
		start = 0
	}
	if end >= int64(len(bytes)) {
		end = int64(len(bytes)) - 1
	}

	// Search in appropriate direction
	if reverse {
		for i := end; i >= start; i-- {
			if pos := c.findBitInByte(bytes[i], bit, true); pos >= 0 {
				return i*8 + int64(pos), nil
			}
		}
	} else {
		for i := start; i <= end; i++ {
			if pos := c.findBitInByte(bytes[i], bit, false); pos >= 0 {
				return i*8 + int64(pos), nil
			}
		}
	}

	return -1, nil
}

// Helper methods

// findBitInByte finds the position of a bit value within a byte
func (c *CountOps) findBitInByte(b byte, bit int, reverse bool) int {
	if reverse {
		for i := 7; i >= 0; i-- {
			if ((b >> i) & 1) == byte(bit) {
				return 7 - i // Adjust reverse position correctly
			}
		}
	} else {
		for i := 0; i < 8; i++ {
			if ((b >> (7 - i)) & 1) == byte(bit) {
				return i
			}
		}
	}
	return -1
}

// CountBitsInRange counts bits in a specific range within a byte
func (c *CountOps) CountBitsInRange(b byte, start, end int) int {
	if start > end || start < 0 || end > 7 {
		return 0
	}

	// Create mask to clear bits before `start` and after `end`
	maskStart := byte(0xFF) >> uint(start) // Clear bits before `start`
	maskEnd := byte(0xFF) << uint(7-end)   // Clear bits after `end`
	mask := maskStart & maskEnd            // Combine masks

	// Apply mask and count the set bits
	return bits.OnesCount8(b & mask)
}
