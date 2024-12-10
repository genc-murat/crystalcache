package bitmap

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCountOps(t *testing.T) {
	// Create mock sync.Maps
	cache := &sync.Map{}
	version := &sync.Map{}

	// Initialize BasicOps and CountOps
	basicOps := NewBasicOps(cache, version)
	countOps := NewCountOps(basicOps)

	// Mock data for testing
	key := "testKey"
	bitmap := []byte{0b10101010, 0b11110000, 0b00001111}
	cache.Store(key, bitmap)

	t.Run("Test BitCount", func(t *testing.T) {
		count, err := countOps.BitCount(key, 0, 2)
		assert.NoError(t, err, "BitCount should not return an error")
		assert.Equal(t, int64(12), count, "BitCount should correctly count the number of set bits")
	})

	t.Run("Test BitCount with Negative Indices", func(t *testing.T) {
		count, err := countOps.BitCount(key, -3, -1)
		assert.NoError(t, err, "BitCount should not return an error with negative indices")
		assert.Equal(t, int64(12), count, "BitCount should correctly count the number of set bits in range")
	})

	t.Run("Test BitCount with Out-of-Bounds Indices", func(t *testing.T) {
		count, err := countOps.BitCount(key, -10, 10)
		assert.NoError(t, err, "BitCount should not return an error with out-of-bounds indices")
		assert.Equal(t, int64(12), count, "BitCount should count bits only within valid bounds")
	})

	t.Run("Test BitPos for Bit 1", func(t *testing.T) {
		pos, err := countOps.BitPos(key, 1, 0, 2, false)
		assert.NoError(t, err, "BitPos should not return an error")
		assert.Equal(t, int64(0), pos, "BitPos should return the correct position of the first bit set to 1")
	})

	t.Run("Test BitPos for Bit 0", func(t *testing.T) {
		pos, err := countOps.BitPos(key, 0, 0, 2, false)
		assert.NoError(t, err, "BitPos should not return an error")
		assert.Equal(t, int64(1), pos, "BitPos should return the correct position of the first bit set to 0")
	})

	t.Run("Test BitPos in Reverse for Bit 1", func(t *testing.T) {
		pos, err := countOps.BitPos(key, 1, 0, 2, true)
		assert.NoError(t, err, "BitPos should not return an error in reverse search")
		assert.Equal(t, int64(20), pos, "BitPos should return the correct position in reverse")
	})

	t.Run("Test BitPos for Missing Bit 1", func(t *testing.T) {
		newKey := "emptyKey"
		cache.Store(newKey, []byte{0b00000000})
		pos, err := countOps.BitPos(newKey, 1, 0, 0, false)
		assert.NoError(t, err, "BitPos should not return an error for missing bits")
		assert.Equal(t, int64(-1), pos, "BitPos should return -1 when the bit is not found")
	})

	t.Run("Test CountBitsInRange", func(t *testing.T) {
		b := byte(0b10101010) // Bits: 10101010
		count := countOps.CountBitsInRange(b, 2, 5)
		assert.Equal(t, 2, count, "CountBitsInRange should correctly count bits within the specified range")
	})

	t.Run("Test CountBitsInRange with Invalid Range", func(t *testing.T) {
		b := byte(0b10101010)
		count := countOps.CountBitsInRange(b, 5, 2)
		assert.Equal(t, 0, count, "CountBitsInRange should return 0 for an invalid range")
	})
}
