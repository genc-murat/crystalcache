package bitmap

import (
	"sync"
	"testing"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/stretchr/testify/assert"
)

func TestFieldOps(t *testing.T) {
	// Create mock sync.Maps
	cache := &sync.Map{}
	version := &sync.Map{}

	// Initialize BasicOps and FieldOps
	basicOps := NewBasicOps(cache, version)
	fieldOps := NewFieldOps(basicOps)

	// Test data
	key := "testKey"
	bitmap := []byte{0b00000000, 0b11111111}
	cache.Store(key, bitmap)

	t.Run("Test BitField GET", func(t *testing.T) {
		commands := []models.BitFieldCommand{
			{Op: "GET", Type: "u4", Offset: 0}, // Get 4 unsigned bits from offset 0
			{Op: "GET", Type: "i8", Offset: 8}, // Get 8 signed bits from offset 8
		}

		results, err := fieldOps.BitField(key, commands)
		assert.NoError(t, err, "BitField GET should not return an error")
		assert.Equal(t, []int64{0, -1}, results, "BitField GET should return correct results")
	})

	t.Run("Test BitField SET", func(t *testing.T) {
		commands := []models.BitFieldCommand{
			{Op: "SET", Type: "u4", Offset: 0, Value: 10}, // Set 4 unsigned bits at offset 0
		}

		results, err := fieldOps.BitField(key, commands)
		assert.NoError(t, err, "BitField SET should not return an error")
		assert.Equal(t, []int64{0}, results, "BitField SET should return the old value")

		// Verify the update
		newValue, _ := fieldOps.bitfieldGet(key, "u4", 0)
		assert.Equal(t, int64(10), newValue, "BitField SET should correctly update the bitmap")
	})

	t.Run("Test BitField INCRBY", func(t *testing.T) {
		commands := []models.BitFieldCommand{
			{Op: "INCRBY", Type: "u4", Offset: 0, Increment: 5}, // Increment 4 unsigned bits at offset 0
		}

		results, err := fieldOps.BitField(key, commands)
		assert.NoError(t, err, "BitField INCRBY should not return an error")
		assert.Equal(t, []int64{15}, results, "BitField INCRBY should return the new value")

		// Verify the update
		newValue, _ := fieldOps.bitfieldGet(key, "u4", 0)
		assert.Equal(t, int64(15), newValue, "BitField INCRBY should correctly update the bitmap")
	})

	t.Run("Test BitFieldRO", func(t *testing.T) {
		commands := []models.BitFieldCommand{
			{Op: "GET", Type: "u4", Offset: 0}, // Get 4 unsigned bits from offset 0
		}

		results, err := fieldOps.BitFieldRO(key, commands)
		assert.NoError(t, err, "BitFieldRO should not return an error")
		assert.Equal(t, []int64{15}, results, "BitFieldRO should return correct results")
	})

	t.Run("Test BitFieldRO with Invalid Command", func(t *testing.T) {
		commands := []models.BitFieldCommand{
			{Op: "SET", Type: "u4", Offset: 0, Value: 10}, // Invalid for BitFieldRO
		}

		_, err := fieldOps.BitFieldRO(key, commands)
		assert.Error(t, err, "BitFieldRO should return an error for invalid commands")
		assert.EqualError(t, err, "ERR BITFIELD_RO only supports GET operation")
	})

	t.Run("Test parseBitfieldType", func(t *testing.T) {
		bits, signed, err := fieldOps.parseBitfieldType("i16")
		assert.NoError(t, err, "parseBitfieldType should not return an error")
		assert.Equal(t, 16, bits, "Bit size should be correctly parsed")
		assert.True(t, signed, "Signed flag should be correctly set")

		_, _, err = fieldOps.parseBitfieldType("x8")
		assert.Error(t, err, "parseBitfieldType should return an error for invalid type")
	})
}
