package bitmap

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicOps(t *testing.T) {
	// Create mock sync.Maps
	cache := &sync.Map{}
	version := &sync.Map{}

	// Initialize BasicOps
	basicOps := NewBasicOps(cache, version)

	// Mock data
	key := "testKey"

	t.Run("Test SetBit and GetBit", func(t *testing.T) {
		offset := int64(5)
		value := 1

		// Set bit
		oldValue, err := basicOps.SetBit(key, offset, value)
		assert.NoError(t, err, "SetBit should not return an error")
		assert.Equal(t, 0, oldValue, "Old bit value should be 0")

		// Get bit
		newValue, err := basicOps.GetBit(key, offset)
		assert.NoError(t, err, "GetBit should not return an error")
		assert.Equal(t, value, newValue, "New bit value should match the set value")
	})

	t.Run("Test SetBit with Invalid Value", func(t *testing.T) {
		offset := int64(5)
		invalidValue := 2

		_, err := basicOps.SetBit(key, offset, invalidValue)
		assert.Error(t, err, "SetBit should return an error for invalid value")
		assert.EqualError(t, err, "ERR bit value must be 0 or 1")
	})

	t.Run("Test Extend Bitmap", func(t *testing.T) {
		offset := int64(20)
		value := 1

		// Set a bit beyond the current bitmap size
		_, err := basicOps.SetBit(key, offset, value)
		assert.NoError(t, err, "SetBit should not return an error for extending the bitmap")

		// Verify bitmap size
		bitmap := basicOps.GetBitmap(key)
		assert.GreaterOrEqual(t, len(bitmap), 3, "Bitmap should extend to accommodate the offset")
	})

	t.Run("Test GetBit Out of Bounds", func(t *testing.T) {
		// Attempt to get a bit outside the current bitmap size
		offset := int64(50)
		value, err := basicOps.GetBit(key, offset)
		assert.NoError(t, err, "GetBit should not return an error for out-of-bounds access")
		assert.Equal(t, 0, value, "GetBit should return 0 for out-of-bounds access")
	})

	t.Run("Test GetBitmap", func(t *testing.T) {
		// Get the full bitmap
		bitmap := basicOps.GetBitmap(key)
		assert.NotNil(t, bitmap, "GetBitmap should return a non-nil byte slice")
		assert.Equal(t, 3, len(bitmap), "GetBitmap should return the expected size")
	})

	t.Run("Test CreateBitmap", func(t *testing.T) {
		newKey := "newBitmap"
		size := 10

		// Create a new bitmap
		err := basicOps.CreateBitmap(newKey, size)
		assert.NoError(t, err, "CreateBitmap should not return an error")

		// Verify bitmap size
		bitmap := basicOps.GetBitmap(newKey)
		assert.NotNil(t, bitmap, "GetBitmap should return a non-nil byte slice")
		assert.Equal(t, size, len(bitmap), "Created bitmap should have the specified size")
	})

	t.Run("Test CreateBitmap with Invalid Size", func(t *testing.T) {
		newKey := "invalidBitmap"
		invalidSize := -1

		// Attempt to create a bitmap with a negative size
		err := basicOps.CreateBitmap(newKey, invalidSize)
		assert.Error(t, err, "CreateBitmap should return an error for invalid size")
		assert.EqualError(t, err, "ERR size must be non-negative")
	})

	t.Run("Test Key Versioning", func(t *testing.T) {
		// Get initial version
		initialVersion, _ := version.LoadOrStore(key, int64(0))

		// Update bitmap
		_, err := basicOps.SetBit(key, 2, 1)
		assert.NoError(t, err, "SetBit should not return an error")

		// Get updated version
		updatedVersion, _ := version.Load(key)
		assert.Greater(t, updatedVersion.(int64), initialVersion.(int64), "Version should increment after bitmap modification")
	})
}
