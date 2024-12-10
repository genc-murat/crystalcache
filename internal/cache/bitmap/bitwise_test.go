package bitmap

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitwiseOps(t *testing.T) {
	// Create mock sync.Maps
	cache := &sync.Map{}
	version := &sync.Map{}

	// Initialize BasicOps and BitwiseOps
	basicOps := NewBasicOps(cache, version)
	bitwiseOps := NewBitwiseOps(basicOps)

	// Setup test data
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	destKey := "destKey"

	cache.Store(key1, []byte{0b10101010, 0b11110000})
	cache.Store(key2, []byte{0b11001100, 0b10101010})
	cache.Store(key3, []byte{0b11111111, 0b00001111})

	t.Run("Test BitOp AND", func(t *testing.T) {
		length, err := bitwiseOps.BitOp("AND", destKey, key1, key2, key3)
		assert.NoError(t, err, "BitOp AND should not return an error")
		assert.Equal(t, int64(2), length, "BitOp AND should return the correct result length")

		result, _ := cache.Load(destKey)
		expected := []byte{0b10001000, 0b00000000}
		assert.Equal(t, expected, result, "BitOp AND should return the correct result")
	})

	t.Run("Test BitOp OR", func(t *testing.T) {
		length, err := bitwiseOps.BitOp("OR", destKey, key1, key2, key3)
		assert.NoError(t, err, "BitOp OR should not return an error")
		assert.Equal(t, int64(2), length, "BitOp OR should return the correct result length")

		result, _ := cache.Load(destKey)
		expected := []byte{0b11111111, 0b11111111}
		assert.Equal(t, expected, result, "BitOp OR should return the correct result")
	})

	t.Run("Test BitOp XOR", func(t *testing.T) {
		length, err := bitwiseOps.BitOp("XOR", destKey, key1, key2, key3)
		assert.NoError(t, err, "BitOp XOR should not return an error")
		assert.Equal(t, int64(2), length, "BitOp XOR should return the correct result length")

		result, _ := cache.Load(destKey)
		expected := []byte{0x99, 0x55} // Corrected expected result
		assert.Equal(t, expected, result, "BitOp XOR should return the correct result")
	})

	t.Run("Test BitOp NOT", func(t *testing.T) {
		length, err := bitwiseOps.BitOp("NOT", destKey, key1)
		assert.NoError(t, err, "BitOp NOT should not return an error")
		assert.Equal(t, int64(2), length, "BitOp NOT should return the correct result length")

		result, _ := cache.Load(destKey)
		expected := []byte{0b01010101, 0b00001111}
		assert.Equal(t, expected, result, "BitOp NOT should return the correct result")
	})

	t.Run("Test BitOp NOT with Multiple Keys", func(t *testing.T) {
		_, err := bitwiseOps.BitOp("NOT", destKey, key1, key2)
		assert.Error(t, err, "BitOp NOT should return an error with multiple keys")
		assert.EqualError(t, err, "ERR BITOP NOT must be called with a single source key")
	})

	t.Run("Test BitOp with Unknown Operation", func(t *testing.T) {
		_, err := bitwiseOps.BitOp("UNKNOWN", destKey, key1)
		assert.Error(t, err, "BitOp should return an error for unknown operations")
		assert.EqualError(t, err, "ERR unknown operation 'UNKNOWN'")
	})

	t.Run("Test BitOp with No Keys", func(t *testing.T) {
		_, err := bitwiseOps.BitOp("AND", destKey)
		assert.Error(t, err, "BitOp should return an error when no keys are provided")
		assert.EqualError(t, err, "ERR wrong number of arguments for 'bitop' command")
	})
}
