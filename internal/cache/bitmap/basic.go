package bitmap

import (
	"fmt"
	"sync"
)

// BasicOps handles basic bitmap operations
type BasicOps struct {
	cache   *sync.Map
	version *sync.Map
}

// NewBasicOps creates a new BasicOps instance
func NewBasicOps(cache *sync.Map, version *sync.Map) *BasicOps {
	return &BasicOps{
		cache:   cache,
		version: version,
	}
}

// GetBit returns the bit value at offset
func (b *BasicOps) GetBit(key string, offset int64) (int, error) {
	val, exists := b.cache.Load(key)
	if !exists {
		return 0, nil
	}

	valBytes := val.([]byte)
	byteIndex := offset / 8
	if int64(len(valBytes)) <= byteIndex {
		return 0, nil
	}

	bitIndex := offset % 8
	return int((valBytes[byteIndex] >> (7 - bitIndex)) & 1), nil
}

// SetBit sets the bit at offset to value and returns the old value
func (b *BasicOps) SetBit(key string, offset int64, value int) (int, error) {
	if value != 0 && value != 1 {
		return 0, fmt.Errorf("ERR bit value must be 0 or 1")
	}

	valI, _ := b.cache.LoadOrStore(key, make([]byte, 0))
	valBytes := valI.([]byte)

	byteIndex := offset / 8
	bitIndex := offset % 8

	// Extend the bitmap if needed
	if int64(len(valBytes)) <= byteIndex {
		newBytes := make([]byte, byteIndex+1)
		copy(newBytes, valBytes)
		valBytes = newBytes
	}

	// Get old bit value
	oldBit := (valBytes[byteIndex] >> (7 - bitIndex)) & 1

	// Set new bit value
	if value == 1 {
		valBytes[byteIndex] |= 1 << (7 - bitIndex)
	} else {
		valBytes[byteIndex] &= ^(1 << (7 - bitIndex))
	}

	// Store updated bitmap
	b.cache.Store(key, valBytes)
	b.incrementKeyVersion(key)

	return int(oldBit), nil
}

// Helper methods

// incrementKeyVersion increments the version of a key
func (b *BasicOps) incrementKeyVersion(key string) {
	for {
		var version int64
		oldVersionI, _ := b.version.LoadOrStore(key, version)
		oldVersion := oldVersionI.(int64)
		if b.version.CompareAndSwap(key, oldVersion, oldVersion+1) {
			break
		}
	}
}

// GetBitmap returns the underlying byte slice for a key
func (b *BasicOps) GetBitmap(key string) []byte {
	val, exists := b.cache.Load(key)
	if !exists {
		return nil
	}
	return val.([]byte)
}

// CreateBitmap creates a new bitmap of given size
func (b *BasicOps) CreateBitmap(key string, size int) error {
	bitmap := make([]byte, size)
	b.cache.Store(key, bitmap)
	b.incrementKeyVersion(key)
	return nil
}
