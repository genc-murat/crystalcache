package bitmap

import (
	"fmt"
	"sync"
)

// BasicOps handles basic bitmap operations
type BasicOps struct {
	cache      *sync.Map
	version    *sync.Map
	keyMutexes sync.Map
}

// NewBasicOps creates a new BasicOps instance
func NewBasicOps(cache *sync.Map, version *sync.Map) *BasicOps {
	return &BasicOps{
		cache:   cache,
		version: version,
	}
}

// ErrOutOfBoundsOffset is returned when the requested bit offset is beyond the bitmap's bounds.
var ErrOutOfBoundsOffset = fmt.Errorf("ERR requested bit offset is out of bounds")

// GetBit retrieves the bit value (0 or 1) at the given offset within the bitmap associated with the key.
// If the key is not found, it returns 0 and nil error.
// If the value associated with the key is not a byte slice, it returns an error.
// If the offset is beyond the bounds of the bitmap, it returns 0 and an ErrOutOfBoundsOffset error.
func (b *BasicOps) GetBit(key string, offset int64) (int, error) {
	val, exists := b.cache.Load(key)
	if !exists {
		return 0, nil
	}

	valBytes, ok := val.([]byte)
	if !ok {
		return 0, fmt.Errorf("ERR invalid bitmap format")
	}

	byteIndex := offset / 8
	if int64(len(valBytes)) <= byteIndex {
		return 0, ErrOutOfBoundsOffset // Return specific error for out-of-bounds
	}

	bitIndex := offset % 8
	return int((valBytes[byteIndex] >> (7 - bitIndex)) & 1), nil
}

// Get or create a mutex for a given key.
func (b *BasicOps) getMutex(key string) *sync.Mutex {
	m, _ := b.keyMutexes.LoadOrStore(key, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// SetBit sets the bit at the given offset to the specified value (0 or 1) within the bitmap associated with the key.
// It returns the original bit value at the offset before the update.
// Returns an error if the value is not 0 or 1, or if there's an issue with the cache.
func (b *BasicOps) SetBit(key string, offset int64, value int) (int, error) {
	if value != 0 && value != 1 {
		return 0, fmt.Errorf("ERR bit value must be 0 or 1")
	}

	mu := b.getMutex(key)
	mu.Lock()
	defer mu.Unlock()

	valI, _ := b.cache.LoadOrStore(key, make([]byte, 0)) // Assuming LoadOrStore never returns an error
	valBytes, ok := valI.([]byte)
	if !ok {
		return 0, fmt.Errorf("ERR invalid bitmap format")
	}

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
		valBytes[byteIndex] &^= 1 << (7 - bitIndex)
	}

	// Store updated bitmap
	b.cache.Store(key, valBytes) // Assuming Store never returns an error
	b.incrementKeyVersion(key)

	return int(oldBit), nil
}

// incrementKeyVersion increments the version of a key
func (b *BasicOps) incrementKeyVersion(key string) {
	val, _ := b.version.LoadOrStore(key, int64(0))
	version := val.(int64)
	b.version.Store(key, version+1)
}

// GetBitmap returns the underlying byte slice for a key
func (b *BasicOps) GetBitmap(key string) []byte {
	val, exists := b.cache.Load(key)
	if !exists {
		return nil
	}

	valBytes, ok := val.([]byte)
	if !ok {
		return nil
	}

	return valBytes
}

// CreateBitmap creates a new bitmap of given size
func (b *BasicOps) CreateBitmap(key string, size int) error {
	if size < 0 {
		return fmt.Errorf("ERR size must be non-negative")
	}

	bitmap := make([]byte, size)
	b.cache.Store(key, bitmap)
	b.incrementKeyVersion(key)
	return nil
}
