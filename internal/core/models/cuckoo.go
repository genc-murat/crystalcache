package models

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

const (
	bucketSize = 4   // Number of entries per bucket
	fingerBits = 8   // Number of bits for each fingerprint
	maxKicks   = 500 // Maximum number of kicks before declaring filter full
)

// CuckooFilter represents a Cuckoo Filter data structure
type CuckooFilter struct {
	capacity  uint64
	itemCount uint64
	buckets   [][]byte
	tagMask   byte
}

// CuckooInfo represents information about a Cuckoo Filter
type CuckooInfo struct {
	Size         uint64
	ItemCount    uint64
	FilterFilled float64
	BucketSize   int
	Expansion    int
}

// NewCuckooFilter creates a new Cuckoo Filter with a given capacity
func NewCuckooFilter(capacity uint64) *CuckooFilter {
	// Round up to next power of 2
	capacity = uint64(math.Pow(2, math.Ceil(math.Log2(float64(capacity)))))

	cf := &CuckooFilter{
		capacity: capacity,
		buckets:  make([][]byte, capacity),
		tagMask:  byte(math.Pow(2, fingerBits) - 1),
	}

	// Initialize buckets
	for i := range cf.buckets {
		cf.buckets[i] = make([]byte, bucketSize)
	}

	return cf
}

// GetMemoryUsage returns the memory usage of CuckooFilter in bytes
func (cf *CuckooFilter) GetMemoryUsage() int64 {
	var size int64
	for _, bucket := range cf.buckets {
		size += int64(len(bucket))
	}
	return size
}

// generateIndex creates the first index for an item
func (cf *CuckooFilter) generateIndex(item string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(item))
	return h.Sum64() % cf.capacity
}

// generateFingerprint creates a fingerprint for an item
func (cf *CuckooFilter) generateFingerprint(item string) byte {
	h := fnv.New32a()
	h.Write([]byte(item))
	return byte(h.Sum32() & uint32(cf.tagMask))
}

// alternateIndex calculates the alternate index for a given index and fingerprint
func (cf *CuckooFilter) alternateIndex(index uint64, fingerprint byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte{fingerprint})
	altIndex := h.Sum64()
	return (index ^ altIndex) % cf.capacity
}

// Add adds an item to the filter
func (cf *CuckooFilter) Add(item string) bool {
	// Generate fingerprint and initial index
	fingerprint := cf.generateFingerprint(item)
	i1 := cf.generateIndex(item)
	i2 := cf.alternateIndex(i1, fingerprint)

	// Try to insert in either bucket
	if cf.insertIntoBucket(i1, fingerprint) || cf.insertIntoBucket(i2, fingerprint) {
		cf.itemCount++
		return true
	}

	// Need to relocate existing items
	currIndex := i1
	currFingerprint := fingerprint
	for k := 0; k < maxKicks; k++ {
		// Randomly select bucket and entry
		bucket := cf.buckets[currIndex]
		randPos := randomInt(bucketSize)

		// Swap fingerprints
		currFingerprint, bucket[randPos] = bucket[randPos], currFingerprint

		// Calculate alternate location for displaced fingerprint
		currIndex = cf.alternateIndex(currIndex, currFingerprint)

		// Try to insert displaced fingerprint
		if cf.insertIntoBucket(currIndex, currFingerprint) {
			cf.itemCount++
			return true
		}
	}

	return false // Filter is too full
}

// AddNX adds an item if it doesn't exist
func (cf *CuckooFilter) AddNX(item string) bool {
	if cf.Exists(item) {
		return false
	}
	return cf.Add(item)
}

// Exists checks if an item might be in the filter
func (cf *CuckooFilter) Exists(item string) bool {
	fingerprint := cf.generateFingerprint(item)
	i1 := cf.generateIndex(item)
	i2 := cf.alternateIndex(i1, fingerprint)

	return cf.findInBucket(i1, fingerprint) || cf.findInBucket(i2, fingerprint)
}

// Delete removes an item from the filter if it exists
func (cf *CuckooFilter) Delete(item string) bool {
	fingerprint := cf.generateFingerprint(item)
	i1 := cf.generateIndex(item)
	i2 := cf.alternateIndex(i1, fingerprint)

	if cf.deleteFromBucket(i1, fingerprint) || cf.deleteFromBucket(i2, fingerprint) {
		cf.itemCount--
		return true
	}
	return false
}

// Count returns the number of copies of an item in the filter
func (cf *CuckooFilter) Count(item string) int {
	fingerprint := cf.generateFingerprint(item)
	i1 := cf.generateIndex(item)
	i2 := cf.alternateIndex(i1, fingerprint)

	count := 0
	count += cf.countInBucket(i1, fingerprint)
	count += cf.countInBucket(i2, fingerprint)
	return count
}

// Info returns information about the filter
func (cf *CuckooFilter) Info() CuckooInfo {
	return CuckooInfo{
		Size:         cf.capacity,
		BucketSize:   bucketSize,
		ItemCount:    cf.itemCount,
		FilterFilled: float64(cf.itemCount) / float64(cf.capacity*uint64(bucketSize)),
		Expansion:    0, // Not implemented yet
	}
}

// Helper functions

func (cf *CuckooFilter) insertIntoBucket(index uint64, fingerprint byte) bool {
	bucket := cf.buckets[index]
	for i := 0; i < bucketSize; i++ {
		if bucket[i] == 0 {
			bucket[i] = fingerprint
			return true
		}
	}
	return false
}

func (cf *CuckooFilter) findInBucket(index uint64, fingerprint byte) bool {
	bucket := cf.buckets[index]
	for i := 0; i < bucketSize; i++ {
		if bucket[i] == fingerprint {
			return true
		}
	}
	return false
}

func (cf *CuckooFilter) deleteFromBucket(index uint64, fingerprint byte) bool {
	bucket := cf.buckets[index]
	for i := 0; i < bucketSize; i++ {
		if bucket[i] == fingerprint {
			bucket[i] = 0
			return true
		}
	}
	return false
}

func (cf *CuckooFilter) countInBucket(index uint64, fingerprint byte) int {
	count := 0
	bucket := cf.buckets[index]
	for i := 0; i < bucketSize; i++ {
		if bucket[i] == fingerprint {
			count++
		}
	}
	return count
}

// ScanDump returns a part of the filter for serialization
func (cf *CuckooFilter) ScanDump(iter uint64) (uint64, []byte) {
	if iter >= cf.capacity {
		return 0, nil
	}

	// Return current bucket and next iterator
	return iter + 1, cf.buckets[iter]
}

// LoadChunk loads a part of the filter from serialization
func (cf *CuckooFilter) LoadChunk(iter uint64, data []byte) error {
	if iter >= cf.capacity {
		return fmt.Errorf("invalid iterator")
	}
	if len(data) != bucketSize {
		return fmt.Errorf("invalid chunk size")
	}

	copy(cf.buckets[iter], data)
	return nil
}

// Helper function to generate random numbers
func randomInt(max int) int {
	b := make([]byte, 8)
	rand.Read(b)
	return int(binary.LittleEndian.Uint64(b) % uint64(max))
}
