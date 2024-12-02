package models

import (
	"hash/fnv"
	"math"
	"sync"
)

type BloomFilter struct {
	bitset    []bool
	size      uint
	hashCount uint
	count     uint
	mu        sync.RWMutex
}

type BloomFilterConfig struct {
	ExpectedItems     uint
	FalsePositiveRate float64
}

func NewBloomFilter(config BloomFilterConfig) *BloomFilter {
	size := optimalSize(config.ExpectedItems, config.FalsePositiveRate)
	hashCount := optimalHashCount(size, config.ExpectedItems)

	return &BloomFilter{
		bitset:    make([]bool, size),
		size:      size,
		hashCount: hashCount,
		count:     0,
	}
}

func optimalSize(n uint, p float64) uint {
	return uint(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
}

func optimalHashCount(size uint, n uint) uint {
	return uint(math.Ceil(float64(size) / float64(n) * math.Log(2)))
}

func (bf *BloomFilter) getHashValues(data []byte) []uint {
	hashValues := make([]uint, bf.hashCount)
	h1 := fnv.New64()
	h2 := fnv.New64a()

	h1.Write(data)
	hash1 := h1.Sum64()

	h2.Write(data)
	hash2 := h2.Sum64()

	for i := uint(0); i < bf.hashCount; i++ {
		hashValues[i] = uint((hash1 + uint64(i)*hash2) % uint64(bf.size))
	}

	return hashValues
}

func (bf *BloomFilter) Add(item []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for _, hash := range bf.getHashValues(item) {
		bf.bitset[hash] = true
	}
	bf.count++
}

func (bf *BloomFilter) Contains(item []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for _, hash := range bf.getHashValues(item) {
		if !bf.bitset[hash] {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	bf.bitset = make([]bool, bf.size)
	bf.count = 0
}

func (bf *BloomFilter) ApproximateCount() uint {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setCount := uint(0)
	for _, bit := range bf.bitset {
		if bit {
			setCount++
		}
	}

	return uint(-(float64(bf.size) / float64(bf.hashCount)) * math.Log(1-float64(setCount)/float64(bf.size)))
}

func (bf *BloomFilter) FalsePositiveRate() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setCount := 0
	for _, bit := range bf.bitset {
		if bit {
			setCount++
		}
	}

	probability := math.Pow(float64(setCount)/float64(bf.size), float64(bf.hashCount))
	return probability
}

type BloomFilterStats struct {
	Size              uint
	HashCount         uint
	Count             uint
	BitsetSize        uint
	SetBits           uint
	FalsePositiveRate float64
	MemoryUsage       uint // bytes
}

func (bf *BloomFilter) Stats() BloomFilterStats {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setBits := uint(0)
	for _, bit := range bf.bitset {
		if bit {
			setBits++
		}
	}

	return BloomFilterStats{
		Size:              bf.size,
		HashCount:         bf.hashCount,
		Count:             bf.count,
		BitsetSize:        uint(len(bf.bitset)),
		SetBits:           setBits,
		FalsePositiveRate: bf.FalsePositiveRate(),
		MemoryUsage:       uint(len(bf.bitset) / 8), // bits to bytes
	}
}
