package models

import (
	"hash/fnv"
	"math"
	"sync"
)

// BloomFilter yapısı
type BloomFilter struct {
	bitset    []bool // Bit array
	size      uint   // Bit array boyutu
	hashCount uint   // Hash fonksiyon sayısı
	count     uint   // Eklenen eleman sayısı
	mu        sync.RWMutex
}

// BloomFilter config yapısı
type BloomFilterConfig struct {
	ExpectedItems     uint    // Beklenen maksimum eleman sayısı
	FalsePositiveRate float64 // İstenen false positive oranı
}

// Yeni bir BloomFilter oluştur
func NewBloomFilter(config BloomFilterConfig) *BloomFilter {
	// Optimal büyüklük ve hash fonksiyon sayısını hesapla
	size := optimalSize(config.ExpectedItems, config.FalsePositiveRate)
	hashCount := optimalHashCount(size, config.ExpectedItems)

	return &BloomFilter{
		bitset:    make([]bool, size),
		size:      size,
		hashCount: hashCount,
		count:     0,
	}
}

// Optimal bit array boyutunu hesapla
func optimalSize(n uint, p float64) uint {
	return uint(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
}

// Optimal hash fonksiyon sayısını hesapla
func optimalHashCount(size uint, n uint) uint {
	return uint(math.Ceil(float64(size) / float64(n) * math.Log(2)))
}

// Hash değerlerini hesapla
func (bf *BloomFilter) getHashValues(data []byte) []uint {
	hashValues := make([]uint, bf.hashCount)
	h1 := fnv.New64()
	h2 := fnv.New64a()

	// İlk hash değerini hesapla
	h1.Write(data)
	hash1 := h1.Sum64()

	// İkinci hash değerini hesapla
	h2.Write(data)
	hash2 := h2.Sum64()

	// Double hashing tekniği ile diğer hash değerlerini üret
	for i := uint(0); i < bf.hashCount; i++ {
		hashValues[i] = uint((hash1 + uint64(i)*hash2) % uint64(bf.size))
	}

	return hashValues
}

// Add elemanı ekle
func (bf *BloomFilter) Add(item []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	// Hash değerlerini hesapla ve ilgili bitleri set et
	for _, hash := range bf.getHashValues(item) {
		bf.bitset[hash] = true
	}
	bf.count++
}

// Contains eleman var mı kontrol et
func (bf *BloomFilter) Contains(item []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// Tüm hash değerleri için bitleri kontrol et
	for _, hash := range bf.getHashValues(item) {
		if !bf.bitset[hash] {
			return false
		}
	}
	return true
}

// Clear bloom filter'ı temizle
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	bf.bitset = make([]bool, bf.size)
	bf.count = 0
}

// Approximate Count yaklaşık eleman sayısını döndür
func (bf *BloomFilter) ApproximateCount() uint {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setCount := uint(0)
	for _, bit := range bf.bitset {
		if bit {
			setCount++
		}
	}

	// Yaklaşık eleman sayısını hesapla
	return uint(-(float64(bf.size) / float64(bf.hashCount)) * math.Log(1-float64(setCount)/float64(bf.size)))
}

// FalsePositiveRate mevcut false positive oranını hesapla
func (bf *BloomFilter) FalsePositiveRate() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setCount := 0
	for _, bit := range bf.bitset {
		if bit {
			setCount++
		}
	}

	// False positive olasılığını hesapla
	probability := math.Pow(float64(setCount)/float64(bf.size), float64(bf.hashCount))
	return probability
}

// Stats bloom filter istatistiklerini döndür
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
