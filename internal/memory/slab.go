package memory

import (
	"sort"
	"sync"
	"unsafe"
)

// Slab boyutları - power of 2
const (
	MinSlabSize = 64      // En küçük slab boyutu (byte)
	MaxSlabSize = 1 << 20 // En büyük slab boyutu (1MB)
)

// Slab havuzu
type SlabPool struct {
	slabs   [][]byte        // Slab array'leri
	free    [][]int         // Her slab sınıfı için boş slot'lar
	classes []int           // Slab boyutları
	usage   map[uintptr]int // Pointer -> slab class mapping
	mu      sync.RWMutex
	stats   SlabStats
}

type SlabStats struct {
	TotalMemory     int64
	UsedMemory      int64
	FragmentedBytes int64
	SlabClasses     int
	AllocCount      int64
	FreeCount       int64
}

func NewSlabPool() *SlabPool {
	pool := &SlabPool{
		usage: make(map[uintptr]int),
	}
	pool.initSlabClasses()
	return pool
}

// Slab sınıflarını initialize et
func (sp *SlabPool) initSlabClasses() {
	size := MinSlabSize
	for size <= MaxSlabSize {
		sp.classes = append(sp.classes, size)
		sp.slabs = append(sp.slabs, make([]byte, 0))
		sp.free = append(sp.free, make([]int, 0))
		size *= 2
	}
	sp.stats.SlabClasses = len(sp.classes)
}

// Bellek ayır
func (sp *SlabPool) Allocate(size int) []byte {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	// Uygun slab class'ını bul
	class := sp.findSlabClass(size)
	if class == -1 {
		return nil
	}

	var memory []byte

	// Boş slot var mı kontrol et
	if len(sp.free[class]) > 0 {
		// Boş slot'u kullan
		slotIndex := sp.free[class][len(sp.free[class])-1]
		sp.free[class] = sp.free[class][:len(sp.free[class])-1]

		start := slotIndex * sp.classes[class]
		end := start + sp.classes[class]
		memory = sp.slabs[class][start:end]
	} else {
		// Yeni slab oluştur
		memory = make([]byte, sp.classes[class])
		sp.slabs[class] = append(sp.slabs[class], memory...)
	}

	// İstatistikleri güncelle
	sp.stats.AllocCount++
	sp.stats.UsedMemory += int64(len(memory))
	sp.usage[uintptr(unsafe.Pointer(&memory[0]))] = class

	return memory
}

// Belleği serbest bırak
func (sp *SlabPool) Free(memory []byte) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	ptr := uintptr(unsafe.Pointer(&memory[0]))
	if class, exists := sp.usage[ptr]; exists {
		// Slot'u free list'e ekle
		slotIndex := int(ptr-uintptr(unsafe.Pointer(&sp.slabs[class][0]))) / sp.classes[class]
		sp.free[class] = append(sp.free[class], slotIndex)

		// İstatistikleri güncelle
		sp.stats.FreeCount++
		sp.stats.UsedMemory -= int64(sp.classes[class])
		delete(sp.usage, ptr)
	}
}

// Defragmentasyon işlemi
func (sp *SlabPool) Defragment() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for class := range sp.classes {
		if len(sp.free[class]) == 0 {
			continue
		}

		// Boş slot'ları sırala
		sort.Ints(sp.free[class])

		// Sıralı boş slot'ları birleştir
		newSlabs := make([]byte, 0)
		newFree := make([]int, 0)

		usedSlots := make(map[int]bool)
		for ptr, cl := range sp.usage {
			if cl == class {
				slotIndex := int(ptr-uintptr(unsafe.Pointer(&sp.slabs[class][0]))) / sp.classes[class]
				usedSlots[slotIndex] = true
			}
		}

		// Kullanılan slot'ları yeni array'e kopyala
		slotCount := len(sp.slabs[class]) / sp.classes[class]
		for i := 0; i < slotCount; i++ {
			if usedSlots[i] {
				start := i * sp.classes[class]
				end := start + sp.classes[class]
				newSlabs = append(newSlabs, sp.slabs[class][start:end]...)
			}
		}

		// Pointer'ları güncelle
		sp.updatePointers(class, newSlabs)

		// Yeni array'leri ata
		sp.slabs[class] = newSlabs
		sp.free[class] = newFree
	}

	// İstatistikleri güncelle
	sp.updateStats()
}

// Memory compaction
func (sp *SlabPool) Compact() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for class := range sp.classes {
		if float64(len(sp.free[class]))/float64(len(sp.slabs[class])/sp.classes[class]) < 0.25 {
			continue // %25'ten az fragmantasyon varsa skip
		}

		// Yeni compacted array oluştur
		compactedSize := len(sp.slabs[class]) - (len(sp.free[class]) * sp.classes[class])
		compacted := make([]byte, compactedSize)

		// Kullanılan slot'ları kopyala
		writeIndex := 0
		readIndex := 0
		freeIndex := 0

		for readIndex < len(sp.slabs[class]) {
			if freeIndex < len(sp.free[class]) && sp.free[class][freeIndex]*sp.classes[class] == readIndex {
				// Bu slot boş, atla
				freeIndex++
				readIndex += sp.classes[class]
				continue
			}

			// Slot'u kopyala
			copy(compacted[writeIndex:], sp.slabs[class][readIndex:readIndex+sp.classes[class]])
			writeIndex += sp.classes[class]
			readIndex += sp.classes[class]
		}

		// Pointer'ları güncelle
		sp.updatePointers(class, compacted)

		// Yeni array'i ata
		sp.slabs[class] = compacted
		sp.free[class] = make([]int, 0)
	}

	sp.updateStats()
}

// İstatistik güncelleme
func (sp *SlabPool) updateStats() {
	var totalMem, usedMem, fragMem int64

	for class, slabs := range sp.slabs {
		totalMem += int64(len(slabs))
		usedMem += int64(len(slabs) - (len(sp.free[class]) * sp.classes[class]))
	}

	fragMem = totalMem - usedMem

	sp.stats.TotalMemory = totalMem
	sp.stats.UsedMemory = usedMem
	sp.stats.FragmentedBytes = fragMem
}

// Pointer güncelleme
func (sp *SlabPool) updatePointers(class int, newSlabs []byte) {
	newUsage := make(map[uintptr]int)
	for ptr, cl := range sp.usage {
		if cl == class {
			offset := ptr - uintptr(unsafe.Pointer(&sp.slabs[class][0]))
			newPtr := uintptr(unsafe.Pointer(&newSlabs[0])) + offset
			newUsage[newPtr] = cl
		} else {
			newUsage[ptr] = cl
		}
	}
	sp.usage = newUsage
}

// Uygun slab class'ını bul
func (sp *SlabPool) findSlabClass(size int) int {
	for i, classSize := range sp.classes {
		if size <= classSize {
			return i
		}
	}
	return -1
}

// İstatistikleri getir
func (sp *SlabPool) GetStats() SlabStats {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.stats
}
