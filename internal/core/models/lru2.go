package models

import (
	"container/list"
	"sync"
	"time"
)

// Cache entry yapısı
type Entry struct {
	Key         string
	Value       interface{}
	LastAccess  time.Time // Son erişim zamanı
	PrevAccess  time.Time // Önceki erişim zamanı
	AccessCount int       // Toplam erişim sayısı
	IsGhost     bool      // Ghost entry flag'i
	Size        int       // Entry boyutu (byte)
}

// LRU2Cache yapısı
type LRU2Cache struct {
	capacity   int                      // Maksimum kapasite
	size       int                      // Mevcut boyut
	items      map[string]*list.Element // Key -> list element mapping
	mainQueue  *list.List               // Ana queue (çift erişimli elemanlar)
	ghostQueue *list.List               // Ghost queue (tek erişimli elemanlar)
	coroutine  int                      // İki erişim arasındaki minimum süre
	mu         sync.RWMutex
}

type LRU2Stats struct {
	Size           int
	Capacity       int
	MainQueueSize  int
	GhostQueueSize int
	HitCount       int64
	MissCount      int64
	EvictionCount  int64
	AvgAccessTime  float64
}

func NewLRU2Cache(capacity int, coroutine int) *LRU2Cache {
	return &LRU2Cache{
		capacity:   capacity,
		items:      make(map[string]*list.Element),
		mainQueue:  list.New(),
		ghostQueue: list.New(),
		coroutine:  coroutine,
	}
}

// Get elemanı al
func (c *LRU2Cache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ana queue'da ara
	if element, exists := c.items[key]; exists {
		entry := element.Value.(*Entry)
		if !entry.IsGhost {
			// Erişim bilgilerini güncelle
			entry.PrevAccess = entry.LastAccess
			entry.LastAccess = time.Now()
			entry.AccessCount++

			// Queue'da en sona taşı
			c.mainQueue.MoveToBack(element)
			return entry.Value, true
		}
	}

	return nil, false
}

// Set elemanı ekle/güncelle
func (c *LRU2Cache) Set(key string, value interface{}, size int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Mevcut entry'yi kontrol et
	if element, exists := c.items[key]; exists {
		entry := element.Value.(*Entry)

		// Ghost entry ise ve coroutine süresi geçtiyse main queue'ya taşı
		if entry.IsGhost && now.Sub(entry.LastAccess) >= time.Duration(c.coroutine)*time.Second {
			entry.IsGhost = false
			entry.Value = value
			entry.PrevAccess = entry.LastAccess
			entry.LastAccess = now
			entry.AccessCount++
			entry.Size = size

			// Ghost queue'dan çıkar
			c.ghostQueue.Remove(element)

			// Main queue'ya ekle
			newElement := c.mainQueue.PushBack(entry)
			c.items[key] = newElement

			c.size += size
			c.evictIfNeeded()
			return true
		}

		// Normal entry güncelleme
		if !entry.IsGhost {
			oldSize := entry.Size
			entry.Value = value
			entry.PrevAccess = entry.LastAccess
			entry.LastAccess = now
			entry.AccessCount++
			entry.Size = size

			c.mainQueue.MoveToBack(element)
			c.size = c.size - oldSize + size
			c.evictIfNeeded()
			return true
		}
	}

	// Yeni entry oluştur
	entry := &Entry{
		Key:         key,
		Value:       value,
		LastAccess:  now,
		AccessCount: 1,
		IsGhost:     true,
		Size:        size,
	}

	// Ghost queue'ya ekle
	element := c.ghostQueue.PushBack(entry)
	c.items[key] = element

	// Boyut kontrolü
	c.evictIfNeeded()
	return true
}

// Remove elemanı sil
func (c *LRU2Cache) Remove(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, exists := c.items[key]; exists {
		entry := element.Value.(*Entry)
		if !entry.IsGhost {
			c.size -= entry.Size
		}

		if entry.IsGhost {
			c.ghostQueue.Remove(element)
		} else {
			c.mainQueue.Remove(element)
		}

		delete(c.items, key)
		return true
	}
	return false
}

// Clear cache'i temizle
func (c *LRU2Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.mainQueue = list.New()
	c.ghostQueue = list.New()
	c.size = 0
}

// evictIfNeeded kapasiteyi aşan durumda elemanları çıkar
func (c *LRU2Cache) evictIfNeeded() {
	for c.size > c.capacity {
		// Önce ghost entries'leri temizle
		if c.ghostQueue.Len() > 0 {
			element := c.ghostQueue.Front()
			entry := element.Value.(*Entry)
			c.ghostQueue.Remove(element)
			delete(c.items, entry.Key)
			continue
		}

		// Sonra main queue'dan çıkar
		if c.mainQueue.Len() > 0 {
			element := c.mainQueue.Front()
			entry := element.Value.(*Entry)
			c.size -= entry.Size
			c.mainQueue.Remove(element)
			delete(c.items, entry.Key)

			// Ghost entry olarak ekle
			entry.IsGhost = true
			entry.Value = nil
			ghostElement := c.ghostQueue.PushBack(entry)
			c.items[entry.Key] = ghostElement
		}
	}
}

// Stats cache istatistiklerini döndür
func (c *LRU2Cache) Stats() LRU2Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var totalAccessTime float64
	var accessCount int

	// Ana queue istatistikleri
	for e := c.mainQueue.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*Entry)
		if entry.AccessCount > 1 {
			totalAccessTime += entry.LastAccess.Sub(entry.PrevAccess).Seconds()
			accessCount++
		}
	}

	avgAccessTime := 0.0
	if accessCount > 0 {
		avgAccessTime = totalAccessTime / float64(accessCount)
	}

	return LRU2Stats{
		Size:           c.size,
		Capacity:       c.capacity,
		MainQueueSize:  c.mainQueue.Len(),
		GhostQueueSize: c.ghostQueue.Len(),
		AvgAccessTime:  avgAccessTime,
	}
}
