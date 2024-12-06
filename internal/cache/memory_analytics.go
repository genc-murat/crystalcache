package cache

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type MemoryAnalytics struct {
	// General stats
	TotalAllocated int64
	TotalFreed     int64
	CurrentlyInUse int64
	MaxMemoryUsed  int64
	LastGCTime     time.Time

	// Per data structure stats
	StringMemory int64
	HashMemory   int64
	ListMemory   int64
	SetMemory    int64
	ZSetMemory   int64

	// Memory fragmentation
	FragmentationRatio float64

	// Operation stats
	AllocationCount int64
	FreeCount       int64

	// Size stats
	KeyCount        int64
	ExpiredKeyCount int64
	EvictedKeyCount int64
}

func (c *MemoryCache) GetMemoryAnalytics() *MemoryAnalytics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	analytics := &MemoryAnalytics{
		TotalAllocated:     int64(m.TotalAlloc),
		TotalFreed:         int64(m.Frees),
		CurrentlyInUse:     int64(m.Alloc),
		MaxMemoryUsed:      int64(m.Sys),
		LastGCTime:         time.Unix(0, int64(m.LastGC)),
		FragmentationRatio: float64(m.Sys-m.HeapInuse) / float64(m.HeapInuse),
		AllocationCount:    int64(m.Mallocs),
		FreeCount:          int64(m.Frees),
	}

	// Calculate per-structure memory usage
	c.calculateStructureMemory(analytics)

	return analytics
}

func (c *MemoryCache) calculateStructureMemory(analytics *MemoryAnalytics) {
	// String memory
	c.sets.Range(func(key, value interface{}) bool {
		k := key.(string)
		v := value.(string)
		size := int64(len(k) + len(v))
		atomic.AddInt64(&analytics.StringMemory, size)
		return true
	})

	// Hash memory
	c.hsets.Range(func(key, hash interface{}) bool {
		k := key.(string)
		h := hash.(*sync.Map)
		size := int64(len(k))
		h.Range(func(field, value interface{}) bool {
			f := field.(string)
			v := value.(string)
			size += int64(len(f) + len(v))
			return true
		})
		atomic.AddInt64(&analytics.HashMemory, size)
		return true
	})

	// List memory
	c.lists.Range(func(key, list interface{}) bool {
		k := key.(string)
		l := list.([]string)
		size := int64(len(k))
		for _, item := range l {
			size += int64(len(item))
		}
		atomic.AddInt64(&analytics.ListMemory, size)
		return true
	})

	// Set memory
	c.sets_.Range(func(key, set interface{}) bool {
		k := key.(string)
		s := set.(*sync.Map)
		size := int64(len(k))
		s.Range(func(member, _ interface{}) bool {
			m := member.(string)
			size += int64(len(m))
			return true
		})
		atomic.AddInt64(&analytics.SetMemory, size)
		return true
	})

	// ZSet memory
	c.zsets.Range(func(key, zset interface{}) bool {
		k := key.(string)
		zs := zset.(*sync.Map)
		size := int64(len(k))
		zs.Range(func(member, _ interface{}) bool {
			m := member.(string)
			size += int64(len(m)) + 8 // 8 bytes for float64 score
			return true
		})
		atomic.AddInt64(&analytics.ZSetMemory, size)
		return true
	})

	// Key statistics
	analytics.KeyCount = c.getKeyCount()
	analytics.ExpiredKeyCount = c.getExpiredKeyCount()
}

func (c *MemoryCache) getKeyCount() int64 {
	var count int64
	c.sets.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true // Tüm elemanları gezmeye devam et
	})
	return count
}

func (c *MemoryCache) getExpiredKeyCount() int64 {
	var count int64
	now := time.Now()

	c.expires.Range(func(_, expireTime interface{}) bool {
		if now.After(expireTime.(time.Time)) {
			atomic.AddInt64(&count, 1)
		}
		return true
	})

	return count
}

// Add memory monitoring capabilities
func (c *MemoryCache) StartMemoryMonitor(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			analytics := c.GetMemoryAnalytics()

			// Log analytics or expose metrics
			log.Printf("Memory Analytics: In Use: %d MB, Fragmentation: %.2f%%",
				analytics.CurrentlyInUse/(1024*1024),
				analytics.FragmentationRatio*100)

			// Check for high memory usage
			if analytics.FragmentationRatio > 1.5 {
				c.Defragment()
			}
		}
	}()
}

// Add memory usage limits
func (c *MemoryCache) SetMemoryLimit(maxBytes int64) {
	go func() {
		for {
			analytics := c.GetMemoryAnalytics()
			if analytics.CurrentlyInUse > maxBytes {
				c.evictKeys(maxBytes)
			}
			time.Sleep(time.Second)
		}
	}()
}

func (c *MemoryCache) evictKeys(targetBytes int64) {
	for {
		analytics := c.GetMemoryAnalytics()
		if analytics.CurrentlyInUse <= targetBytes {
			break
		}

		var keyToDelete interface{}
		c.sets.Range(func(key, _ interface{}) bool {
			keyToDelete = key
			return false // İlk bulunan anahtarı silmek için döngüyü sonlandır
		})

		if keyToDelete != nil {
			c.sets.Delete(keyToDelete)               // Anahtarı sil
			atomic.AddInt64(&c.stats.evictedKeys, 1) // Silme işlemini kaydet
		} else {
			break // Silinecek başka anahtar kalmadıysa çık
		}
	}
}
