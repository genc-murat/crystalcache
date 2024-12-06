package cache

import (
	"log"
	"runtime"
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
	c.setsMu.RLock()
	for key, value := range c.sets {
		size := int64(len(key) + len(value))
		atomic.AddInt64(&analytics.StringMemory, size)
	}
	c.setsMu.RUnlock()

	// Hash memory
	c.hsetsMu.RLock()
	for key, hash := range c.hsets {
		size := int64(len(key))
		for field, value := range hash {
			size += int64(len(field) + len(value))
		}
		atomic.AddInt64(&analytics.HashMemory, size)
	}
	c.hsetsMu.RUnlock()

	// List memory
	c.listsMu.RLock()
	for key, list := range c.lists {
		size := int64(len(key))
		for _, item := range list {
			size += int64(len(item))
		}
		atomic.AddInt64(&analytics.ListMemory, size)
	}
	c.listsMu.RUnlock()

	// Set memory
	c.setsMu_.RLock()
	for key, set := range c.sets_ {
		size := int64(len(key))
		for member := range set {
			size += int64(len(member))
		}
		atomic.AddInt64(&analytics.SetMemory, size)
	}
	c.setsMu_.RUnlock()

	// ZSet memory
	c.zsetsMu.RLock()
	for key, zset := range c.zsets {
		size := int64(len(key))
		for member := range zset {
			size += int64(len(member)) + 8 // 8 bytes for float64
		}
		atomic.AddInt64(&analytics.ZSetMemory, size)
	}
	c.zsetsMu.RUnlock()

	// Key statistics
	analytics.KeyCount = int64(c.DBSize())
	analytics.ExpiredKeyCount = c.getExpiredKeyCount()
}

func (c *MemoryCache) getExpiredKeyCount() int64 {
	var count int64
	now := time.Now()

	c.setsMu.RLock()
	for _, expireTime := range c.expires {
		if now.After(expireTime) {
			count++
		}
	}
	c.setsMu.RUnlock()

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
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	for key := range c.sets {
		if c.GetMemoryAnalytics().CurrentlyInUse <= targetBytes {
			break
		}
		delete(c.sets, key)
		c.stats.IncrEvictedKeys()
	}
}
