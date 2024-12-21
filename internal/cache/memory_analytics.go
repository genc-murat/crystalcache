package cache

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
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

	HLLMemory         int64
	JSONMemory        int64
	StreamMemory      int64
	StreamGroupMemory int64
	BitmapMemory      int64

	GeoMemory        int64
	SuggestionMemory int64
	CMSMemory        int64
	CuckooMemory     int64
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

	// JSON data memory
	c.jsonData.Range(func(key, value interface{}) bool {
		k := key.(string)
		v := value.([]byte) // Assuming JSON is stored as []byte
		size := int64(len(k) + len(v))
		atomic.AddInt64(&analytics.JSONMemory, size)
		return true
	})

	// Stream entries memory
	c.streams.Range(func(key, stream interface{}) bool {
		k := key.(string)
		s := stream.(*sync.Map)
		size := int64(len(k))
		s.Range(func(id, entry interface{}) bool {
			// Assuming each entry has an ID (string) and payload ([]byte)
			entryID := id.(string)
			entryData := entry.([]byte)
			size += int64(len(entryID) + len(entryData))
			return true
		})
		atomic.AddInt64(&analytics.StreamMemory, size)
		return true
	})

	// Stream consumer groups memory
	c.streamGroups.Range(func(key, groups interface{}) bool {
		k := key.(string)
		g := groups.(*sync.Map)
		size := int64(len(k))
		g.Range(func(groupName, consumers interface{}) bool {
			gName := groupName.(string)
			size += int64(len(gName))
			// Add estimated size for consumer state (PEL, last-delivered-id, etc.)
			size += 256 // Estimated overhead per consumer group
			return true
		})
		atomic.AddInt64(&analytics.StreamGroupMemory, size)
		return true
	})

	// Bitmap memory
	c.bitmaps.Range(func(key, bitmap interface{}) bool {
		k := key.(string)
		b := bitmap.([]byte)
		size := int64(len(k) + len(b))
		atomic.AddInt64(&analytics.BitmapMemory, size)
		return true
	})

	// HyperLogLog memory
	c.hlls.Range(func(key, hll interface{}) bool {
		k := key.(string)
		h := hll.(*models.HyperLogLog)
		size := int64(len(k)) + h.GetMemoryUsage()
		atomic.AddInt64(&analytics.HLLMemory, size)
		return true
	})
	// Geo memory
	c.geoData.Range(func(key, geoSet interface{}) bool {
		k := key.(string)
		gs := geoSet.(*sync.Map)
		size := int64(len(k))
		gs.Range(func(member, point interface{}) bool {
			m := member.(string)
			p := point.(*models.GeoPoint)
			size += int64(len(m))         // member name
			size += 24                    // Longitude, Latitude (float64 * 2)
			size += int64(len(p.GeoHash)) // GeoHash string
			return true
		})
		atomic.AddInt64(&analytics.GeoMemory, size)
		return true
	})

	// Suggestion memory
	c.suggestions.Range(func(key, dict interface{}) bool {
		k := key.(string)
		d := dict.(*models.SuggestionDict)
		size := int64(len(k))
		for str, sug := range d.Entries {
			size += int64(len(str))         // String key
			size += int64(len(sug.String))  // Suggestion string
			size += 8                       // Score (float64)
			size += int64(len(sug.Payload)) // Payload
		}
		atomic.AddInt64(&analytics.SuggestionMemory, size)
		return true
	})

	// Count-Min Sketch memory
	c.cms.Range(func(key, sketch interface{}) bool {
		k := key.(string)
		cms := sketch.(*models.CountMinSketch)
		size := int64(len(k))
		size += int64(cms.Width * cms.Depth * 8) // uint64 cells
		size += int64(len(cms.HashSeed) * 8)     // Hash seeds
		atomic.AddInt64(&analytics.CMSMemory, size)
		return true
	})

	// Cuckoo Filter memory
	c.cuckooFilters.Range(func(key, filter interface{}) bool {
		k := key.(string)
		cf := filter.(*models.CuckooFilter)
		size := int64(len(k)) + cf.GetMemoryUsage()
		atomic.AddInt64(&analytics.CuckooMemory, size)
		return true
	})

	// Key statistics
	analytics.KeyCount = c.getKeyCount()
	analytics.ExpiredKeyCount = c.getExpiredKeyCount()
}

func (c *MemoryCache) getKeyCount() int64 {
	var count int64

	// Count keys in sets
	c.sets.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count keys in hash sets
	c.hsets.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count keys in lists
	c.lists.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count keys in sets_
	c.sets_.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count keys in sorted sets
	c.zsets.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count keys in JSON data
	c.jsonData.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count keys in streams
	c.streams.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count keys in stream groups
	c.streamGroups.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count keys in bitmaps
	c.bitmaps.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count Geo keys
	c.geoData.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count Suggestion keys
	c.suggestions.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count Count-Min Sketch keys
	c.cms.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count HyperLogLog keys
	c.hlls.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
	})

	// Count Cuckoo Filter keys
	c.cuckooFilters.Range(func(_, _ interface{}) bool {
		atomic.AddInt64(&count, 1)
		return true
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

		maps := [...]*sync.Map{
			c.sets,
			c.jsonData,
			c.streams,
			c.bitmaps,
			c.hsets,         // Hash maps
			c.lists,         // Lists
			c.sets_,         // Sets
			c.zsets,         // Sorted sets
			c.streamGroups,  // Stream groups
			c.geoData,       // Geo data
			c.suggestions,   // Suggestions
			c.cms,           // Count-Min Sketches
			c.hlls,          // HyperLogLog
			c.cuckooFilters, // Cuckoo Filters
		}

		evicted := false
		for _, m := range maps {
			var keyToDelete interface{}

			// Get the map's memory usage before deciding to evict
			memUsage := c.getMapMemoryUsage(m)

			// Get the first available key if memory usage is significant
			if memUsage > 0 {
				m.Range(func(key, _ interface{}) bool {
					keyToDelete = key
					return false // Stop after first key
				})

				if keyToDelete != nil {
					m.Delete(keyToDelete)
					atomic.AddInt64(&c.stats.evictedKeys, 1)
					evicted = true
					break
				}
			}
		}

		if !evicted {
			break
		}
	}
}

// Helper to estimate memory usage of a map
func (c *MemoryCache) getMapMemoryUsage(m *sync.Map) int64 {
	var size int64
	m.Range(func(key, value interface{}) bool {
		k := key.(string)
		size += int64(len(k))

		switch v := value.(type) {
		case string:
			size += int64(len(v))
		case []byte:
			size += int64(len(v))
		case *models.HyperLogLog:
			size += v.GetMemoryUsage()
		case *models.CuckooFilter:
			size += v.GetMemoryUsage()
		case *sync.Map: // For nested maps (hsets, sets_, etc.)
			v.Range(func(k, val interface{}) bool {
				size += int64(len(k.(string)))
				if str, ok := val.(string); ok {
					size += int64(len(str))
				}
				return true
			})
		}
		return true
	})
	return size
}
