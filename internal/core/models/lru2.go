package models

import (
	"container/list"
	"sync"
	"time"
)

type Entry struct {
	Key         string
	Value       interface{}
	LastAccess  time.Time
	PrevAccess  time.Time
	AccessCount int
	IsGhost     bool
	Size        int // byte
}

type LRU2Cache struct {
	capacity   int
	size       int
	items      map[string]*list.Element
	mainQueue  *list.List
	ghostQueue *list.List
	coroutine  int
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

func (c *LRU2Cache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, exists := c.items[key]; exists {
		entry := element.Value.(*Entry)
		if !entry.IsGhost {
			entry.PrevAccess = entry.LastAccess
			entry.LastAccess = time.Now()
			entry.AccessCount++

			c.mainQueue.MoveToBack(element)
			return entry.Value, true
		}
	}

	return nil, false
}

func (c *LRU2Cache) Set(key string, value interface{}, size int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	if element, exists := c.items[key]; exists {
		entry := element.Value.(*Entry)

		if entry.IsGhost && now.Sub(entry.LastAccess) >= time.Duration(c.coroutine)*time.Second {
			entry.IsGhost = false
			entry.Value = value
			entry.PrevAccess = entry.LastAccess
			entry.LastAccess = now
			entry.AccessCount++
			entry.Size = size

			c.ghostQueue.Remove(element)

			newElement := c.mainQueue.PushBack(entry)
			c.items[key] = newElement

			c.size += size
			c.evictIfNeeded()
			return true
		}

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

	entry := &Entry{
		Key:         key,
		Value:       value,
		LastAccess:  now,
		AccessCount: 1,
		IsGhost:     true,
		Size:        size,
	}

	element := c.ghostQueue.PushBack(entry)
	c.items[key] = element

	c.evictIfNeeded()
	return true
}

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

func (c *LRU2Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.mainQueue = list.New()
	c.ghostQueue = list.New()
	c.size = 0
}

func (c *LRU2Cache) evictIfNeeded() {
	for c.size > c.capacity {
		if c.ghostQueue.Len() > 0 {
			element := c.ghostQueue.Front()
			entry := element.Value.(*Entry)
			c.ghostQueue.Remove(element)
			delete(c.items, entry.Key)
			continue
		}

		if c.mainQueue.Len() > 0 {
			element := c.mainQueue.Front()
			entry := element.Value.(*Entry)
			c.size -= entry.Size
			c.mainQueue.Remove(element)
			delete(c.items, entry.Key)

			entry.IsGhost = true
			entry.Value = nil
			ghostElement := c.ghostQueue.PushBack(entry)
			c.items[entry.Key] = ghostElement
		}
	}
}

func (c *LRU2Cache) Stats() LRU2Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var totalAccessTime float64
	var accessCount int

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
