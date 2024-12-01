package cache

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type MemoryCache struct {
	sets    map[string]string
	hsets   map[string]map[string]string
	expires map[string]time.Time
	setsMu  sync.RWMutex
	hsetsMu sync.RWMutex
}

func NewMemoryCache() *MemoryCache {
	mc := &MemoryCache{
		sets:    make(map[string]string),
		hsets:   make(map[string]map[string]string),
		expires: make(map[string]time.Time),
	}

	// Expire check goroutine'u
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			mc.cleanExpired()
		}
	}()

	return mc
}

func (c *MemoryCache) cleanExpired() {
	now := time.Now()
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	for key, expireTime := range c.expires {
		if now.After(expireTime) {
			delete(c.sets, key)
			delete(c.expires, key)
		}
	}
}

func (c *MemoryCache) Incr(key string) (int, error) {
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	val, exists := c.sets[key]
	if !exists {
		c.sets[key] = "1"
		return 1, nil
	}

	num, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer")
	}

	num++
	c.sets[key] = strconv.Itoa(num)
	return num, nil
}

func (c *MemoryCache) Expire(key string, seconds int) error {
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	if _, exists := c.sets[key]; !exists {
		return nil // Redis returns 0 if key doesn't exist
	}

	c.expires[key] = time.Now().Add(time.Duration(seconds) * time.Second)
	return nil
}

func (c *MemoryCache) Del(key string) (bool, error) {
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	if _, exists := c.sets[key]; !exists {
		return false, nil
	}

	delete(c.sets, key)
	delete(c.expires, key)
	return true, nil
}

func (c *MemoryCache) Set(key string, value string) error {
	c.setsMu.Lock()
	defer c.setsMu.Unlock()
	c.sets[key] = value
	return nil
}

func (c *MemoryCache) Get(key string) (string, bool) {
	c.setsMu.RLock()
	defer c.setsMu.RUnlock()

	// Expire kontrolü
	if expireTime, hasExpire := c.expires[key]; hasExpire && time.Now().After(expireTime) {
		delete(c.sets, key)
		delete(c.expires, key)
		return "", false
	}

	value, ok := c.sets[key]
	return value, ok
}

func (c *MemoryCache) HSet(hash string, key string, value string) error {
	c.hsetsMu.Lock()
	defer c.hsetsMu.Unlock()

	if _, ok := c.hsets[hash]; !ok {
		c.hsets[hash] = make(map[string]string)
	}
	c.hsets[hash][key] = value
	return nil
}

func (c *MemoryCache) HGet(hash string, key string) (string, bool) {
	c.hsetsMu.RLock()
	defer c.hsetsMu.RUnlock()

	if hashMap, ok := c.hsets[hash]; ok {
		value, exists := hashMap[key]
		return value, exists
	}
	return "", false
}

func (c *MemoryCache) HGetAll(hash string) map[string]string {
	c.hsetsMu.RLock()
	defer c.hsetsMu.RUnlock()

	if hashMap, ok := c.hsets[hash]; ok {
		// Orijinal map'i değiştirmemek için kopya oluşturuyoruz
		result := make(map[string]string, len(hashMap))
		for k, v := range hashMap {
			result[k] = v
		}
		return result
	}
	return make(map[string]string)
}
