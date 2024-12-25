package cache

import (
	"fmt"
	"strconv"
	"time"
)

func (c *MemoryCache) Set(key string, value string) error {
	c.bloomFilter.Add([]byte(key))
	c.sets.Store(key, value)
	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) Get(key string) (string, bool) {
	if !c.bloomFilter.Contains([]byte(key)) {
		return "", false
	}

	if expireTime, ok := c.expires.Load(key); ok {
		if expTime, ok := expireTime.(time.Time); ok && time.Now().After(expTime) {
			c.sets.Delete(key)
			c.expires.Delete(key)
			return "", false
		}
	}

	if value, ok := c.sets.Load(key); ok {
		return value.(string), true
	}
	return "", false
}

func (c *MemoryCache) Incr(key string) (int, error) {
	for {
		val, exists := c.sets.Load(key)
		if !exists {
			if c.sets.CompareAndSwap(key, nil, "1") {
				return 1, nil
			}
			continue
		}

		num, err := strconv.Atoi(val.(string))
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer")
		}

		num++
		if c.sets.CompareAndSwap(key, val, strconv.Itoa(num)) {
			return num, nil
		}
	}
}

func (c *MemoryCache) Del(key string) (bool, error) {
	deleted := false

	if _, ok := c.sets.LoadAndDelete(key); ok {
		c.expires.Delete(key)
		deleted = true
	}

	if _, ok := c.sets_.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.hsets.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.lists.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.jsonData.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.zsets.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.geoData.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.suggestions.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.cms.LoadAndDelete(key); ok {
		deleted = true
	}

	if deleted {
		c.incrementKeyVersion(key)
	}

	if _, ok := c.cuckooFilters.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.hlls.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.tdigests.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.bfilters.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.topks.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.timeSeries.LoadAndDelete(key); ok {
		deleted = true
	}

	return deleted, nil
}
