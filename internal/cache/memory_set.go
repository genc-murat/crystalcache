package cache

import (
	"sort"
	"sync"
)

func (c *MemoryCache) SAdd(key string, member string) (bool, error) {
	setI, _ := c.sets_.LoadOrStore(key, &sync.Map{})
	actualSet := setI.(*sync.Map)

	_, loaded := actualSet.LoadOrStore(member, true)
	if !loaded {
		c.incrementKeyVersion(key)
		return true, nil
	}
	return false, nil
}

func (c *MemoryCache) SMembers(key string) ([]string, error) {
	var members []string
	if setI, ok := c.sets_.Load(key); ok {
		set := setI.(*sync.Map)
		size := 0
		set.Range(func(_, _ interface{}) bool {
			size++
			return true
		})
		members = make([]string, 0, size)
		set.Range(func(key, _ interface{}) bool {
			members = append(members, key.(string))
			return true
		})
	}
	sort.Strings(members)
	return members, nil
}

func (c *MemoryCache) SCard(key string) int {
	if setI, ok := c.sets_.Load(key); ok {
		count := 0
		setI.(*sync.Map).Range(func(_, _ interface{}) bool {
			count++
			return true
		})
		return count
	}
	return 0
}

func (c *MemoryCache) SRem(key string, member string) (bool, error) {
	if setI, ok := c.sets_.Load(key); ok {
		if _, exists := setI.(*sync.Map).LoadAndDelete(member); exists {
			c.incrementKeyVersion(key)
			empty := true
			setI.(*sync.Map).Range(func(_, _ interface{}) bool {
				empty = false
				return false
			})
			if empty {
				c.sets_.Delete(key)
			}
			return true, nil
		}
	}
	return false, nil
}

func (c *MemoryCache) SIsMember(key string, member string) bool {
	if setI, ok := c.sets_.Load(key); ok {
		_, exists := setI.(*sync.Map).Load(member)
		return exists
	}
	return false
}

func (c *MemoryCache) SInter(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Slice(sortedKeys, func(i, j int) bool {
		setI, exists := c.sets_.Load(sortedKeys[i])
		sizeI := 0
		if exists {
			set := setI.(*sync.Map)
			set.Range(func(_, _ interface{}) bool {
				sizeI++
				return true
			})
		}

		setJ, exists := c.sets_.Load(sortedKeys[j])
		sizeJ := 0
		if exists {
			set := setJ.(*sync.Map)
			set.Range(func(_, _ interface{}) bool {
				sizeJ++
				return true
			})
		}
		return sizeI < sizeJ
	})

	firstSetI, exists := c.sets_.Load(sortedKeys[0])
	if !exists {
		return []string{}
	}

	result := make(map[string]bool)
	firstSet := firstSetI.(*sync.Map)
	firstSet.Range(func(key, _ interface{}) bool {
		result[key.(string)] = true
		return true
	})

	for _, key := range sortedKeys[1:] {
		setI, exists := c.sets_.Load(key)
		if !exists {
			return []string{}
		}

		set := setI.(*sync.Map)
		for member := range result {
			if _, exists := set.Load(member); !exists {
				delete(result, member)
			}
		}

		if len(result) == 0 {
			return []string{}
		}
	}

	intersection := make([]string, 0, len(result))
	for member := range result {
		intersection = append(intersection, member)
	}
	sort.Strings(intersection)
	return intersection
}

func (c *MemoryCache) SUnion(keys ...string) []string {
	result := make(map[string]bool)

	for _, key := range keys {
		if setI, exists := c.sets_.Load(key); exists {
			set := setI.(*sync.Map)
			set.Range(func(key, _ interface{}) bool {
				result[key.(string)] = true
				return true
			})
		}
	}

	union := make([]string, 0, len(result))
	for member := range result {
		union = append(union, member)
	}
	sort.Strings(union)
	return union
}

func (c *MemoryCache) SDiff(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	firstSetI, exists := c.sets_.Load(keys[0])
	if !exists {
		return []string{}
	}
	firstSet := firstSetI.(*sync.Map)

	if len(keys) == 1 {
		diff := make([]string, 0)
		firstSet.Range(func(key, _ interface{}) bool {
			diff = append(diff, key.(string))
			return true
		})
		sort.Strings(diff)
		return diff
	}

	result := make(map[string]bool)
	firstSet.Range(func(key, _ interface{}) bool {
		result[key.(string)] = true
		return true
	})

	for _, key := range keys[1:] {
		setI, exists := c.sets_.Load(key)
		if !exists {
			continue
		}

		set := setI.(*sync.Map)
		set.Range(func(member, _ interface{}) bool {
			delete(result, member.(string))
			return true
		})
		if len(result) == 0 {
			return []string{}
		}
	}

	diff := make([]string, 0, len(result))
	for member := range result {
		diff = append(diff, member)
	}
	sort.Strings(diff)
	return diff
}
