package cache

import (
	"sort"
	"sync"
)

func (c *MemoryCache) SAdd(key string, member string) (bool, error) {
	var set sync.Map
	actual, _ := c.sets_.LoadOrStore(key, &set)
	actualSet := actual.(*sync.Map)

	_, loaded := actualSet.LoadOrStore(member, true)
	if !loaded {
		c.incrementKeyVersion(key)
		return true, nil
	}
	return false, nil
}

// Set Operations
func (c *MemoryCache) SMembers(key string) ([]string, error) {
	members := make([]string, 0)
	if setI, ok := c.sets_.Load(key); ok {
		set := setI.(*sync.Map)
		set.Range(func(key, _ interface{}) bool {
			members = append(members, key.(string))
			return true
		})
	}
	sort.Strings(members)
	return members, nil
}

func (c *MemoryCache) SCard(key string) int {
	count := 0
	if setI, ok := c.sets_.Load(key); ok {
		set := setI.(*sync.Map)
		set.Range(func(_, _ interface{}) bool {
			count++
			return true
		})
	}
	return count
}

func (c *MemoryCache) SRem(key string, member string) (bool, error) {
	if setI, ok := c.sets_.Load(key); ok {
		set := setI.(*sync.Map)
		if _, exists := set.LoadAndDelete(member); exists {
			c.incrementKeyVersion(key)

			// Check if set is empty
			empty := true
			set.Range(func(_, _ interface{}) bool {
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
		set := setI.(*sync.Map)
		_, exists := set.Load(member)
		return exists
	}
	return false
}

func (c *MemoryCache) SInter(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	// Get first set
	firstSetI, exists := c.sets_.Load(keys[0])
	if !exists {
		return []string{}
	}

	result := make(map[string]bool)
	firstSet := firstSetI.(*sync.Map)
	firstSet.Range(func(key, _ interface{}) bool {
		result[key.(string)] = true
		return true
	})

	// Intersect with other sets
	for _, key := range keys[1:] {
		setI, exists := c.sets_.Load(key)
		if !exists {
			return []string{}
		}

		set := setI.(*sync.Map)
		toDelete := make([]string, 0)

		for member := range result {
			if _, exists := set.Load(member); !exists {
				toDelete = append(toDelete, member)
			}
		}

		for _, member := range toDelete {
			delete(result, member)
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

	result := make(map[string]bool)
	firstSet := firstSetI.(*sync.Map)
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
	}

	diff := make([]string, 0, len(result))
	for member := range result {
		diff = append(diff, member)
	}
	sort.Strings(diff)
	return diff
}
