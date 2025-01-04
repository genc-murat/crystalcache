package cache

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
)

// SAdd adds a member to the set stored at the given key. If the member is
// added successfully (i.e., it was not already present in the set), it
// increments the version of the key and returns true. If the member was
// already present in the set, it returns false.
//
// Parameters:
//
//	key: The key under which the set is stored.
//	member: The member to add to the set.
//
// Returns:
//
//	bool: True if the member was added successfully, false if the member
//	      was already present in the set.
//	error: An error if there was an issue adding the member to the set.
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

// SMembers retrieves all the members of the set stored at the given key.
// It returns a sorted slice of strings containing the members of the set,
// or an error if the operation fails.
//
// Parameters:
//   - key: The key of the set to retrieve members from.
//
// Returns:
//   - []string: A sorted slice of strings containing the members of the set.
//   - error: An error if the operation fails, or nil if successful.
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

// SCard returns the number of elements in the set stored at the given key.
// If the set does not exist, it returns 0.
//
// Parameters:
//   - key: The key of the set.
//
// Returns:
//   - int: The number of elements in the set.
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

// SRem removes a member from the set stored at the given key.
// If the member was present in the set and removed successfully, it returns true.
// If the member was not present in the set, it returns false.
// If the set becomes empty after removing the member, the set is deleted from the cache.
// It returns an error if any issue occurs during the operation.
//
// Parameters:
//   - key: The key of the set from which the member should be removed.
//   - member: The member to be removed from the set.
//
// Returns:
//   - bool: True if the member was successfully removed, false otherwise.
//   - error: An error if any issue occurs during the operation.
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

// SIsMember checks if a given member exists in the set associated with the specified key.
// It returns true if the member exists in the set, otherwise it returns false.
//
// Parameters:
//   - key: The key associated with the set.
//   - member: The member to check for existence in the set.
//
// Returns:
//   - bool: True if the member exists in the set, false otherwise.
func (c *MemoryCache) SIsMember(key string, member string) bool {
	if setI, ok := c.sets_.Load(key); ok {
		_, exists := setI.(*sync.Map).Load(member)
		return exists
	}
	return false
}

// SInter returns the intersection of multiple sets stored in the MemoryCache.
// The sets are identified by the provided keys. If no keys are provided, an
// empty slice is returned. The function sorts the keys based on the size of
// the sets they reference, then iterates through the sets to find common
// elements. The result is a sorted slice of strings containing the members
// present in all sets.
//
// Parameters:
//
//	keys - A variadic parameter representing the keys of the sets to intersect.
//
// Returns:
//
//	A sorted slice of strings containing the members present in all sets
//	identified by the provided keys. If any set does not exist or if there
//	are no common members, an empty slice is returned.
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

// SDiff returns the members of the set resulting from the difference between the first set
// and all the successive sets specified by the given keys. If no keys are provided, it returns
// an empty slice. If only one key is provided, it returns all members of the set associated
// with that key. The result is sorted in lexicographical order.
//
// Parameters:
// - keys: A variadic list of string keys representing the sets to be compared.
//
// Returns:
// - A sorted slice of strings containing the members of the resulting set difference.
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

// SMemRandomCount retrieves a specified number of random members from a set stored in memory.
// The set is identified by the provided key. The function allows for the option to include
// duplicate members in the result.
//
// Parameters:
//   - key: The key identifying the set in the memory cache.
//   - count: The number of random members to retrieve from the set.
//   - allowDuplicates: A boolean flag indicating whether duplicate members are allowed in the result.
//
// Returns:
//   - A slice of strings containing the random members from the set.
//   - An error if any issues occur during the operation.
//
// If the set does not exist or is empty, an empty slice is returned without an error.
// If the count is greater than the number of members in the set and duplicates are not allowed,
// all members are returned in random order.
func (c *MemoryCache) SMemRandomCount(key string, count int, allowDuplicates bool) ([]string, error) {
	// Get the set from sync.Map
	setI, exists := c.sets_.Load(key)
	if !exists {
		return []string{}, nil
	}

	setMap := setI.(*sync.Map)

	// Collect all members into a slice for random selection
	var members []string
	setMap.Range(func(key, _ interface{}) bool {
		members = append(members, key.(string))
		return true
	})

	if len(members) == 0 {
		return []string{}, nil
	}

	// If count is greater than set size and duplicates are not allowed,
	// return all members in random order
	if count > len(members) && !allowDuplicates {
		count = len(members)
	}

	result := make([]string, 0, count)

	if allowDuplicates {
		// With duplicates: simply select random members count times
		for i := 0; i < count; i++ {
			idx := rand.Intn(len(members))
			result = append(result, members[idx])
		}
	} else {
		// Without duplicates: shuffle and take first count elements
		rand.Shuffle(len(members), func(i, j int) {
			members[i], members[j] = members[j], members[i]
		})
		result = append(result, members[:count]...)
	}

	// Update stats if needed
	if c.stats != nil {
		atomic.AddInt64(&c.stats.cmdCount, 1)
	}

	return result, nil
}

// SDiffStoreDel computes the difference between the first set and all subsequent sets,
// stores the result in the destination set, and deletes the elements from the source sets
// that were used in the difference. If the first set becomes empty after the operation,
// it is removed from the cache.
//
// Parameters:
// - destination: The key for the destination set where the result will be stored.
// - keys: A slice of keys representing the sets to compute the difference from.
//
// Returns:
// - int: The number of elements in the resulting set.
// - error: An error if the number of keys is zero or any other issue occurs during the operation.
func (c *MemoryCache) SDiffStoreDel(destination string, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments")
	}

	// Lock all sets to ensure atomicity
	c.defragMu.Lock()
	defer c.defragMu.Unlock()

	// First, compute the difference
	result := make(map[string]bool)

	// Get the first set
	firstSetI, exists := c.sets_.Load(keys[0])
	if !exists {
		return 0, nil
	}
	firstSet := firstSetI.(*sync.Map)

	// Add all elements from first set to result
	firstSet.Range(func(key, _ interface{}) bool {
		result[key.(string)] = true
		return true
	})

	// Remove elements that exist in other sets
	for _, key := range keys[1:] {
		if setI, exists := c.sets_.Load(key); exists {
			set := setI.(*sync.Map)
			set.Range(func(key, _ interface{}) bool {
				delete(result, key.(string))
				return true
			})
		}
	}

	// Create new set for destination
	destSet := &sync.Map{}

	// Store result in destination
	for member := range result {
		destSet.Store(member, struct{}{})
	}

	c.sets_.Store(destination, destSet)

	// Delete elements from source sets that were used in difference
	for member := range result {
		firstSet.Delete(member)
	}

	// Check if first set is now empty and delete if so
	empty := true
	firstSet.Range(func(_, _ interface{}) bool {
		empty = false
		return false
	})
	if empty {
		c.sets_.Delete(keys[0])
	}

	// Increment key versions
	c.incrementKeyVersion(destination)
	for _, key := range keys {
		c.incrementKeyVersion(key)
	}

	return len(result), nil
}

// SMembersPattern retrieves all members of a set stored at the given key that match the specified pattern.
// If the pattern is "*", all members of the set are returned.
// The members are returned in a sorted order for consistent ordering.
//
// Parameters:
//   - key: The key of the set to retrieve members from.
//   - pattern: The pattern to match members against.
//
// Returns:
//   - A slice of strings containing the matching members.
//   - An error if any issues occur during retrieval.
func (c *MemoryCache) SMembersPattern(key string, pattern string) ([]string, error) {
	// Get the set
	setI, exists := c.sets_.Load(key)
	if !exists {
		return []string{}, nil
	}

	set := setI.(*sync.Map)
	matches := make([]string, 0)

	// If pattern is "*", return all members
	if pattern == "*" {
		set.Range(func(memberI, _ interface{}) bool {
			member := memberI.(string)
			matches = append(matches, member)
			return true
		})
	} else {
		// Iterate through set members and check pattern
		set.Range(func(memberI, _ interface{}) bool {
			member := memberI.(string)
			if c.patternMatcher.MatchCached(pattern, member) {
				matches = append(matches, member)
			}
			return true
		})
	}

	// Sort for consistent ordering
	sort.Strings(matches)
	return matches, nil
}

func (c *MemoryCache) SPopCount(key string, count int) ([]string, error) {
	// Get the set
	setI, exists := c.sets_.Load(key)
	if !exists {
		return []string{}, nil
	}
	set := setI.(*sync.Map)

	// Collect all members
	var members []string
	set.Range(func(key, _ interface{}) bool {
		members = append(members, key.(string))
		return true
	})

	if len(members) == 0 {
		return []string{}, nil
	}

	// If count is larger than set size, adjust it
	if count > len(members) {
		count = len(members)
	}

	// Randomly select members
	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		// Get random index
		idx := rand.Intn(len(members))
		// Add member to result
		result = append(result, members[idx])
		// Remove member from set
		set.Delete(members[idx])
		// Remove from members slice
		members[idx] = members[len(members)-1]
		members = members[:len(members)-1]
	}

	// Check if set is now empty
	empty := true
	set.Range(func(_, _ interface{}) bool {
		empty = false
		return false
	})
	if empty {
		c.sets_.Delete(key)
	}

	c.incrementKeyVersion(key)
	return result, nil
}

func (c *MemoryCache) SDiffMulti(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	// Get first set
	firstSetI, exists := c.sets_.Load(keys[0])
	if !exists {
		return []string{}
	}

	// Create result set
	result := make(map[string]bool)
	firstSet := firstSetI.(*sync.Map)
	firstSet.Range(func(key, _ interface{}) bool {
		result[key.(string)] = true
		return true
	})

	// Remove elements that exist in other sets
	for _, key := range keys[1:] {
		if setI, exists := c.sets_.Load(key); exists {
			set := setI.(*sync.Map)
			set.Range(func(key, _ interface{}) bool {
				delete(result, key.(string))
				return true
			})
		}
	}

	// Convert map to sorted slice
	finalResult := make([]string, 0, len(result))
	for member := range result {
		finalResult = append(finalResult, member)
	}
	sort.Strings(finalResult)

	return finalResult
}

func (c *MemoryCache) SInterMulti(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	// Get first set
	firstSetI, exists := c.sets_.Load(keys[0])
	if !exists {
		return []string{}
	}

	// Create result set with first set's members
	result := make(map[string]bool)
	firstSet := firstSetI.(*sync.Map)
	firstSet.Range(func(key, _ interface{}) bool {
		result[key.(string)] = true
		return true
	})

	// Intersect with each subsequent set
	for _, key := range keys[1:] {
		if setI, exists := c.sets_.Load(key); exists {
			set := setI.(*sync.Map)
			newResult := make(map[string]bool)
			set.Range(func(key, _ interface{}) bool {
				if result[key.(string)] {
					newResult[key.(string)] = true
				}
				return true
			})
			result = newResult
		} else {
			// If any set doesn't exist, result is empty
			return []string{}
		}
	}

	// Convert map to sorted slice
	finalResult := make([]string, 0, len(result))
	for member := range result {
		finalResult = append(finalResult, member)
	}
	sort.Strings(finalResult)

	return finalResult
}

func (c *MemoryCache) SUnionMulti(keys ...string) []string {
	result := make(map[string]bool)

	// Union all sets
	for _, key := range keys {
		if setI, exists := c.sets_.Load(key); exists {
			set := setI.(*sync.Map)
			set.Range(func(key, _ interface{}) bool {
				result[key.(string)] = true
				return true
			})
		}
	}

	// Convert map to sorted slice
	finalResult := make([]string, 0, len(result))
	for member := range result {
		finalResult = append(finalResult, member)
	}
	sort.Strings(finalResult)

	return finalResult
}
