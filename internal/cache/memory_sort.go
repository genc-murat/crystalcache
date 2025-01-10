package cache

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func (c *MemoryCache) Sort(key string, desc bool, alpha bool, limit bool, start int, count int, store string) ([]string, error) {
	// Get the values to sort based on key type
	var values []string

	// Check what type of key we're dealing with
	switch c.Type(key) {
	case "list":
		if listI, exists := c.lists.Load(key); exists {
			list := listI.(*[]string)
			values = make([]string, len(*list))
			copy(values, *list)
		}
	case "set":
		if setI, exists := c.sets_.Load(key); exists {
			set := setI.(*sync.Map)
			values = make([]string, 0)
			set.Range(func(k, _ interface{}) bool {
				values = append(values, k.(string))
				return true
			})
		}
	case "zset":
		if zsetI, exists := c.zsets.Load(key); exists {
			zset := zsetI.(*sync.Map)
			values = make([]string, 0)
			zset.Range(func(k, _ interface{}) bool {
				values = append(values, k.(string))
				return true
			})
		}
	default:
		return nil, fmt.Errorf("ERR not supported type for SORT")
	}

	// Sort the values
	if alpha {
		if desc {
			sort.Sort(sort.Reverse(sort.StringSlice(values)))
		} else {
			sort.Strings(values)
		}
	} else {
		// Convert to float64 for numeric sorting
		nums := make([]float64, len(values))
		for i, v := range values {
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("ERR One or more values cannot be converted to numeric")
			}
			nums[i] = n
		}

		// Create index slice for sorting
		indices := make([]int, len(nums))
		for i := range indices {
			indices[i] = i
		}

		// Sort indices based on numeric values
		if desc {
			sort.Slice(indices, func(i, j int) bool {
				return nums[indices[i]] > nums[indices[j]]
			})
		} else {
			sort.Slice(indices, func(i, j int) bool {
				return nums[indices[i]] < nums[indices[j]]
			})
		}

		// Reorder values based on sorted indices
		sortedValues := make([]string, len(values))
		for i, idx := range indices {
			sortedValues[i] = values[idx]
		}
		values = sortedValues
	}

	// Apply limit if specified
	if limit {
		if start < 0 {
			start = 0
		}
		if start >= len(values) {
			values = []string{}
		} else {
			end := start + count
			if end > len(values) {
				end = len(values)
			}
			values = values[start:end]
		}
	}

	// Store result if specified
	if store != "" {
		err := c.Set(store, strings.Join(values, "\n"))
		if err != nil {
			return nil, err
		}
		c.incrementKeyVersion(store)
	}

	return values, nil
}
