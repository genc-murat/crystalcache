package models

import (
	"sort"
	"sync"
)

// TopK structure to track frequent items
type TopK struct {
	mu       sync.RWMutex
	items    map[string]int64
	decay    float64
	k        int
	capacity int
}

// NewTopK creates a new TopK structure
func NewTopK(k, capacity int, decay float64) *TopK {
	return &TopK{
		k:        k,
		capacity: capacity,
		decay:    decay,
		items:    make(map[string]int64),
	}
}

// Add items to the TopK structure with a count of 1
func (tk *TopK) Add(items ...string) []bool {
	results := make([]bool, len(items))
	for i, item := range items {
		results[i] = tk.IncrBy(item, 1)
	}
	return results
}

// IncrBy increases the count of items by specified amounts
func (tk *TopK) IncrBy(item string, increment int64) bool {
	tk.mu.Lock()
	defer tk.mu.Unlock()

	// Decay existing counts
	tk.decay_()

	// If item exists, increment it
	if count, exists := tk.items[item]; exists {
		tk.items[item] = count + increment
		return true
	}

	// If we're at capacity and the item isn't in the lowest k items, don't add it
	if len(tk.items) >= tk.capacity {
		if !tk.shouldAdd_(increment) {
			return false
		}
		// Remove lowest item to make space
		tk.removeLeastFrequent_()
	}

	// Add new item
	tk.items[item] = increment
	return true
}

// Query checks if items are in the top-k list
func (tk *TopK) Query(items ...string) []bool {
	tk.mu.RLock()
	defer tk.mu.RUnlock()

	results := make([]bool, len(items))
	for i, item := range items {
		_, results[i] = tk.items[item]
	}
	return results
}

// Count returns the count of items
func (tk *TopK) Count(items ...string) []int64 {
	tk.mu.RLock()
	defer tk.mu.RUnlock()

	counts := make([]int64, len(items))
	for i, item := range items {
		counts[i] = tk.items[item]
	}
	return counts
}

// List returns the current top-k items with their counts
func (tk *TopK) List() []struct {
	Item  string
	Count int64
} {
	tk.mu.RLock()
	defer tk.mu.RUnlock()

	// Create slice of items
	items := make([]struct {
		Item  string
		Count int64
	}, 0, len(tk.items))

	for item, count := range tk.items {
		items = append(items, struct {
			Item  string
			Count int64
		}{item, count})
	}

	// Sort by count in descending order
	sort.Slice(items, func(i, j int) bool {
		return items[i].Count > items[j].Count
	})

	// Return top k items
	if len(items) > tk.k {
		return items[:tk.k]
	}
	return items
}

// Info returns information about the TopK structure
func (tk *TopK) Info() map[string]interface{} {
	tk.mu.RLock()
	defer tk.mu.RUnlock()

	return map[string]interface{}{
		"k":        tk.k,
		"capacity": tk.capacity,
		"decay":    tk.decay,
		"size":     len(tk.items),
	}
}

// Internal helper methods

// decay_ applies decay factor to all counts
func (tk *TopK) decay_() {
	if tk.decay >= 1.0 {
		return
	}
	for item := range tk.items {
		tk.items[item] = int64(float64(tk.items[item]) * tk.decay)
		if tk.items[item] <= 0 {
			delete(tk.items, item)
		}
	}
}

// shouldAdd_ checks if a new item with given count should be added
func (tk *TopK) shouldAdd_(count int64) bool {
	if len(tk.items) < tk.k {
		return true
	}

	// Find kth largest count
	counts := make([]int64, 0, len(tk.items))
	for _, c := range tk.items {
		counts = append(counts, c)
	}
	sort.Slice(counts, func(i, j int) bool {
		return counts[i] > counts[j]
	})

	kthCount := counts[tk.k-1]
	return count > kthCount
}

// removeLeastFrequent_ removes the item with lowest count
func (tk *TopK) removeLeastFrequent_() {
	var minItem string
	var minCount int64 = 1<<63 - 1

	for item, count := range tk.items {
		if count < minCount {
			minCount = count
			minItem = item
		}
	}

	delete(tk.items, minItem)
}
