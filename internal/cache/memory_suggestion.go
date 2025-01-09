package cache

import (
	"sort"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// FTSugAdd adds a suggestion to the in-memory cache with a given key, string, and score.
// Additional options can be provided as key-value pairs, such as "PAYLOAD" to specify a payload for the suggestion.
//
// Parameters:
//   - key: The key under which the suggestion is stored.
//   - str: The suggestion string.
//   - score: The score associated with the suggestion.
//   - opts: Optional key-value pairs for additional suggestion properties.
//
// Returns:
//   - bool: Always returns true.
//   - error: Always returns nil.
func (c *MemoryCache) FTSugAdd(key, str string, score float64, opts ...string) (bool, error) {
	dictI, _ := c.suggestions.LoadOrStore(key, models.NewSuggestionDict())
	dict := dictI.(*models.SuggestionDict)

	// Create new suggestion
	sug := &models.Suggestion{
		String: str,
		Score:  score,
	}

	// Handle options
	for i := 0; i < len(opts)-1; i += 2 {
		if opts[i] == "PAYLOAD" {
			sug.Payload = opts[i+1]
		}
	}

	// Store suggestion
	dict.Entries[str] = sug
	c.incrementKeyVersion(key)

	return true, nil
}

// FTSugDel removes a suggestion string from the suggestion dictionary associated with the given key.
// It returns true if the suggestion was successfully removed, and false if the suggestion did not exist.
// If the key does not exist in the cache, it returns false with no error.
//
// Parameters:
//   - key: The key associated with the suggestion dictionary.
//   - str: The suggestion string to be removed.
//
// Returns:
//   - bool: True if the suggestion was successfully removed, false otherwise.
//   - error: An error if there was an issue during the operation.
func (c *MemoryCache) FTSugDel(key, str string) (bool, error) {
	dictI, exists := c.suggestions.Load(key)
	if !exists {
		return false, nil
	}

	dict := dictI.(*models.SuggestionDict)
	if _, exists := dict.Entries[str]; exists {
		delete(dict.Entries, str)
		c.incrementKeyVersion(key)
		return true, nil
	}

	return false, nil
}

// FTSugGet retrieves suggestions from the memory cache based on the provided key and prefix.
// It supports both fuzzy and non-fuzzy matching and returns a sorted list of suggestions
// limited by the specified maximum number of results.
//
// Parameters:
//   - key: The key to identify the suggestion dictionary in the cache.
//   - prefix: The prefix string to match suggestions against.
//   - fuzzy: A boolean indicating whether to use fuzzy matching (true) or exact prefix matching (false).
//   - max: The maximum number of suggestions to return. If max is 0 or negative, all matches are returned.
//
// Returns:
//   - A slice of models.Suggestion containing the matching suggestions, sorted by score in descending order.
//   - An error if any issues occur during retrieval.
//
// If the key does not exist in the cache, it returns nil for the suggestions and no error.
func (c *MemoryCache) FTSugGet(key, prefix string, fuzzy bool, max int) ([]models.Suggestion, error) {
	dictI, exists := c.suggestions.Load(key)
	if !exists {
		return nil, nil
	}

	dict := dictI.(*models.SuggestionDict)
	var matches []models.Suggestion

	// Collect matching suggestions
	for _, sug := range dict.Entries {
		if fuzzy {
			// Use minTwo here
			prefixLen := minTwo(len(prefix), len(sug.String))
			if levenshteinDistance(prefix, sug.String[:prefixLen]) <= 2 {
				matches = append(matches, *sug)
			}
		} else {
			if strings.HasPrefix(strings.ToLower(sug.String), strings.ToLower(prefix)) {
				matches = append(matches, *sug)
			}
		}
	}

	// Sort by score
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Score > matches[j].Score
	})

	// Apply limit
	if max > 0 && len(matches) > max {
		matches = matches[:max]
	}

	return matches, nil
}

// FTSugLen returns the length of the suggestion dictionary for the given key.
// If the key does not exist in the cache, it returns 0 and no error.
//
// Parameters:
//   - key: The key for which to retrieve the suggestion dictionary length.
//
// Returns:
//   - int64: The length of the suggestion dictionary.
//   - error: An error if one occurred, otherwise nil.
func (c *MemoryCache) FTSugLen(key string) (int64, error) {
	dictI, exists := c.suggestions.Load(key)
	if !exists {
		return 0, nil
	}

	dict := dictI.(*models.SuggestionDict)
	return int64(len(dict.Entries)), nil
}

// levenshteinDistance calculates the Levenshtein distance between two strings.
// The Levenshtein distance is a measure of the difference between two sequences,
// defined as the minimum number of single-character edits (insertions, deletions, or substitutions)
// required to change one string into the other.
//
// Parameters:
//   - s1: The first string.
//   - s2: The second string.
//
// Returns:
//   - An integer representing the Levenshtein distance between the two strings.
func levenshteinDistance(s1, s2 string) int {
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}

	// Initialize matrix
	matrix := make([][]int, len(s1)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(s2)+1)
	}

	// Initialize first row and column
	for i := 0; i <= len(s1); i++ {
		matrix[i][0] = i
	}
	for j := 0; j <= len(s2); j++ {
		matrix[0][j] = j
	}

	// Fill in the rest of the matrix
	for i := 1; i <= len(s1); i++ {
		for j := 1; j <= len(s2); j++ {
			if s1[i-1] == s2[j-1] {
				matrix[i][j] = matrix[i-1][j-1]
			} else {
				matrix[i][j] = min(
					matrix[i-1][j]+1,   // deletion
					matrix[i][j-1]+1,   // insertion
					matrix[i-1][j-1]+1, // substitution
				)
			}
		}
	}

	return matrix[len(s1)][len(s2)]
}

// minTwo returns the smaller of two integers a and b.
// If a is less than b, it returns a; otherwise, it returns b.
func minTwo(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// min returns the smallest of three integers a, b, and c.
// It compares the integers and returns the minimum value.
//
// Parameters:
//   - a: The first integer to compare.
//   - b: The second integer to compare.
//   - c: The third integer to compare.
//
// Returns:
//
//	The smallest integer among a, b, and c.
func min(a, b, c int) int {
	if a < b && a < c {
		return a
	} else if b < c {
		return b
	}
	return c
}

// defragSuggestions defragments the suggestions stored in the MemoryCache.
// It creates a new sync.Map and copies all entries from the current suggestions map
// to the new map, ensuring that the new map has the correct capacity for each
// SuggestionDict. This helps in optimizing memory usage and improving performance.
func (c *MemoryCache) defragSuggestions() {
	c.suggestions.Range(func(key, valueI interface{}) bool {
		dict := valueI.(*models.SuggestionDict)
		if float64(len(dict.Entries)) > 0 && float64(len(dict.Entries))/float64(approximateCapacityForMap(len(dict.Entries))) < 0.5 {
			// Create a new dictionary with the correct capacity
			newDict := models.NewSuggestionDict()
			newDict.Entries = make(map[string]*models.Suggestion, len(dict.Entries))
			// Copy all entries
			for str, sug := range dict.Entries {
				newDict.Entries[str] = sug
			}
			c.suggestions.Store(key, newDict)
		}
		return true
	})
}

// approximateCapacityForMap provides a rough estimate of a map's underlying capacity.
// This is a heuristic and might not be perfectly accurate.
func approximateCapacityForMap(count int) int {
	// You can adjust this factor based on your observations of map behavior.
	return int(float64(count) * 1.5)
}
