package cache

import (
	"sort"
	"strings"
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// FTSugAdd implements suggestion addition
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

// FTSugDel implements suggestion deletion
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

// FTSugGet implements suggestion retrieval
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

// FTSugLen implements suggestion dictionary size retrieval
func (c *MemoryCache) FTSugLen(key string) (int64, error) {
	dictI, exists := c.suggestions.Load(key)
	if !exists {
		return 0, nil
	}

	dict := dictI.(*models.SuggestionDict)
	return int64(len(dict.Entries)), nil
}

// Helper functions

// levenshteinDistance calculates the edit distance between two strings
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

func minTwo(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func min(a, b, c int) int {
	if a < b && a < c {
		return a
	} else if b < c {
		return b
	}
	return c
}

func (c *MemoryCache) defragSuggestions() {
	newSuggestions := &sync.Map{}
	c.suggestions.Range(func(key, valueI interface{}) bool {
		dict := valueI.(*models.SuggestionDict)
		// Create a new dictionary with the correct capacity
		newDict := models.NewSuggestionDict()
		newDict.Entries = make(map[string]*models.Suggestion, len(dict.Entries))
		// Copy all entries
		for str, sug := range dict.Entries {
			newDict.Entries[str] = sug
		}
		newSuggestions.Store(key, newDict)
		return true
	})
	c.suggestions = newSuggestions
}
