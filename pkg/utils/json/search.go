package json

import (
	"fmt"
	"sort"
	"strings"
)

// SearchOptions contains configuration for search operations
type SearchOptions struct {
	// CaseSensitive determines if search should be case-sensitive
	CaseSensitive bool
	// IncludeKeys determines if keys should be included in search
	IncludeKeys bool
	// IncludeValues determines if values should be included in search
	IncludeValues bool
	// MaxDepth sets the maximum recursion depth (-1 for unlimited)
	MaxDepth int
}

// DefaultSearchOptions returns default search options
func DefaultSearchOptions() *SearchOptions {
	return &SearchOptions{
		CaseSensitive: false,
		IncludeKeys:   true,
		IncludeValues: true,
		MaxDepth:      -1,
	}
}

// SearchResult represents a search match
type SearchResult struct {
	// Path where the match was found
	Path string
	// Key that matched (if searching keys)
	Key string
	// Value that matched (if searching values)
	Value interface{}
	// IsKeyMatch indicates if the match was in a key
	IsKeyMatch bool
}

// SearchUtil provides JSON search functionality
type SearchUtil struct{}

// NewSearchUtil creates a new instance of SearchUtil
func NewSearchUtil() *SearchUtil {
	return &SearchUtil{}
}

// SearchDetailed performs a detailed search and returns SearchResult objects
func (s *SearchUtil) SearchDetailed(value interface{}, keyword string, opts *SearchOptions) []SearchResult {
	results := make([]SearchResult, 0)
	s.searchDetailed(value, keyword, opts, "", &results)
	return results
}

// searchDetailedInObject performs detailed search in objects
func (s *SearchUtil) searchDetailedInObject(obj map[string]interface{}, keyword string, opts *SearchOptions, currentPath string, results *[]SearchResult) {
	newOpts := *opts
	if newOpts.MaxDepth > 0 {
		newOpts.MaxDepth--
	}

	for key, val := range obj {
		newPath := s.buildPath(currentPath, key)

		// First check key match
		if opts.IncludeKeys && s.isMatch(key, keyword, opts.CaseSensitive) {
			*results = append(*results, SearchResult{
				Path:       newPath,
				Key:        key,
				Value:      val,
				IsKeyMatch: true,
			})
		}

		// Handle string values directly here
		if str, ok := val.(string); ok && opts.IncludeValues && s.isMatch(str, keyword, opts.CaseSensitive) {
			*results = append(*results, SearchResult{
				Path:       newPath,
				Value:      str,
				IsKeyMatch: false,
			})
		} else {
			// Only recurse for non-string values
			switch v := val.(type) {
			case map[string]interface{}, []interface{}:
				s.searchDetailed(v, keyword, &newOpts, newPath, results)
			}
		}
	}
}

// searchDetailed is the internal implementation of detailed search
func (s *SearchUtil) searchDetailed(value interface{}, keyword string, opts *SearchOptions, currentPath string, results *[]SearchResult) {
	if opts.MaxDepth == 0 {
		return
	}

	switch v := value.(type) {
	case map[string]interface{}:
		s.searchDetailedInObject(v, keyword, opts, currentPath, results)
	case []interface{}:
		s.searchDetailedInArray(v, keyword, opts, currentPath, results)
	}
	// Remove string case handling from here as it's now handled in searchDetailedInObject
}

// searchDetailedInArray performs detailed search in arrays
func (s *SearchUtil) searchDetailedInArray(arr []interface{}, keyword string, opts *SearchOptions, currentPath string, results *[]SearchResult) {
	newOpts := *opts
	if newOpts.MaxDepth > 0 {
		newOpts.MaxDepth--
	}

	for i, val := range arr {
		newPath := fmt.Sprintf("%s[%d]", currentPath, i)

		// Handle string values directly here
		if str, ok := val.(string); ok && opts.IncludeValues && s.isMatch(str, keyword, opts.CaseSensitive) {
			*results = append(*results, SearchResult{
				Path:       newPath,
				Value:      str,
				IsKeyMatch: false,
			})
		} else {
			// Only recurse for non-string values
			switch v := val.(type) {
			case map[string]interface{}, []interface{}:
				s.searchDetailed(v, keyword, &newOpts, newPath, results)
			}
		}
	}
}

// calculateDepth correctly accounts for array indices
func (s *SearchUtil) calculateDepth(path string) int {
	if path == "" {
		return 0
	}
	count := strings.Count(path, ".")
	count += strings.Count(path, "[")
	return count
}

func (s *SearchUtil) SearchWithOptions(value interface{}, keyword string, opts *SearchOptions, currentPath string, paths *[]string) {
	if keyword == "" {
		return
	}

	currentDepth := s.calculateDepth(currentPath)
	seen := make(map[string]bool)

	switch v := value.(type) {
	case map[string]interface{}:
		for key, val := range v {
			newPath := s.buildPath(currentPath, key)
			newPathDepth := s.calculateDepth(newPath)

			newOpts := *opts
			if newOpts.MaxDepth > 0 {
				newOpts.MaxDepth--
			}

			keyMatches := opts.IncludeKeys && s.isMatch(key, keyword, opts.CaseSensitive)

			if keyMatches {
				if opts.MaxDepth < 0 || newPathDepth <= opts.MaxDepth {
					if !seen[newPath] {
						*paths = append(*paths, newPath)
						seen[newPath] = true
					}
				}
			}

			// *** FIX: Check for string value match *BEFORE* recursing ***
			if strVal, ok := val.(string); ok && opts.IncludeValues && s.isMatch(strVal, keyword, opts.CaseSensitive) {
				if opts.MaxDepth < 0 || newPathDepth <= opts.MaxDepth {
					if !seen[newPath] {
						*paths = append(*paths, newPath)
						seen[newPath] = true
					}
				}
			} else if !ok { // If not a string, then recurse
				if opts.MaxDepth < 0 || newPathDepth < opts.MaxDepth {
					s.SearchWithOptions(val, keyword, &newOpts, newPath, paths)
				}
			}

		}
	case []interface{}:
		for i, item := range v {
			newPath := fmt.Sprintf("%s[%d]", currentPath, i)

			// ***FIX: Calculate newPathDepth AFTER creating newPath***
			newPathDepth := s.calculateDepth(newPath) // This is the crucial change

			newOpts := *opts
			if newOpts.MaxDepth > 0 {
				newOpts.MaxDepth--
			}

			if opts.MaxDepth < 0 || newPathDepth <= opts.MaxDepth {
				s.SearchWithOptions(item, keyword, &newOpts, newPath, paths)
			}
		}
	case string:
		if opts.IncludeValues && s.isMatch(v, keyword, opts.CaseSensitive) && !seen[currentPath] {
			if opts.MaxDepth < 0 || currentDepth <= opts.MaxDepth { // <= is crucial here
				*paths = append(*paths, currentPath)
				seen[currentPath] = true
			}

		}
	}
}

func (s *SearchUtil) Search(value interface{}, keyword string, opts *SearchOptions) []string {
	if opts == nil {
		opts = DefaultSearchOptions() // This line is fine
	}

	// *** CRUCIAL FIX: Create a copy of the options ***
	searchOpts := *opts // Create a copy!

	paths := make([]string, 0)
	s.SearchWithOptions(value, keyword, &searchOpts, "", &paths) // Pass the copy to SearchWithOptions
	sort.Strings(paths)
	return paths
}

// isMatch checks if a string matches the search keyword
func (s *SearchUtil) isMatch(value, keyword string, caseSensitive bool) bool {
	if keyword == "" || value == "" {
		return false
	}

	if !caseSensitive {
		value = strings.ToLower(value)
		keyword = strings.ToLower(keyword)
	}

	return strings.Contains(value, keyword)
}

// searchInObject searches within a JSON object
func (s *SearchUtil) searchInObject(obj map[string]interface{}, keyword string, opts *SearchOptions, currentPath string, paths *[]string) {
	newOpts := *opts
	if newOpts.MaxDepth > 0 {
		newOpts.MaxDepth--
	}

	for key, val := range obj {
		newPath := s.buildPath(currentPath, key)

		// Check key if enabled
		if opts.IncludeKeys && s.isMatch(key, keyword, opts.CaseSensitive) {
			*paths = append(*paths, newPath)
		}

		// Recursively search in value
		s.SearchWithOptions(val, keyword, &newOpts, newPath, paths)
	}
}

// searchInArray searches within a JSON array
func (s *SearchUtil) searchInArray(arr []interface{}, keyword string, opts *SearchOptions, currentPath string, paths *[]string) {
	newOpts := *opts
	if newOpts.MaxDepth > 0 {
		newOpts.MaxDepth--
	}

	for i, val := range arr {
		newPath := fmt.Sprintf("%s[%d]", currentPath, i)
		s.SearchWithOptions(val, keyword, &newOpts, newPath, paths)
	}
}

// buildPath creates a path string from current path and key
func (s *SearchUtil) buildPath(currentPath, key string) string {
	if currentPath == "" {
		return key
	}
	return currentPath + "." + key
}
