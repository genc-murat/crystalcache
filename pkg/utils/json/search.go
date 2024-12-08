package json

import (
	"fmt"
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

// Search performs a search in JSON data and returns matching paths
func (s *SearchUtil) Search(value interface{}, keyword string, opts *SearchOptions) []string {
	if opts == nil {
		opts = DefaultSearchOptions()
	}
	paths := make([]string, 0)
	s.SearchWithOptions(value, keyword, opts, "", &paths)
	return paths
}

// SearchWithOptions performs a search with custom options
func (s *SearchUtil) SearchWithOptions(value interface{}, keyword string, opts *SearchOptions, currentPath string, paths *[]string) {
	// Check depth limit
	if opts.MaxDepth == 0 {
		return
	}

	switch v := value.(type) {
	case map[string]interface{}:
		s.searchInObject(v, keyword, opts, currentPath, paths)
	case []interface{}:
		s.searchInArray(v, keyword, opts, currentPath, paths)
	case string:
		if opts.IncludeValues && s.isMatch(v, keyword, opts.CaseSensitive) {
			if currentPath != "" {
				*paths = append(*paths, currentPath)
			}
		}
	}
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

// SearchDetailed performs a detailed search and returns SearchResult objects
func (s *SearchUtil) SearchDetailed(value interface{}, keyword string, opts *SearchOptions) []SearchResult {
	results := make([]SearchResult, 0)
	s.searchDetailed(value, keyword, opts, "", &results)
	return results
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
	case string:
		if opts.IncludeValues && s.isMatch(v, keyword, opts.CaseSensitive) {
			*results = append(*results, SearchResult{
				Path:       currentPath,
				Value:      v,
				IsKeyMatch: false,
			})
		}
	}
}

// searchDetailedInObject performs detailed search in objects
func (s *SearchUtil) searchDetailedInObject(obj map[string]interface{}, keyword string, opts *SearchOptions, currentPath string, results *[]SearchResult) {
	newOpts := *opts
	if newOpts.MaxDepth > 0 {
		newOpts.MaxDepth--
	}

	for key, val := range obj {
		newPath := s.buildPath(currentPath, key)

		if opts.IncludeKeys && s.isMatch(key, keyword, opts.CaseSensitive) {
			*results = append(*results, SearchResult{
				Path:       newPath,
				Key:        key,
				Value:      val,
				IsKeyMatch: true,
			})
		}

		s.searchDetailed(val, keyword, &newOpts, newPath, results)
	}
}

// searchDetailedInArray performs detailed search in arrays
func (s *SearchUtil) searchDetailedInArray(arr []interface{}, keyword string, opts *SearchOptions, currentPath string, results *[]SearchResult) {
	newOpts := *opts
	if newOpts.MaxDepth > 0 {
		newOpts.MaxDepth--
	}

	for i, val := range arr {
		newPath := fmt.Sprintf("%s[%d]", currentPath, i)
		s.searchDetailed(val, keyword, &newOpts, newPath, results)
	}
}

// isMatch checks if a string matches the search keyword
func (s *SearchUtil) isMatch(value, keyword string, caseSensitive bool) bool {
	if !caseSensitive {
		value = strings.ToLower(value)
		keyword = strings.ToLower(keyword)
	}
	return strings.Contains(value, keyword)
}

// buildPath creates a path string from current path and key
func (s *SearchUtil) buildPath(currentPath, key string) string {
	if currentPath == "" {
		return key
	}
	return currentPath + "." + key
}
