package json

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

// SearchOptions contains configuration for search operations
type SearchOptions struct {
	CaseSensitive bool // Case-sensitive search
	IncludeKeys   bool // Search in keys
	IncludeValues bool // Search in values
}

// DefaultSearchOptions returns default search options
func DefaultSearchOptions() *SearchOptions {
	return &SearchOptions{
		CaseSensitive: false,
		IncludeKeys:   true,
		IncludeValues: true,
	}
}

// SearchResult represents a search match
type SearchResult struct {
	Path  string      // Path where the match was found
	Key   string      // Key that matched
	Value interface{} // Value that matched
	IsKey bool        // Indicates if the match was in a key
}

// SearchUtil provides JSON search functionality
type SearchUtil struct{}

// NewSearchUtil creates a new instance of SearchUtil
func NewSearchUtil() *SearchUtil {
	return &SearchUtil{}
}

func (s *SearchUtil) Search(jsonData, keyword string, opts *SearchOptions) []SearchResult {
	var results []SearchResult
	if keyword == "" || opts == nil {
		return results
	}

	root := gjson.Parse(jsonData)
	searchKeyword := keyword
	if !opts.CaseSensitive {
		searchKeyword = strings.ToLower(keyword)
	}

	if root.IsObject() {
		root.ForEach(func(key, value gjson.Result) bool {
			handleObject(key, value, "", searchKeyword, opts, &results)
			return true
		})
	}

	return results
}

func handleObject(key, value gjson.Result, parentPath string, keyword string, opts *SearchOptions, results *[]SearchResult) {
	// Build current path
	currentPath := key.String()
	if parentPath != "" {
		currentPath = parentPath + "." + currentPath
	}

	// Check if the key itself contains the keyword
	if opts.IncludeKeys {
		keyStr := key.String()
		compareKey := keyStr
		if !opts.CaseSensitive {
			compareKey = strings.ToLower(compareKey)
		}
		if strings.Contains(compareKey, keyword) {
			*results = append(*results, SearchResult{
				Path:  currentPath,
				Key:   keyStr,
				Value: value.Value(),
				IsKey: true,
			})
		}
	}

	// Check if string value contains keyword
	if value.Type == gjson.String {
		valueStr := value.String()
		compareValue := valueStr
		if !opts.CaseSensitive {
			compareValue = strings.ToLower(valueStr)
		}

		if strings.Contains(compareValue, keyword) {
			// If value matches and we're including keys, add key match first
			if opts.IncludeKeys && !opts.IncludeValues {
				*results = append(*results, SearchResult{
					Path:  currentPath,
					Key:   key.String(),
					Value: value.Value(),
					IsKey: true,
				})
			} else if opts.IncludeValues {
				// For combined search, add both key and value matches
				if opts.IncludeKeys {
					*results = append(*results, SearchResult{
						Path:  currentPath,
						Key:   key.String(),
						Value: value.Value(),
						IsKey: true,
					})
				}
				*results = append(*results, SearchResult{
					Path:  currentPath,
					Key:   "",
					Value: valueStr,
					IsKey: false,
				})
			}
		}
	}

	// Recurse into nested structures
	if value.IsObject() {
		value.ForEach(func(k, v gjson.Result) bool {
			handleObject(k, v, currentPath, keyword, opts, results)
			return true
		})
	} else if value.IsArray() {
		for i, item := range value.Array() {
			arrayPath := fmt.Sprintf("%s[%d]", currentPath, i)

			// For array elements, we only add value matches
			if opts.IncludeValues && item.Type == gjson.String {
				valueStr := item.String()
				compareValue := valueStr
				if !opts.CaseSensitive {
					compareValue = strings.ToLower(valueStr)
				}
				if strings.Contains(compareValue, keyword) {
					*results = append(*results, SearchResult{
						Path:  arrayPath,
						Key:   "",
						Value: valueStr,
						IsKey: false,
					})
				}
			}

			// Recurse into array objects
			if item.IsObject() {
				item.ForEach(func(k, v gjson.Result) bool {
					handleObject(k, v, arrayPath, keyword, opts, results)
					return true
				})
			}
		}
	}
}

func handleNode(node gjson.Result, parentPath string, keyword string, opts *SearchOptions, results *[]SearchResult) {
	if node.IsObject() {
		node.ForEach(func(key, value gjson.Result) bool {
			keyStr := key.String()
			currentPath := keyStr
			if parentPath != "" {
				currentPath = parentPath + "." + keyStr
			}

			// Check for key match FIRST
			if opts.IncludeKeys {
				compareKey := keyStr
				if !opts.CaseSensitive {
					compareKey = strings.ToLower(compareKey)
				}
				if strings.Contains(compareKey, keyword) {
					*results = append(*results, SearchResult{
						Path:  currentPath,
						Key:   keyStr,
						Value: value.Value(),
						IsKey: true,
					})
				}
			}

			// Then check for value match
			if opts.IncludeValues && value.Type == gjson.String {
				strValue := value.String()
				compareValue := strValue
				if !opts.CaseSensitive {
					compareValue = strings.ToLower(compareValue)
				}
				if strings.Contains(compareValue, keyword) {
					*results = append(*results, SearchResult{
						Path:  currentPath,
						Key:   "", // Empty for value matches
						Value: strValue,
						IsKey: false,
					})
				}
			}

			// Finally, recurse into the value if needed
			if value.IsObject() || value.IsArray() {
				handleNode(value, currentPath, keyword, opts, results)
			}
			return true
		})
	} else if node.IsArray() {
		node.ForEach(func(index, value gjson.Result) bool {
			currentPath := fmt.Sprintf("%s[%d]", parentPath, int(index.Int()))

			// Arrays only have value matches
			if opts.IncludeValues && value.Type == gjson.String {
				strValue := value.String()
				compareValue := strValue
				if !opts.CaseSensitive {
					compareValue = strings.ToLower(compareValue)
				}
				if strings.Contains(compareValue, keyword) {
					*results = append(*results, SearchResult{
						Path:  currentPath,
						Key:   "", // Always empty for array values
						Value: strValue,
						IsKey: false,
					})
				}
			}

			// Recurse if needed
			if value.IsObject() || value.IsArray() {
				handleNode(value, currentPath, keyword, opts, results)
			}
			return true
		})
	}
}

func searchNode(node gjson.Result, keyword string, parentPath string, opts *SearchOptions, results *[]SearchResult) {
	if node.IsObject() {
		node.ForEach(func(key, value gjson.Result) bool {
			currentPath := key.String()
			if parentPath != "" {
				currentPath = parentPath + "." + currentPath
			}

			// Check key match
			if opts.IncludeKeys {
				keyStr := key.String()
				compareKey := keyStr
				if !opts.CaseSensitive {
					compareKey = strings.ToLower(keyStr)
				}
				if strings.Contains(compareKey, keyword) {
					*results = append(*results, SearchResult{
						Path:  currentPath,
						Key:   keyStr,
						Value: value.Value(),
						IsKey: true,
					})
				}
			}

			// Check value match
			if opts.IncludeValues && value.Type == gjson.String {
				strValue := value.String()
				compareValue := strValue
				if !opts.CaseSensitive {
					compareValue = strings.ToLower(strValue)
				}
				if strings.Contains(compareValue, keyword) {
					*results = append(*results, SearchResult{
						Path:  currentPath,
						Key:   "",
						Value: strValue,
						IsKey: false,
					})
				}
			}

			// Recurse
			if value.IsObject() || value.IsArray() {
				searchNode(value, keyword, currentPath, opts, results)
			}

			return true
		})
	} else if node.IsArray() {
		for i, item := range node.Array() {
			arrayPath := fmt.Sprintf("%s[%d]", parentPath, i)

			// Check value match for array items
			if opts.IncludeValues && item.Type == gjson.String {
				strValue := item.String()
				compareValue := strValue
				if !opts.CaseSensitive {
					compareValue = strings.ToLower(strValue)
				}
				if strings.Contains(compareValue, keyword) {
					*results = append(*results, SearchResult{
						Path:  arrayPath,
						Key:   "",
						Value: strValue,
						IsKey: false,
					})
				}
			}

			// Recurse for nested structures
			if item.IsObject() || item.IsArray() {
				searchNode(item, keyword, arrayPath, opts, results)
			}
		}
	}
}

func searchObject(node gjson.Result, parentPath string, keyword string, opts *SearchOptions, results *[]SearchResult) {
	node.ForEach(func(key, value gjson.Result) bool {
		currentPath := key.String()
		if parentPath != "" {
			currentPath = parentPath + "." + currentPath
		}

		// Key matches
		keyStr := key.String()
		compareKey := keyStr
		if !opts.CaseSensitive {
			compareKey = strings.ToLower(keyStr)
		}

		if opts.IncludeKeys && strings.Contains(compareKey, keyword) {
			*results = append(*results, SearchResult{
				Path:  currentPath,
				Key:   keyStr,
				Value: value.Value(),
				IsKey: true,
			})
		}

		// Value matches
		if value.Type == gjson.String {
			valueStr := value.String()
			compareValue := valueStr
			if !opts.CaseSensitive {
				compareValue = strings.ToLower(valueStr)
			}
			if opts.IncludeValues && strings.Contains(compareValue, keyword) {
				*results = append(*results, SearchResult{
					Path:  currentPath,
					Key:   "",
					Value: valueStr,
					IsKey: false,
				})
			}
		}

		// Recursion
		if value.IsObject() {
			searchObject(value, currentPath, keyword, opts, results)
		} else if value.IsArray() {
			searchArray(value, currentPath, keyword, opts, results)
		}

		return true
	})
}

func searchArray(node gjson.Result, parentPath string, keyword string, opts *SearchOptions, results *[]SearchResult) {
	for i, item := range node.Array() {
		currentPath := fmt.Sprintf("%s[%d]", parentPath, i)

		if item.Type == gjson.String {
			valueStr := item.String()
			compareValue := valueStr
			if !opts.CaseSensitive {
				compareValue = strings.ToLower(valueStr)
			}
			if opts.IncludeValues && strings.Contains(compareValue, keyword) {
				*results = append(*results, SearchResult{
					Path:  currentPath,
					Key:   "",
					Value: valueStr,
					IsKey: false,
				})
			}
		}

		if item.IsObject() {
			searchObject(item, currentPath, keyword, opts, results)
		} else if item.IsArray() {
			searchArray(item, currentPath, keyword, opts, results)
		}
	}
}

func findKeyMatches(node gjson.Result, parentPath string, keyword string, opts *SearchOptions) []SearchResult {
	var results []SearchResult

	if !node.IsObject() {
		return results
	}

	node.ForEach(func(key, value gjson.Result) bool {
		keyStr := key.String()
		currentPath := keyStr
		if parentPath != "" {
			currentPath = parentPath + "." + keyStr
		}

		// Check key match
		compareKey := keyStr
		if !opts.CaseSensitive {
			compareKey = strings.ToLower(keyStr)
			keyword = strings.ToLower(keyword)
		}
		if strings.Contains(compareKey, keyword) {
			results = append(results, SearchResult{
				Path:  currentPath,
				Key:   keyStr,
				Value: value.Value(),
				IsKey: true,
			})
		}

		// Recurse into nested objects
		results = append(results, findKeyMatches(value, currentPath, keyword, opts)...)
		return true
	})

	return results
}

func findValueMatches(node gjson.Result, parentPath string, keyword string, opts *SearchOptions) []SearchResult {
	var results []SearchResult

	if node.IsObject() {
		node.ForEach(func(key, value gjson.Result) bool {
			currentPath := key.String()
			if parentPath != "" {
				currentPath = parentPath + "." + currentPath
			}

			// Check string value
			if value.Type == gjson.String {
				valueStr := value.String()
				compareValue := valueStr
				if !opts.CaseSensitive {
					compareValue = strings.ToLower(valueStr)
					keyword = strings.ToLower(keyword)
				}
				if strings.Contains(compareValue, keyword) {
					results = append(results, SearchResult{
						Path:  currentPath,
						Key:   "",
						Value: valueStr,
						IsKey: false,
					})
				}
			}

			// Recurse
			results = append(results, findValueMatches(value, currentPath, keyword, opts)...)
			return true
		})
	} else if node.IsArray() {
		for i, item := range node.Array() {
			currentPath := fmt.Sprintf("%s[%d]", parentPath, i)

			if item.Type == gjson.String {
				valueStr := item.String()
				compareValue := valueStr
				if !opts.CaseSensitive {
					compareValue = strings.ToLower(valueStr)
					keyword = strings.ToLower(keyword)
				}
				if strings.Contains(compareValue, keyword) {
					results = append(results, SearchResult{
						Path:  currentPath,
						Key:   "",
						Value: valueStr,
						IsKey: false,
					})
				}
			}

			results = append(results, findValueMatches(item, currentPath, keyword, opts)...)
		}
	}

	return results
}

func searchRecursive(node gjson.Result, parentPath string, searchKeyword string, opts *SearchOptions, results *[]SearchResult) {
	if node.IsObject() {
		node.ForEach(func(key, value gjson.Result) bool {
			currentPath := key.String()
			if parentPath != "" {
				currentPath = parentPath + "." + currentPath
			}

			// Key matching
			if opts.IncludeKeys {
				keyStr := key.String()
				compareKey := keyStr
				if !opts.CaseSensitive {
					compareKey = strings.ToLower(keyStr)
				}
				if strings.Contains(compareKey, searchKeyword) {
					*results = append(*results, SearchResult{
						Path:  currentPath,
						Key:   keyStr,
						Value: value.Value(),
						IsKey: true,
					})
				}
			}

			// Value matching for strings
			if opts.IncludeValues && value.Type == gjson.String {
				valueStr := value.String()
				compareValue := valueStr
				if !opts.CaseSensitive {
					compareValue = strings.ToLower(valueStr)
				}
				if strings.Contains(compareValue, searchKeyword) {
					*results = append(*results, SearchResult{
						Path:  currentPath,
						Key:   "",
						Value: valueStr,
						IsKey: false,
					})
				}
			}

			// Recurse into the value
			searchRecursive(value, currentPath, searchKeyword, opts, results)
			return true
		})
	} else if node.IsArray() {
		for i, item := range node.Array() {
			arrayPath := fmt.Sprintf("%s[%d]", parentPath, i)

			// Value matching for string array elements
			if opts.IncludeValues && item.Type == gjson.String {
				valueStr := item.String()
				compareValue := valueStr
				if !opts.CaseSensitive {
					compareValue = strings.ToLower(valueStr)
				}
				if strings.Contains(compareValue, searchKeyword) {
					*results = append(*results, SearchResult{
						Path:  arrayPath,
						Key:   "",
						Value: valueStr,
						IsKey: false,
					})
				}
			}

			searchRecursive(item, arrayPath, searchKeyword, opts, results)
		}
	}
}

func processNode(parentPath string, key, value gjson.Result, keyword string, opts *SearchOptions, results *[]SearchResult) {
	// Build current path
	currentPath := key.String()
	if parentPath != "" {
		currentPath = parentPath + "." + currentPath
	}

	// Check for key match
	if opts.IncludeKeys {
		keyStr := key.String()
		compareKey := keyStr
		if !opts.CaseSensitive {
			compareKey = strings.ToLower(keyStr)
		}
		if strings.Contains(compareKey, keyword) {
			*results = append(*results, SearchResult{
				Path:  currentPath,
				Key:   keyStr,
				Value: value.Value(),
				IsKey: true,
			})
		}
	}

	// Check for string value match
	if opts.IncludeValues && value.Type == gjson.String {
		valueStr := value.String()
		compareValue := valueStr
		if !opts.CaseSensitive {
			compareValue = strings.ToLower(valueStr)
		}
		if strings.Contains(compareValue, keyword) {
			*results = append(*results, SearchResult{
				Path:  currentPath,
				Key:   "",
				Value: valueStr,
				IsKey: false,
			})
		}
	}

	// Recurse into nested structures
	if value.IsObject() {
		value.ForEach(func(k, v gjson.Result) bool {
			processNode(currentPath, k, v, keyword, opts, results)
			return true
		})
	} else if value.IsArray() {
		for i, item := range value.Array() {
			arrayPath := fmt.Sprintf("%s[%d]", currentPath, i)
			// For array items, we pass an empty key since arrays don't have keys
			processNode(arrayPath, gjson.Result{}, gjson.Parse(item.Raw), keyword, opts, results)
		}
	}
}

func matchesString(value, keyword string, caseSensitive bool) bool {
	if !caseSensitive {
		value = strings.ToLower(value)
		keyword = strings.ToLower(keyword)
	}
	return strings.Contains(value, keyword)
}

// Matches checks if a value matches the keyword
func matches(value, keyword string, caseSensitive bool) bool {
	if !caseSensitive {
		value = strings.ToLower(value)
	}
	return strings.Contains(value, keyword)
}
