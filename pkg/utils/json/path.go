package json

import (
	"fmt"
	"strconv"
	"strings"
)

// PathUtil provides path parsing and manipulation utilities for JSON operations
type PathUtil struct{}

// NewPathUtil creates a new instance of PathUtil
func NewPathUtil() *PathUtil {
	return &PathUtil{}
}

// ParsePath splits a JSON path into its component parts
// Example: "users.0.name" -> ["users", "0", "name"]
// Example: "users[0].name" -> ["users", "[0]", "name"]
func (p *PathUtil) ParsePath(path string) []string {
	parts := make([]string, 0)
	current := ""
	escaped := false

	for _, c := range path {
		if c == '\\' && !escaped {
			escaped = true
			continue
		}
		if c == '.' && !escaped {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(c)
			escaped = false
		}
	}
	if current != "" {
		parts = append(parts, current)
	}

	return parts
}

// ParseArrayIndex extracts the index from an array accessor
// Example: "[0]" -> 0, true
// Example: "name" -> 0, false
func (p *PathUtil) ParseArrayIndex(part string) (int, bool) {
	if len(part) < 3 || part[0] != '[' || part[len(part)-1] != ']' {
		return 0, false
	}

	indexStr := part[1 : len(part)-1]
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return 0, false
	}

	return index, true
}

// BuildPath constructs a path string from parts
func (p *PathUtil) BuildPath(parts ...string) string {
	return strings.Join(parts, ".")
}

// IsRoot checks if the given path is the root path
func (p *PathUtil) IsRoot(path string) bool {
	return path == "." || path == ""
}

// GetParentPath returns the parent path of the given path
func (p *PathUtil) GetParentPath(path string) string {
	if p.IsRoot(path) {
		return "."
	}

	parts := p.ParsePath(path)
	if len(parts) <= 1 {
		return "."
	}

	return p.BuildPath(parts[:len(parts)-1]...)
}

// GetLastPart returns the last component of the path
func (p *PathUtil) GetLastPart(path string) string {
	parts := p.ParsePath(path)
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

// ValidatePath checks if a path is syntactically valid
func (p *PathUtil) ValidatePath(path string) error {
	if p.IsRoot(path) {
		return nil
	}

	parts := p.ParsePath(path)
	if len(parts) == 0 {
		return fmt.Errorf("empty path")
	}

	for i, part := range parts {
		if part == "" {
			return fmt.Errorf("empty path component at position %d", i)
		}

		if _, isArray := p.ParseArrayIndex(part); isArray {
			if i == 0 {
				return fmt.Errorf("path cannot start with array index")
			}
		}
	}

	return nil
}

// ResolvePath resolves a path against a JSON value and returns the target
func (p *PathUtil) ResolvePath(data interface{}, path string) (interface{}, error) {
	if p.IsRoot(path) {
		return data, nil
	}

	parts := p.ParsePath(path)
	current := data

	for _, part := range parts {
		arrayIndex, isArray := p.ParseArrayIndex(part)

		if isArray {
			// Handle array access
			arr, ok := current.([]interface{})
			if !ok {
				return nil, fmt.Errorf("path element is not an array")
			}
			if arrayIndex >= len(arr) {
				return nil, fmt.Errorf("array index out of range")
			}
			current = arr[arrayIndex]
		} else {
			// Handle object access
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("path element is not an object")
			}
			var exists bool
			current, exists = obj[part]
			if !exists {
				return nil, fmt.Errorf("path does not exist")
			}
		}
	}

	return current, nil
}

// SetValueAtPath sets a value at the specified path in a JSON object
func (p *PathUtil) SetValueAtPath(data map[string]interface{}, path string, value interface{}) error {
	if p.IsRoot(path) {
		return fmt.Errorf("cannot set value at root path")
	}

	parts := p.ParsePath(path)
	current := data

	// Navigate to the parent of the target location
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		arrayIndex, isArray := p.ParseArrayIndex(part)

		if isArray {
			arr, ok := current[parts[i-1]].([]interface{})
			if !ok {
				return fmt.Errorf("path element is not an array")
			}
			if arrayIndex >= len(arr) {
				return fmt.Errorf("array index out of range")
			}
			if nextMap, ok := arr[arrayIndex].(map[string]interface{}); ok {
				current = nextMap
			} else {
				newMap := make(map[string]interface{})
				arr[arrayIndex] = newMap
				current = newMap
			}
		} else {
			next, exists := current[part]
			if !exists {
				next = make(map[string]interface{})
				current[part] = next
			}
			if nextMap, ok := next.(map[string]interface{}); ok {
				current = nextMap
			} else {
				return fmt.Errorf("path element is not an object")
			}
		}
	}

	// Set the value at the final location
	lastPart := parts[len(parts)-1]
	arrayIndex, isArray := p.ParseArrayIndex(lastPart)

	if isArray {
		arr, ok := current[parts[len(parts)-2]].([]interface{})
		if !ok {
			return fmt.Errorf("path element is not an array")
		}
		if arrayIndex >= len(arr) {
			newArr := make([]interface{}, arrayIndex+1)
			copy(newArr, arr)
			arr = newArr
			current[parts[len(parts)-2]] = arr
		}
		arr[arrayIndex] = value
	} else {
		current[lastPart] = value
	}

	return nil
}
