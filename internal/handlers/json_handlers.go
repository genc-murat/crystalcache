package handlers

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type JSONHandlers struct {
	cache ports.Cache
}

func NewJSONHandlers(cache ports.Cache) *JSONHandlers {
	return &JSONHandlers{cache: cache}
}

func (h *JSONHandlers) HandleJSON(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.SET command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	jsonStr := args[2].Bulk

	var value interface{}
	if err := json.Unmarshal([]byte(jsonStr), &value); err != nil {
		return models.Value{Type: "error", Str: "ERR invalid JSON string"}
	}

	// Handle root path
	if path == "." {
		if err := h.cache.SetJSON(key, value); err != nil {
			return util.ToValue(err)
		}
		return models.Value{Type: "string", Str: "OK"}
	}

	// Get existing JSON if exists
	existingValue, exists := h.cache.GetJSON(key)
	var data map[string]interface{}

	if exists {
		if existingMap, ok := existingValue.(map[string]interface{}); ok {
			data = existingMap
		} else {
			data = make(map[string]interface{})
		}
	} else {
		data = make(map[string]interface{})
	}

	// Set value at path
	if err := h.setNestedValue(data, path, value); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	// Store updated data
	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *JSONHandlers) setNestedValue(data map[string]interface{}, path string, value interface{}) error {
	parts := parsePath(path)
	current := data

	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		arrayIndex, isArray := parseArrayIndex(part)

		if isArray {
			arr, ok := current[parts[i-1]].([]interface{})
			if !ok {
				return fmt.Errorf("ERR path element is not an array")
			}
			if arrayIndex >= len(arr) {
				return fmt.Errorf("ERR array index out of range")
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
				return fmt.Errorf("ERR path element is not an object")
			}
		}
	}

	lastPart := parts[len(parts)-1]
	arrayIndex, isArray := parseArrayIndex(lastPart)

	if isArray {
		arr, ok := current[parts[len(parts)-2]].([]interface{})
		if !ok {
			return fmt.Errorf("ERR path element is not an array")
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

func parsePath(path string) []string {
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

func parseArrayIndex(part string) (int, bool) {
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

func (h *JSONHandlers) HandleJSONGet(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.GET command"}
	}

	key := args[0].Bulk
	path := "."
	if len(args) > 1 {
		path = args[1].Bulk
	}

	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "null"}
	}

	if path == "." {
		result, err := json.Marshal(value)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR failed to encode JSON"}
		}
		return models.Value{Type: "bulk", Bulk: string(result)}
	}

	result, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode JSON"}
	}

	return models.Value{Type: "bulk", Bulk: string(resultJSON)}
}

func (h *JSONHandlers) HandleJSONDel(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.DEL command"}
	}

	key := args[0].Bulk
	path := "."
	if len(args) > 1 {
		path = args[1].Bulk
	}

	// If key doesn't exist, return 0
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "integer", Num: 0}
	}

	// If root path, delete entire key
	if path == "." {
		deleted := h.cache.DeleteJSON(key)
		if deleted {
			return models.Value{Type: "integer", Num: 1}
		}
		return models.Value{Type: "integer", Num: 0}
	}

	// Handle nested deletion
	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR key contains non-object JSON"}
	}

	// Delete at path
	ok, delErr := h.deleteNestedValue(data, path)
	if delErr != nil {
		return models.Value{Type: "error", Str: delErr.Error()}
	}
	if !ok {
		return models.Value{Type: "integer", Num: 0}
	}

	// Store updated data
	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}

// Helper method to delete values at nested paths (keep existing implementation)
func (h *JSONHandlers) deleteNestedValue(data map[string]interface{}, path string) (bool, error) {
	parts := parsePath(path)
	if len(parts) == 0 {
		return false, fmt.Errorf("ERR invalid path")
	}

	// If we're deleting a top-level key
	if len(parts) == 1 {
		if _, exists := data[parts[0]]; !exists {
			return false, nil
		}
		delete(data, parts[0])
		return true, nil
	}

	// Navigate to the parent of the target to delete
	current := data
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		arrayIndex, isArray := parseArrayIndex(part)

		if isArray {
			// Handle array access
			arr, ok := current[parts[i-1]].([]interface{})
			if !ok {
				return false, fmt.Errorf("ERR path element is not an array")
			}
			if arrayIndex >= len(arr) {
				return false, fmt.Errorf("ERR array index out of range")
			}

			// Get the next object in the path
			nextObj, ok := arr[arrayIndex].(map[string]interface{})
			if !ok {
				return false, fmt.Errorf("ERR path element is not an object")
			}
			current = nextObj
		} else {
			// Handle object access
			next, exists := current[part]
			if !exists {
				return false, nil
			}
			nextObj, ok := next.(map[string]interface{})
			if !ok {
				return false, fmt.Errorf("ERR path element is not an object")
			}
			current = nextObj
		}
	}

	// Delete the target
	lastPart := parts[len(parts)-1]
	arrayIndex, isArray := parseArrayIndex(lastPart)

	if isArray {
		// Handle array element deletion
		parentKey := parts[len(parts)-2]
		arr, ok := current[parentKey].([]interface{})
		if !ok {
			return false, fmt.Errorf("ERR path element is not an array")
		}
		if arrayIndex >= len(arr) {
			return false, fmt.Errorf("ERR array index out of range")
		}

		// Remove the element at the specified index
		newArr := append(arr[:arrayIndex], arr[arrayIndex+1:]...)
		current[parentKey] = newArr
		return true, nil
	} else {
		// Handle object property deletion
		if _, exists := current[lastPart]; !exists {
			return false, nil
		}
		delete(current, lastPart)
		return true, nil
	}
}

func (h *JSONHandlers) HandleJSONType(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.TYPE command"}
	}

	key := args[0].Bulk
	path := "."
	if len(args) > 1 {
		path = args[1].Bulk
	}

	// Get value from cache using native JSON storage
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "null"}
	}

	// Get value at path
	var target interface{}
	if path == "." {
		target = value
	} else {
		var pathErr error
		target, pathErr = h.getNestedValue(value, path)
		if pathErr != nil {
			return models.Value{Type: "error", Str: pathErr.Error()}
		}
	}

	// Determine type
	jsonType := "none"
	switch v := target.(type) {
	case nil:
		jsonType = "null"
	case bool:
		jsonType = "boolean"
	case float64:
		jsonType = "number"
	case int:
		jsonType = "number"
	case int64:
		jsonType = "number"
	case string:
		jsonType = "string"
	case []interface{}:
		jsonType = "array"
	case map[string]interface{}:
		jsonType = "object"
	default:
		return models.Value{Type: "error", Str: fmt.Sprintf("ERR unknown JSON type: %T", v)}
	}

	return models.Value{Type: "bulk", Bulk: jsonType}
}

// Keep the existing getNestedValue helper as it works with interface{} types
func (h *JSONHandlers) getNestedValue(data interface{}, path string) (interface{}, error) {
	parts := parsePath(path)
	current := data

	for _, part := range parts {
		arrayIndex, isArray := parseArrayIndex(part)

		if isArray {
			// Handle array access
			arr, ok := current.([]interface{})
			if !ok {
				return nil, fmt.Errorf("ERR path element is not an array")
			}
			if arrayIndex >= len(arr) {
				return nil, fmt.Errorf("ERR array index out of range")
			}
			current = arr[arrayIndex]
		} else {
			// Handle object access
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("ERR path element is not an object")
			}
			var exists bool
			current, exists = obj[part]
			if !exists {
				return nil, fmt.Errorf("ERR path does not exist")
			}
		}
	}

	return current, nil
}
