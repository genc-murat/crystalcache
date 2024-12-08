package handlers

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

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

func (h *JSONHandlers) HandleJSONArrAppend(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRAPPEND command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Append all values
	for i := 2; i < len(args); i++ {
		var valueToAppend interface{}
		if err := json.Unmarshal([]byte(args[i].Bulk), &valueToAppend); err != nil {
			return models.Value{Type: "error", Str: "ERR invalid JSON value"}
		}
		arr = append(arr, valueToAppend)
	}

	// Update the array at path
	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	if err := h.setNestedValue(data, path, arr); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: len(arr)}
}

func (h *JSONHandlers) HandleJSONArrLen(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRLEN command"}
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

	var target interface{}
	if path == "." {
		target = value
	} else {
		var err error
		target, err = h.getNestedValue(value, path)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	return models.Value{Type: "integer", Num: len(arr)}
}

func (h *JSONHandlers) HandleJSONStrLen(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.STRLEN command"}
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

	var target interface{}
	if path == "." {
		target = value
	} else {
		var err error
		target, err = h.getNestedValue(value, path)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
	}

	str, ok := target.(string)
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to a string"}
	}

	return models.Value{Type: "integer", Num: len(str)}
}

func (h *JSONHandlers) HandleJSONToggle(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.TOGGLE command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk

	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	boolValue, ok := target.(bool)
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to a boolean"}
	}

	// Toggle the boolean value
	if err := h.setNestedValue(data, path, !boolValue); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: btoi(!boolValue)}
}

// Helper function to convert boolean to int
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (h *JSONHandlers) HandleJSONArrIndex(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRINDEX command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	searchValue := args[2].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "null"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Parse search value
	var searchItem interface{}
	if err := json.Unmarshal([]byte(searchValue), &searchItem); err != nil {
		return models.Value{Type: "error", Str: "ERR invalid JSON value"}
	}

	// Search for value in array
	for i, item := range arr {
		if equalJSON(item, searchItem) {
			return models.Value{Type: "integer", Num: i}
		}
	}

	return models.Value{Type: "integer", Num: -1}
}

func (h *JSONHandlers) HandleJSONArrTrim(args []models.Value) models.Value {
	if len(args) < 4 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRTRIM command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	startStr := args[2].Bulk
	stopStr := args[3].Bulk

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid start index"}
	}

	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid stop index"}
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Handle negative indices
	length := len(arr)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Boundary checks
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		arr = []interface{}{}
	} else {
		arr = arr[start : stop+1]
	}

	// Update the array at path
	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	if err := h.setNestedValue(data, path, arr); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: len(arr)}
}

func (h *JSONHandlers) HandleJSONNumIncrBy(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.NUMINCRBY command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	incrBy, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR increment amount must be a valid number"}
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	// Get number at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	var numValue float64
	switch v := target.(type) {
	case float64:
		numValue = v
	case int:
		numValue = float64(v)
	default:
		return models.Value{Type: "error", Str: "ERR path does not point to a number"}
	}

	// Perform increment
	newValue := numValue + incrBy

	// Update the value at path
	if err := h.setNestedValue(data, path, newValue); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	result, err := json.Marshal(newValue)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode result"}
	}

	return models.Value{Type: "bulk", Bulk: string(result)}
}

// Helper function to compare JSON values
func equalJSON(a, b interface{}) bool {
	switch v := a.(type) {
	case map[string]interface{}:
		bMap, ok := b.(map[string]interface{})
		if !ok {
			return false
		}
		if len(v) != len(bMap) {
			return false
		}
		for key, value := range v {
			if !equalJSON(value, bMap[key]) {
				return false
			}
		}
		return true
	case []interface{}:
		bArr, ok := b.([]interface{})
		if !ok {
			return false
		}
		if len(v) != len(bArr) {
			return false
		}
		for i, value := range v {
			if !equalJSON(value, bArr[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
func (h *JSONHandlers) HandleJSONObjKeys(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.OBJKEYS command"}
	}

	key := args[0].Bulk
	path := "."
	if len(args) > 1 {
		path = args[1].Bulk
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "null"}
	}

	// Get object at path
	var target interface{}
	if path == "." {
		target = value
	} else {
		var err error
		target, err = h.getNestedValue(value, path)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
	}

	obj, ok := target.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an object"}
	}

	// Get all keys
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}

	// Sort keys for consistent output
	sort.Strings(keys)

	// Convert keys to JSON array
	result, err := json.Marshal(keys)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode keys"}
	}

	return models.Value{Type: "bulk", Bulk: string(result)}
}

func (h *JSONHandlers) HandleJSONObjLen(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.OBJLEN command"}
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

	var target interface{}
	if path == "." {
		target = value
	} else {
		var err error
		target, err = h.getNestedValue(value, path)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
	}

	obj, ok := target.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an object"}
	}

	return models.Value{Type: "integer", Num: len(obj)}
}

func (h *JSONHandlers) HandleJSONArrPop(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRPOP command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	index := -1 // default to last element
	if len(args) > 2 {
		var err error
		index, err = strconv.Atoi(args[2].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid index"}
		}
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "null"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	if len(arr) == 0 {
		return models.Value{Type: "null"}
	}

	// Handle negative index
	if index < 0 {
		index = len(arr) + index
	}

	// Check bounds
	if index < 0 || index >= len(arr) {
		return models.Value{Type: "error", Str: "ERR index out of range"}
	}

	// Get the element to return
	popped := arr[index]

	// Remove the element
	arr = append(arr[:index], arr[index+1:]...)

	// Update the array
	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	if err := h.setNestedValue(data, path, arr); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	// Return the popped element
	result, err := json.Marshal(popped)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode popped value"}
	}

	return models.Value{Type: "bulk", Bulk: string(result)}
}

func (h *JSONHandlers) HandleJSONMerge(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.MERGE command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	jsonStr := args[2].Bulk

	// Parse the JSON to merge
	var mergeValue interface{}
	if err := json.Unmarshal([]byte(jsonStr), &mergeValue); err != nil {
		return models.Value{Type: "error", Str: "ERR invalid JSON string"}
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	// Get target object
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	targetObj, ok := target.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an object"}
	}

	mergeObj, ok := mergeValue.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR can only merge objects"}
	}

	// Perform deep merge
	merged := deepMerge(targetObj, mergeObj)

	// Update the object
	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	if err := h.setNestedValue(data, path, merged); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

// Helper function for deep merging objects
func deepMerge(target, source map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy target
	for k, v := range target {
		result[k] = v
	}

	// Merge source
	for k, v := range source {
		if targetVal, ok := target[k]; ok {
			// If both are maps, merge recursively
			if targetMap, isTargetMap := targetVal.(map[string]interface{}); isTargetMap {
				if sourceMap, isSourceMap := v.(map[string]interface{}); isSourceMap {
					result[k] = deepMerge(targetMap, sourceMap)
					continue
				}
			}
		}
		// Otherwise just overwrite
		result[k] = v
	}

	return result
}

func (h *JSONHandlers) HandleJSONArrInsert(args []models.Value) models.Value {
	if len(args) < 4 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRINSERT command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	indexStr := args[2].Bulk

	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid index"}
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Handle negative index
	if index < 0 {
		index = len(arr) + index
	}

	// Check bounds
	if index < 0 || index > len(arr) {
		return models.Value{Type: "error", Str: "ERR index out of range"}
	}

	// Parse and insert all values
	newValues := make([]interface{}, 0, len(args)-3)
	for i := 3; i < len(args); i++ {
		var val interface{}
		if err := json.Unmarshal([]byte(args[i].Bulk), &val); err != nil {
			return models.Value{Type: "error", Str: "ERR invalid JSON value"}
		}
		newValues = append(newValues, val)
	}

	// Insert values at index
	newArr := make([]interface{}, 0, len(arr)+len(newValues))
	newArr = append(newArr, arr[:index]...)
	newArr = append(newArr, newValues...)
	newArr = append(newArr, arr[index:]...)

	// Update the array
	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	if err := h.setNestedValue(data, path, newArr); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: len(newArr)}
}

func (h *JSONHandlers) HandleJSONNumMultBy(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.NUMMULTBY command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	multiplier, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR multiplier must be a valid number"}
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	// Get number at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	var numValue float64
	switch v := target.(type) {
	case float64:
		numValue = v
	case int:
		numValue = float64(v)
	default:
		return models.Value{Type: "error", Str: "ERR path does not point to a number"}
	}

	// Perform multiplication
	newValue := numValue * multiplier

	// Update the value at path
	if err := h.setNestedValue(data, path, newValue); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	result, err := json.Marshal(newValue)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode result"}
	}

	return models.Value{Type: "bulk", Bulk: string(result)}
}

func (h *JSONHandlers) HandleJSONClear(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.CLEAR command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "integer", Num: 0}
	}

	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	// Get value at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	var clearedValue interface{}
	switch target.(type) {
	case []interface{}:
		clearedValue = make([]interface{}, 0)
	case map[string]interface{}:
		clearedValue = make(map[string]interface{})
	case string:
		clearedValue = ""
	case float64, int:
		clearedValue = 0
	case bool:
		clearedValue = false
	default:
		clearedValue = nil
	}

	// Update the value at path
	if err := h.setNestedValue(data, path, clearedValue); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}

func (h *JSONHandlers) HandleJSONCompare(args []models.Value) models.Value {
	if len(args) < 4 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.COMPARE command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	op := args[2].Bulk
	compareValue := args[3].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "null"}
	}

	// Get value at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	// Parse comparison value
	var compareObj interface{}
	if err := json.Unmarshal([]byte(compareValue), &compareObj); err != nil {
		return models.Value{Type: "error", Str: "ERR invalid JSON value"}
	}

	result := 0
	switch op {
	case "eq":
		if equalJSON(target, compareObj) {
			result = 1
		}
	case "lt", "gt":
		comp, err := compareJSON(target, compareObj)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
		if (op == "lt" && comp < 0) || (op == "gt" && comp > 0) {
			result = 1
		}
	default:
		return models.Value{Type: "error", Str: "ERR invalid comparison operator"}
	}

	return models.Value{Type: "integer", Num: result}
}

// Helper function to compare JSON values
func compareJSON(a, b interface{}) (int, error) {
	// Convert to same type for comparison
	switch va := a.(type) {
	case float64:
		switch vb := b.(type) {
		case float64:
			if va < vb {
				return -1, nil
			} else if va > vb {
				return 1, nil
			}
			return 0, nil
		case int:
			return compareJSON(va, float64(vb))
		}
	case int:
		switch vb := b.(type) {
		case float64:
			return compareJSON(float64(va), vb)
		case int:
			if va < vb {
				return -1, nil
			} else if va > vb {
				return 1, nil
			}
			return 0, nil
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1, nil
			} else if va > vb {
				return 1, nil
			}
			return 0, nil
		}
	}
	return 0, fmt.Errorf("ERR cannot compare values of different types")
}

func (h *JSONHandlers) HandleJSONStrAppend(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.STRAPPEND command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	appendStr := args[2].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	// Get string at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	targetStr, ok := target.(string)
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to a string"}
	}

	// Append string
	newStr := targetStr + appendStr

	// Update the string at path
	if err := h.setNestedValue(data, path, newStr); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: len(newStr)}
}

func (h *JSONHandlers) HandleJSONContains(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.CONTAINS command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	searchValue := args[2].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "integer", Num: 0}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Parse search value
	var searchItem interface{}
	if err := json.Unmarshal([]byte(searchValue), &searchItem); err != nil {
		return models.Value{Type: "error", Str: "ERR invalid JSON value"}
	}

	// Check if array contains the value
	for _, item := range arr {
		if equalJSON(item, searchItem) {
			return models.Value{Type: "integer", Num: 1}
		}
	}

	return models.Value{Type: "integer", Num: 0}
}

func (h *JSONHandlers) HandleJSONArrReverse(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRREVERSE command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Reverse the array
	for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
		arr[i], arr[j] = arr[j], arr[i]
	}

	// Update the array at path
	if err := h.setNestedValue(data, path, arr); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: len(arr)}
}

func (h *JSONHandlers) HandleJSONArrSort(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRSORT command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	sortOrder := "ASC"
	if len(args) > 2 {
		sortOrder = strings.ToUpper(args[2].Bulk)
	}

	if sortOrder != "ASC" && sortOrder != "DESC" {
		return models.Value{Type: "error", Str: "ERR sort order must be ASC or DESC"}
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Sort the array
	sort.Slice(arr, func(i, j int) bool {
		comp, err := compareJSON(arr[i], arr[j])
		if err != nil {
			return false
		}
		if sortOrder == "ASC" {
			return comp < 0
		}
		return comp > 0
	})

	// Update the array at path
	if err := h.setNestedValue(data, path, arr); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: len(arr)}
}

func (h *JSONHandlers) HandleJSONArrUnique(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRUNIQUE command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Create unique array using map
	seen := make(map[string]bool)
	unique := make([]interface{}, 0)

	for _, item := range arr {
		jsonStr, err := json.Marshal(item)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR failed to process array item"}
		}

		if !seen[string(jsonStr)] {
			seen[string(jsonStr)] = true
			unique = append(unique, item)
		}
	}

	// Update the array at path
	if err := h.setNestedValue(data, path, unique); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: len(unique)}
}

func (h *JSONHandlers) HandleJSONCount(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.COUNT command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	searchValue := args[2].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "integer", Num: 0}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Parse search value
	var searchItem interface{}
	if err := json.Unmarshal([]byte(searchValue), &searchItem); err != nil {
		return models.Value{Type: "error", Str: "ERR invalid JSON value"}
	}

	// Count occurrences
	count := 0
	for _, item := range arr {
		if equalJSON(item, searchItem) {
			count++
		}
	}

	return models.Value{Type: "integer", Num: count}
}

func (h *JSONHandlers) HandleJSONSwap(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.SWAP command"}
	}

	key := args[0].Bulk
	path1 := args[1].Bulk
	path2 := args[2].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	data, ok := value.(map[string]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR root must be an object"}
	}

	// Get values at both paths
	value1, err1 := h.getNestedValue(value, path1)
	value2, err2 := h.getNestedValue(value, path2)

	if err1 != nil || err2 != nil {
		return models.Value{Type: "error", Str: "ERR invalid path"}
	}

	// Swap values
	if err := h.setNestedValue(data, path1, value2); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.setNestedValue(data, path2, value1); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if err := h.cache.SetJSON(key, data); err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *JSONHandlers) HandleJSONValidate(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.VALIDATE command"}
	}

	key := args[0].Bulk
	schema := args[1].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	// Parse schema
	var schemaObj interface{}
	if err := json.Unmarshal([]byte(schema), &schemaObj); err != nil {
		return models.Value{Type: "error", Str: "ERR invalid schema"}
	}

	// Validate against schema
	if err := validateJSON(value, schemaObj); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: 1}
}

// Helper function to validate JSON against a simple schema
func validateJSON(value, schema interface{}) error {
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return fmt.Errorf("ERR schema must be an object")
	}

	return validateType(value, schemaMap)
}

func validateType(value interface{}, schema map[string]interface{}) error {
	requiredType, ok := schema["type"].(string)
	if !ok {
		return fmt.Errorf("ERR schema must specify type")
	}

	switch requiredType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("ERR value must be string")
		}
	case "number":
		if _, ok := value.(float64); !ok {
			if _, ok := value.(int); !ok {
				return fmt.Errorf("ERR value must be number")
			}
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("ERR value must be boolean")
		}
	case "array":
		arr, ok := value.([]interface{})
		if !ok {
			return fmt.Errorf("ERR value must be array")
		}
		if items, ok := schema["items"].(map[string]interface{}); ok {
			for _, item := range arr {
				if err := validateType(item, items); err != nil {
					return err
				}
			}
		}
	case "object":
		obj, ok := value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("ERR value must be object")
		}
		if properties, ok := schema["properties"].(map[string]interface{}); ok {
			for key, propSchema := range properties {
				if propMap, ok := propSchema.(map[string]interface{}); ok {
					if propValue, exists := obj[key]; exists {
						if err := validateType(propValue, propMap); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func (h *JSONHandlers) HandleJSONArrSum(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRSUM command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	// Calculate sum
	var sum float64
	for _, item := range arr {
		switch v := item.(type) {
		case float64:
			sum += v
		case int:
			sum += float64(v)
		default:
			return models.Value{Type: "error", Str: "ERR array contains non-numeric values"}
		}
	}

	result, err := json.Marshal(sum)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode result"}
	}

	return models.Value{Type: "bulk", Bulk: string(result)}
}

func (h *JSONHandlers) HandleJSONArrAvg(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.ARRAVG command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	if len(arr) == 0 {
		return models.Value{Type: "error", Str: "ERR array is empty"}
	}

	// Calculate average
	var sum float64
	for _, item := range arr {
		switch v := item.(type) {
		case float64:
			sum += v
		case int:
			sum += float64(v)
		default:
			return models.Value{Type: "error", Str: "ERR array contains non-numeric values"}
		}
	}

	avg := sum / float64(len(arr))
	result, err := json.Marshal(avg)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode result"}
	}

	return models.Value{Type: "bulk", Bulk: string(result)}
}

func (h *JSONHandlers) HandleJSONSearch(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.SEARCH command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	keyword := args[2].Bulk
	caseSensitive := false

	if len(args) > 3 && strings.ToUpper(args[3].Bulk) == "CASE" {
		caseSensitive = true
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	// Get target value
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	// Search in value
	paths := make([]string, 0)
	searchJSON(target, keyword, caseSensitive, "", &paths)

	// Convert result to JSON array
	result, err := json.Marshal(paths)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode result"}
	}

	return models.Value{Type: "bulk", Bulk: string(result)}
}

// Helper function for recursive JSON search
func searchJSON(value interface{}, keyword string, caseSensitive bool, currentPath string, paths *[]string) {
	switch v := value.(type) {
	case map[string]interface{}:
		for key, val := range v {
			newPath := currentPath
			if newPath != "" {
				newPath += "."
			}
			newPath += key

			if searchValue(key, keyword, caseSensitive) {
				*paths = append(*paths, newPath)
			}
			searchJSON(val, keyword, caseSensitive, newPath, paths)
		}
	case []interface{}:
		for i, val := range v {
			newPath := fmt.Sprintf("%s[%d]", currentPath, i)
			searchJSON(val, keyword, caseSensitive, newPath, paths)
		}
	case string:
		if searchValue(v, keyword, caseSensitive) && currentPath != "" {
			*paths = append(*paths, currentPath)
		}
	}
}

func searchValue(value, keyword string, caseSensitive bool) bool {
	if !caseSensitive {
		value = strings.ToLower(value)
		keyword = strings.ToLower(keyword)
	}
	return strings.Contains(value, keyword)
}

func (h *JSONHandlers) HandleJSONMinMax(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.MINMAX command"}
	}

	key := args[0].Bulk
	path := args[1].Bulk
	op := strings.ToUpper(args[2].Bulk)

	if op != "MIN" && op != "MAX" {
		return models.Value{Type: "error", Str: "ERR operation must be MIN or MAX"}
	}

	// Get existing JSON
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "error", Str: "ERR key does not exist"}
	}

	// Get array at path
	target, err := h.getNestedValue(value, path)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return models.Value{Type: "error", Str: "ERR path does not point to an array"}
	}

	if len(arr) == 0 {
		return models.Value{Type: "error", Str: "ERR array is empty"}
	}

	// Find min/max value
	var result interface{} = arr[0]
	for _, item := range arr[1:] {
		comp, err := compareJSON(item, result)
		if err != nil {
			continue
		}
		if (op == "MIN" && comp < 0) || (op == "MAX" && comp > 0) {
			result = item
		}
	}

	// Convert result to JSON
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode result"}
	}

	return models.Value{Type: "bulk", Bulk: string(resultJSON)}
}

func (h *JSONHandlers) HandleJSONDebug(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.DEBUG command"}
	}

	subCommand := strings.ToUpper(args[0].Bulk)
	switch subCommand {
	case "MEMORY":
		if len(args) < 2 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.DEBUG MEMORY command"}
		}
		return h.handleJSONDebugMemory(args[1:])
	default:
		return models.Value{Type: "error", Str: "ERR unknown subcommand for JSON.DEBUG"}
	}
}

func (h *JSONHandlers) handleJSONDebugMemory(args []models.Value) models.Value {
	key := args[0].Bulk
	value, exists := h.cache.GetJSON(key)
	if !exists {
		return models.Value{Type: "integer", Num: 0}
	}

	// Estimate memory size by marshaling to JSON
	data, err := json.Marshal(value)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to calculate memory size"}
	}

	return models.Value{Type: "integer", Num: len(data)}
}

func (h *JSONHandlers) HandleJSONForget(args []models.Value) models.Value {
	// JSON.FORGET is an alias for JSON.DEL
	return h.HandleJSONDel(args)
}

func (h *JSONHandlers) HandleJSONMGet(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.MGET command"}
	}

	path := args[len(args)-1].Bulk
	keys := args[:len(args)-1]

	results := make([]interface{}, 0, len(keys))

	for _, keyArg := range keys {
		value, exists := h.cache.GetJSON(keyArg.Bulk)
		if !exists {
			results = append(results, nil)
			continue
		}

		if path == "." {
			results = append(results, value)
			continue
		}

		target, err := h.getNestedValue(value, path)
		if err != nil {
			results = append(results, nil)
			continue
		}
		results = append(results, target)
	}

	resultJSON, err := json.Marshal(results)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR failed to encode results"}
	}

	return models.Value{Type: "bulk", Bulk: string(resultJSON)}
}

func (h *JSONHandlers) HandleJSONMSet(args []models.Value) models.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.MSET command"}
	}

	for i := 0; i < len(args); i += 2 {
		key := args[i].Bulk
		jsonStr := args[i+1].Bulk

		var value interface{}
		if err := json.Unmarshal([]byte(jsonStr), &value); err != nil {
			return models.Value{Type: "error", Str: "ERR invalid JSON string"}
		}

		if err := h.cache.SetJSON(key, value); err != nil {
			return util.ToValue(err)
		}
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *JSONHandlers) HandleJSONResp(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON.RESP command"}
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

	var target interface{}
	if path == "." {
		target = value
	} else {
		var err error
		target, err = h.getNestedValue(value, path)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
	}

	// Convert to RESP format
	return jsonToResp(target)
}

// Helper function to convert JSON values to RESP format
func jsonToResp(value interface{}) models.Value {
	switch v := value.(type) {
	case nil:
		return models.Value{Type: "null"}
	case bool:
		if v {
			return models.Value{Type: "integer", Num: 1}
		}
		return models.Value{Type: "integer", Num: 0}
	case float64:
		return models.Value{Type: "bulk", Bulk: strconv.FormatFloat(v, 'f', -1, 64)}
	case int:
		return models.Value{Type: "integer", Num: v}
	case string:
		return models.Value{Type: "bulk", Bulk: v}
	case []interface{}:
		array := make([]models.Value, len(v))
		for i, item := range v {
			array[i] = jsonToResp(item)
		}
		return models.Value{Type: "array", Array: array}
	case map[string]interface{}:
		array := make([]models.Value, 0, len(v)*2)
		for key, val := range v {
			array = append(array, models.Value{Type: "bulk", Bulk: key})
			array = append(array, jsonToResp(val))
		}
		return models.Value{Type: "array", Array: array}
	default:
		return models.Value{Type: "error", Str: "ERR unsupported JSON type"}
	}
}
