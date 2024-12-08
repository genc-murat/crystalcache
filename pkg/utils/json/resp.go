package json

import (
	"fmt"
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// RespUtil provides utilities for converting between JSON and RESP format
type RespUtil struct{}

// NewRespUtil creates a new instance of RespUtil
func NewRespUtil() *RespUtil {
	return &RespUtil{}
}

// JSONToRESP converts a JSON value to RESP format
func (r *RespUtil) JSONToRESP(value interface{}) models.Value {
	switch v := value.(type) {
	case nil:
		return models.Value{Type: "null"}
	case bool:
		return r.convertBool(v)
	case float64:
		return r.convertFloat(v)
	case int:
		return r.convertInt(v)
	case string:
		return r.convertString(v)
	case []interface{}:
		return r.convertArray(v)
	case map[string]interface{}:
		return r.convertObject(v)
	default:
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR unsupported JSON type: %T", value),
		}
	}
}

// convertBool converts boolean to RESP format
func (r *RespUtil) convertBool(v bool) models.Value {
	if v {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

// convertFloat converts float64 to RESP format
func (r *RespUtil) convertFloat(v float64) models.Value {
	return models.Value{
		Type: "bulk",
		Bulk: strconv.FormatFloat(v, 'f', -1, 64),
	}
}

// convertInt converts int to RESP format
func (r *RespUtil) convertInt(v int) models.Value {
	return models.Value{Type: "integer", Num: v}
}

// convertString converts string to RESP format
func (r *RespUtil) convertString(v string) models.Value {
	return models.Value{Type: "bulk", Bulk: v}
}

// convertArray converts JSON array to RESP format
func (r *RespUtil) convertArray(v []interface{}) models.Value {
	array := make([]models.Value, len(v))
	for i, item := range v {
		array[i] = r.JSONToRESP(item)
	}
	return models.Value{Type: "array", Array: array}
}

// convertObject converts JSON object to RESP format
func (r *RespUtil) convertObject(v map[string]interface{}) models.Value {
	// For objects, we create an array with alternating keys and values
	array := make([]models.Value, 0, len(v)*2)
	for key, val := range v {
		array = append(array, models.Value{Type: "bulk", Bulk: key})
		array = append(array, r.JSONToRESP(val))
	}
	return models.Value{Type: "array", Array: array}
}

// RESPToJSON converts a RESP value to JSON format
func (r *RespUtil) RESPToJSON(value models.Value) interface{} {
	switch value.Type {
	case "null":
		return nil
	case "integer":
		return value.Num
	case "bulk":
		// Try to convert to number if possible
		if num, err := strconv.ParseFloat(value.Bulk, 64); err == nil {
			return num
		}
		return value.Bulk
	case "array":
		return r.convertRESPArray(value.Array)
	case "error":
		return map[string]interface{}{
			"error": value.Str,
		}
	default:
		return nil
	}
}

// convertRESPArray converts RESP array to JSON format
func (r *RespUtil) convertRESPArray(array []models.Value) interface{} {
	// Check if array represents an object (alternating key-value pairs)
	if len(array)%2 == 0 {
		isObject := true
		for i := 0; i < len(array); i += 2 {
			if array[i].Type != "bulk" {
				isObject = false
				break
			}
		}
		if isObject {
			return r.convertRESPArrayToObject(array)
		}
	}

	// Convert as regular array
	result := make([]interface{}, len(array))
	for i, v := range array {
		result[i] = r.RESPToJSON(v)
	}
	return result
}

// convertRESPArrayToObject converts RESP array to JSON object
func (r *RespUtil) convertRESPArrayToObject(array []models.Value) interface{} {
	result := make(map[string]interface{})
	for i := 0; i < len(array); i += 2 {
		key := array[i].Bulk
		value := r.RESPToJSON(array[i+1])
		result[key] = value
	}
	return result
}
