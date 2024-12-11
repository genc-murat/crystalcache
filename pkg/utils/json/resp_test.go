package json

import (
	"reflect"
	"testing"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func TestJSONToRESP(t *testing.T) {
	r := NewRespUtil()

	tests := []struct {
		name     string
		input    interface{}
		expected models.Value
		checkFn  func(t *testing.T, got, want models.Value)
	}{
		{
			name:     "nil value",
			input:    nil,
			expected: models.Value{Type: "null"},
		},
		{
			name:     "boolean true",
			input:    true,
			expected: models.Value{Type: "integer", Num: 1},
		},
		{
			name:     "boolean false",
			input:    false,
			expected: models.Value{Type: "integer", Num: 0},
		},
		{
			name:     "integer",
			input:    42,
			expected: models.Value{Type: "integer", Num: 42},
		},
		{
			name:     "float",
			input:    3.14,
			expected: models.Value{Type: "bulk", Bulk: "3.14"},
		},
		{
			name:     "string",
			input:    "hello",
			expected: models.Value{Type: "bulk", Bulk: "hello"},
		},
		{
			name:  "array",
			input: []interface{}{1, "two", 3.14},
			expected: models.Value{
				Type: "array",
				Array: []models.Value{
					{Type: "integer", Num: 1},
					{Type: "bulk", Bulk: "two"},
					{Type: "bulk", Bulk: "3.14"},
				},
			},
		},
		{
			name:  "object",
			input: map[string]interface{}{"name": "John", "age": 30},
			checkFn: func(t *testing.T, got, want models.Value) {
				if got.Type != "array" {
					t.Errorf("expected array type, got %s", got.Type)
					return
				}

				// Convert array to map for comparison
				gotMap := make(map[string]models.Value)
				for i := 0; i < len(got.Array); i += 2 {
					key := got.Array[i].Bulk
					gotMap[key] = got.Array[i+1]
				}

				// Verify map contents
				expectedValues := map[string]models.Value{
					"name": {Type: "bulk", Bulk: "John"},
					"age":  {Type: "integer", Num: 30},
				}

				if !reflect.DeepEqual(gotMap, expectedValues) {
					t.Errorf("map contents do not match\ngot: %v\nwant: %v", gotMap, expectedValues)
				}
			},
		},
		{
			name:     "unsupported type",
			input:    struct{}{},
			expected: models.Value{Type: "error", Str: "ERR unsupported JSON type: struct {}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := r.JSONToRESP(tt.input)
			if tt.checkFn != nil {
				tt.checkFn(t, result, tt.expected)
			} else if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("JSONToRESP() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestRESPToJSON(t *testing.T) {
	r := NewRespUtil()

	tests := []struct {
		name     string
		input    models.Value
		expected interface{}
		checkFn  func(t *testing.T, got, want interface{})
	}{
		{
			name:     "null value",
			input:    models.Value{Type: "null"},
			expected: nil,
		},
		{
			name:     "integer",
			input:    models.Value{Type: "integer", Num: 42},
			expected: 42,
		},
		{
			name:     "bulk string",
			input:    models.Value{Type: "bulk", Bulk: "hello"},
			expected: "hello",
		},
		{
			name:     "bulk number",
			input:    models.Value{Type: "bulk", Bulk: "3.14"},
			expected: 3.14,
		},
		{
			name: "array",
			input: models.Value{
				Type: "array",
				Array: []models.Value{
					{Type: "integer", Num: 1},
					{Type: "bulk", Bulk: "two"},
					{Type: "bulk", Bulk: "3.14"},
				},
			},
			expected: []interface{}{1, "two", 3.14},
		},
		{
			name: "object array",
			input: models.Value{
				Type: "array",
				Array: []models.Value{
					{Type: "bulk", Bulk: "name"},
					{Type: "bulk", Bulk: "John"},
					{Type: "bulk", Bulk: "age"},
					{Type: "integer", Num: 30},
				},
			},
			expected: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			checkFn: func(t *testing.T, got, want interface{}) {
				gotMap, ok := got.(map[string]interface{})
				if !ok {
					t.Errorf("expected map[string]interface{}, got %T", got)
					return
				}
				wantMap := want.(map[string]interface{})
				if !reflect.DeepEqual(gotMap, wantMap) {
					t.Errorf("map contents do not match\ngot: %v\nwant: %v", gotMap, wantMap)
				}
			},
		},
		{
			name:     "error",
			input:    models.Value{Type: "error", Str: "test error"},
			expected: map[string]interface{}{"error": "test error"},
		},
		{
			name:     "unknown type",
			input:    models.Value{Type: "unknown"},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := r.RESPToJSON(tt.input)
			if tt.checkFn != nil {
				tt.checkFn(t, result, tt.expected)
			} else if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("RESPToJSON() = %v, want %v", result, tt.expected)
			}
		})
	}
}
