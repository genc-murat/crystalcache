package util

import (
	"errors"
	"testing"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected string
	}{
		{"positive integer", 42.0, "42"},
		{"negative integer", -42.0, "-42"},
		{"positive decimal", 3.14159, "3.14159"},
		{"negative decimal", -3.14159, "-3.14159"},
		{"zero", 0.0, "0"},
		{"large number", 1234567.89, "1234567.89"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatFloat(tt.input)
			if result != tt.expected {
				t.Errorf("FormatFloat(%f) = %s; want %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		name        string
		input       models.Value
		expected    int
		expectError bool
	}{
		{"valid positive", models.Value{Bulk: "42"}, 42, false},
		{"valid negative", models.Value{Bulk: "-42"}, -42, false},
		{"valid zero", models.Value{Bulk: "0"}, 0, false},
		{"invalid empty", models.Value{Bulk: ""}, 0, true},
		{"invalid float", models.Value{Bulk: "3.14"}, 0, true},
		{"invalid string", models.Value{Bulk: "abc"}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseInt(tt.input)
			if tt.expectError && err == nil {
				t.Errorf("ParseInt(%v) expected error but got none", tt.input)
			}
			if !tt.expectError && err != nil {
				t.Errorf("ParseInt(%v) unexpected error: %v", tt.input, err)
			}
			if !tt.expectError && result != tt.expected {
				t.Errorf("ParseInt(%v) = %d; want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		name        string
		input       models.Value
		expected    float64
		expectError bool
	}{
		{"valid integer", models.Value{Bulk: "42"}, 42.0, false},
		{"valid negative", models.Value{Bulk: "-42.5"}, -42.5, false},
		{"valid decimal", models.Value{Bulk: "3.14159"}, 3.14159, false},
		{"valid zero", models.Value{Bulk: "0"}, 0.0, false},
		{"invalid empty", models.Value{Bulk: ""}, 0.0, true},
		{"invalid string", models.Value{Bulk: "abc"}, 0.0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseFloat(tt.input)
			if tt.expectError && err == nil {
				t.Errorf("ParseFloat(%v) expected error but got none", tt.input)
			}
			if !tt.expectError && err != nil {
				t.Errorf("ParseFloat(%v) unexpected error: %v", tt.input, err)
			}
			if !tt.expectError && result != tt.expected {
				t.Errorf("ParseFloat(%v) = %f; want %f", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseBool(t *testing.T) {
	tests := []struct {
		name        string
		input       models.Value
		expected    bool
		expectError bool
	}{
		{"valid true", models.Value{Bulk: "true"}, true, false},
		{"valid false", models.Value{Bulk: "false"}, false, false},
		{"valid 1", models.Value{Bulk: "1"}, true, false},
		{"valid 0", models.Value{Bulk: "0"}, false, false},
		{"invalid empty", models.Value{Bulk: ""}, false, true},
		{"invalid string", models.Value{Bulk: "abc"}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseBool(tt.input)
			if tt.expectError && err == nil {
				t.Errorf("ParseBool(%v) expected error but got none", tt.input)
			}
			if !tt.expectError && err != nil {
				t.Errorf("ParseBool(%v) unexpected error: %v", tt.input, err)
			}
			if !tt.expectError && result != tt.expected {
				t.Errorf("ParseBool(%v) = %t; want %t", tt.input, result, tt.expected)
			}
		})
	}
}

func TestToValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected models.Value
	}{
		{
			name:     "string input",
			input:    "test",
			expected: models.Value{Type: "bulk", Bulk: "test"},
		},
		{
			name:     "integer input",
			input:    42,
			expected: models.Value{Type: "integer", Num: 42},
		},
		{
			name:     "nil input",
			input:    nil,
			expected: models.Value{Type: "null"},
		},
		{
			name:     "error input",
			input:    errors.New("test error"),
			expected: models.Value{Type: "error", Str: "test error"},
		},
		{
			name:  "string array input",
			input: []string{"a", "b", "c"},
			expected: models.Value{
				Type: "array",
				Array: []models.Value{
					{Type: "bulk", Bulk: "a"},
					{Type: "bulk", Bulk: "b"},
					{Type: "bulk", Bulk: "c"},
				},
			},
		},
		{
			name:     "unknown type input",
			input:    struct{}{},
			expected: models.Value{Type: "error", Str: "unknown type: struct {}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToValue(tt.input)

			// Compare Types
			if result.Type != tt.expected.Type {
				t.Errorf("ToValue(%v) type = %s; want %s", tt.input, result.Type, tt.expected.Type)
			}

			// Compare specific fields based on type
			switch result.Type {
			case "bulk":
				if result.Bulk != tt.expected.Bulk {
					t.Errorf("ToValue(%v) bulk = %s; want %s", tt.input, result.Bulk, tt.expected.Bulk)
				}
			case "integer":
				if result.Num != tt.expected.Num {
					t.Errorf("ToValue(%v) num = %d; want %d", tt.input, result.Num, tt.expected.Num)
				}
			case "error":
				if result.Str != tt.expected.Str {
					t.Errorf("ToValue(%v) str = %s; want %s", tt.input, result.Str, tt.expected.Str)
				}
			case "array":
				if len(result.Array) != len(tt.expected.Array) {
					t.Errorf("ToValue(%v) array length = %d; want %d", tt.input, len(result.Array), len(tt.expected.Array))
				}
				for i := range result.Array {
					if result.Array[i].Type != tt.expected.Array[i].Type ||
						result.Array[i].Bulk != tt.expected.Array[i].Bulk {
						t.Errorf("ToValue(%v) array[%d] = %v; want %v", tt.input, i, result.Array[i], tt.expected.Array[i])
					}
				}
			}
		})
	}
}
