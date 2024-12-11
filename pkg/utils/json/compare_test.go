package json

import (
	"testing"
)

func TestEqual(t *testing.T) {
	c := NewCompare()

	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{
			name:     "equal numbers",
			a:        float64(42),
			b:        float64(42),
			expected: true,
		},
		{
			name:     "different numbers",
			a:        float64(42),
			b:        float64(43),
			expected: false,
		},
		{
			name:     "int and float equality",
			a:        42,
			b:        float64(42),
			expected: true,
		},
		{
			name:     "equal strings",
			a:        "test",
			b:        "test",
			expected: true,
		},
		{
			name:     "different strings",
			a:        "test",
			b:        "other",
			expected: false,
		},
		{
			name:     "equal booleans",
			a:        true,
			b:        true,
			expected: true,
		},
		{
			name:     "different booleans",
			a:        true,
			b:        false,
			expected: false,
		},
		{
			name:     "equal nil values",
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			name:     "nil and non-nil",
			a:        nil,
			b:        "test",
			expected: false,
		},
		{
			name: "equal objects",
			a: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			b: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			expected: true,
		},
		{
			name: "different objects",
			a: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			b: map[string]interface{}{
				"name": "Jane",
				"age":  30,
			},
			expected: false,
		},
		{
			name:     "equal arrays",
			a:        []interface{}{1, "two", true},
			b:        []interface{}{1, "two", true},
			expected: true,
		},
		{
			name:     "different arrays",
			a:        []interface{}{1, "two", true},
			b:        []interface{}{1, "two", false},
			expected: false,
		},
		{
			name: "nested structures",
			a: map[string]interface{}{
				"name": "John",
				"address": map[string]interface{}{
					"city":    "New York",
					"numbers": []interface{}{1, 2, 3},
				},
			},
			b: map[string]interface{}{
				"name": "John",
				"address": map[string]interface{}{
					"city":    "New York",
					"numbers": []interface{}{1, 2, 3},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := c.Equal(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("Equal() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCompare(t *testing.T) {
	c := NewCompare()

	tests := []struct {
		name           string
		a              interface{}
		b              interface{}
		expectedResult CompareResult
		expectError    bool
	}{
		{
			name:           "comparing equal numbers",
			a:              float64(42),
			b:              float64(42),
			expectedResult: Equal,
			expectError:    false,
		},
		{
			name:           "comparing less number",
			a:              float64(41),
			b:              float64(42),
			expectedResult: Less,
			expectError:    false,
		},
		{
			name:           "comparing greater number",
			a:              float64(43),
			b:              float64(42),
			expectedResult: Greater,
			expectError:    false,
		},
		{
			name:           "comparing equal strings",
			a:              "test",
			b:              "test",
			expectedResult: Equal,
			expectError:    false,
		},
		{
			name:           "comparing less string",
			a:              "abc",
			b:              "def",
			expectedResult: Less,
			expectError:    false,
		},
		{
			name:           "comparing greater string",
			a:              "xyz",
			b:              "abc",
			expectedResult: Greater,
			expectError:    false,
		},
		{
			name:           "comparing equal booleans",
			a:              true,
			b:              true,
			expectedResult: Equal,
			expectError:    false,
		},
		{
			name:           "comparing false with true",
			a:              false,
			b:              true,
			expectedResult: Less,
			expectError:    false,
		},
		{
			name:           "comparing equal arrays",
			a:              []interface{}{1, 2, 3},
			b:              []interface{}{1, 2, 3},
			expectedResult: Equal,
			expectError:    false,
		},
		{
			name:           "comparing arrays with different lengths",
			a:              []interface{}{1, 2},
			b:              []interface{}{1, 2, 3},
			expectedResult: Less,
			expectError:    false,
		},
		{
			name: "comparing equal objects",
			a: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			b: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			expectedResult: Equal,
			expectError:    false,
		},
		{
			name: "comparing objects with different sizes",
			a: map[string]interface{}{
				"name": "John",
			},
			b: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			expectedResult: Less,
			expectError:    false,
		},
		{
			name:           "comparing incompatible types",
			a:              42,
			b:              "test",
			expectedResult: Less,
			expectError:    true,
		},
		{
			name:           "comparing nil values",
			a:              nil,
			b:              nil,
			expectedResult: Equal,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := c.Compare(tt.a, tt.b)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result != tt.expectedResult {
					t.Errorf("Compare() = %v, want %v", result, tt.expectedResult)
				}
			}
		})
	}
}
