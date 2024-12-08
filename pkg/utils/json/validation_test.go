package json

import (
	"testing"
)

func TestValidationUtil(t *testing.T) {
	validator := NewValidationUtil()

	// Helper function to create int pointer
	intPtr := func(i int) *int {
		return &i
	}

	// Helper function to create float64 pointer
	float64Ptr := func(f float64) *float64 {
		return &f
	}

	t.Run("String Validation", func(t *testing.T) {
		schema := &Schema{
			Type:      TypeString,
			MinLength: intPtr(2),
			MaxLength: intPtr(5),
		}

		tests := []struct {
			name    string
			value   interface{}
			wantErr bool
		}{
			{
				name:    "Valid string",
				value:   "test",
				wantErr: false,
			},
			{
				name:    "Too short string",
				value:   "a",
				wantErr: true,
			},
			{
				name:    "Too long string",
				value:   "toolong",
				wantErr: true,
			},
			{
				name:    "Wrong type",
				value:   123,
				wantErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validator.Validate(tt.value, schema)
				if (err != nil) != tt.wantErr {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})

	t.Run("Number Validation", func(t *testing.T) {
		schema := &Schema{
			Type:    TypeNumber,
			Minimum: float64Ptr(0),
			Maximum: float64Ptr(100),
		}

		tests := []struct {
			name    string
			value   interface{}
			wantErr bool
		}{
			{
				name:    "Valid integer",
				value:   50,
				wantErr: false,
			},
			{
				name:    "Valid float",
				value:   50.5,
				wantErr: false,
			},
			{
				name:    "Below minimum",
				value:   -1,
				wantErr: true,
			},
			{
				name:    "Above maximum",
				value:   101,
				wantErr: true,
			},
			{
				name:    "Wrong type",
				value:   "50",
				wantErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validator.Validate(tt.value, schema)
				if (err != nil) != tt.wantErr {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})

	t.Run("Object Validation", func(t *testing.T) {
		schema := &Schema{
			Type: TypeObject,
			Properties: map[string]*Schema{
				"name": {
					Type:      TypeString,
					MinLength: intPtr(1),
				},
				"age": {
					Type:    TypeNumber,
					Minimum: float64Ptr(0),
				},
			},
			Required: []string{"name"},
		}

		tests := []struct {
			name    string
			value   interface{}
			wantErr bool
		}{
			{
				name: "Valid object",
				value: map[string]interface{}{
					"name": "John",
					"age":  30,
				},
				wantErr: false,
			},
			{
				name: "Missing required field",
				value: map[string]interface{}{
					"age": 30,
				},
				wantErr: true,
			},
			{
				name: "Invalid field type",
				value: map[string]interface{}{
					"name": "John",
					"age":  "thirty",
				},
				wantErr: true,
			},
			{
				name:    "Not an object",
				value:   "not an object",
				wantErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validator.Validate(tt.value, schema)
				if (err != nil) != tt.wantErr {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})

	t.Run("Array Validation", func(t *testing.T) {
		schema := &Schema{
			Type: TypeArray,
			Items: &Schema{
				Type:    TypeNumber,
				Minimum: float64Ptr(0),
			},
		}

		tests := []struct {
			name    string
			value   interface{}
			wantErr bool
		}{
			{
				name:    "Valid array",
				value:   []interface{}{1, 2, 3},
				wantErr: false,
			},
			{
				name:    "Empty array",
				value:   []interface{}{},
				wantErr: false,
			},
			{
				name:    "Invalid item type",
				value:   []interface{}{1, "two", 3},
				wantErr: true,
			},
			{
				name:    "Invalid item value",
				value:   []interface{}{1, -2, 3},
				wantErr: true,
			},
			{
				name:    "Not an array",
				value:   "not an array",
				wantErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validator.Validate(tt.value, schema)
				if (err != nil) != tt.wantErr {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})

	t.Run("Nested Structure Validation", func(t *testing.T) {
		schema := &Schema{
			Type: TypeObject,
			Properties: map[string]*Schema{
				"user": {
					Type: TypeObject,
					Properties: map[string]*Schema{
						"name": {
							Type:      TypeString,
							MinLength: intPtr(1),
						},
						"scores": {
							Type: TypeArray,
							Items: &Schema{
								Type:    TypeNumber,
								Minimum: float64Ptr(0),
								Maximum: float64Ptr(100),
							},
						},
					},
					Required: []string{"name"},
				},
			},
			Required: []string{"user"},
		}

		tests := []struct {
			name    string
			value   interface{}
			wantErr bool
		}{
			{
				name: "Valid nested structure",
				value: map[string]interface{}{
					"user": map[string]interface{}{
						"name":   "John",
						"scores": []interface{}{85, 90, 95},
					},
				},
				wantErr: false,
			},
			{
				name: "Invalid nested array value",
				value: map[string]interface{}{
					"user": map[string]interface{}{
						"name":   "John",
						"scores": []interface{}{85, 101, 95}, // 101 > maximum
					},
				},
				wantErr: true,
			},
			{
				name: "Missing required nested field",
				value: map[string]interface{}{
					"user": map[string]interface{}{
						"scores": []interface{}{85, 90, 95},
					},
				},
				wantErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := validator.Validate(tt.value, schema)
				if (err != nil) != tt.wantErr {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})
}
