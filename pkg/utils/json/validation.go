package json

import (
	"fmt"
)

// SchemaType represents the type of a JSON value
type SchemaType string

const (
	TypeString  SchemaType = "string"
	TypeNumber  SchemaType = "number"
	TypeBoolean SchemaType = "boolean"
	TypeArray   SchemaType = "array"
	TypeObject  SchemaType = "object"
	TypeNull    SchemaType = "null"
)

// ValidationError represents a schema validation error
type ValidationError struct {
	Path    string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Path == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Path, e.Message)
}

// Schema represents a JSON schema structure
type Schema struct {
	Type       SchemaType         `json:"type"`
	Properties map[string]*Schema `json:"properties,omitempty"`
	Required   []string           `json:"required,omitempty"`
	Items      *Schema            `json:"items,omitempty"`
	MinLength  *int               `json:"minLength,omitempty"`
	MaxLength  *int               `json:"maxLength,omitempty"`
	Minimum    *float64           `json:"minimum,omitempty"`
	Maximum    *float64           `json:"maximum,omitempty"`
	Pattern    *string            `json:"pattern,omitempty"`
	Enum       []interface{}      `json:"enum,omitempty"`
}

// ValidationUtil provides JSON schema validation functionality
type ValidationUtil struct{}

// NewValidationUtil creates a new instance of ValidationUtil
func NewValidationUtil() *ValidationUtil {
	return &ValidationUtil{}
}

// Validate validates a JSON value against a schema
func (v *ValidationUtil) Validate(value interface{}, schema *Schema) error {
	return v.validateWithPath(value, schema, "")
}

// validateWithPath validates a value at a specific path
func (v *ValidationUtil) validateWithPath(value interface{}, schema *Schema, path string) error {
	if schema == nil {
		return nil
	}

	// Handle null values
	if value == nil {
		if schema.Type != TypeNull {
			return &ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected %s, got null", schema.Type),
			}
		}
		return nil
	}

	// Validate type
	if err := v.validateType(value, schema, path); err != nil {
		return err
	}

	// Type-specific validation
	switch schema.Type {
	case TypeObject:
		return v.validateObject(value, schema, path)
	case TypeArray:
		return v.validateArray(value, schema, path)
	case TypeString:
		return v.validateString(value, schema, path)
	case TypeNumber:
		return v.validateNumber(value, schema, path)
	}

	return nil
}

// validateType checks if the value matches the schema type
func (v *ValidationUtil) validateType(value interface{}, schema *Schema, path string) error {
	switch schema.Type {
	case TypeString:
		if _, ok := value.(string); !ok {
			return &ValidationError{Path: path, Message: "expected string"}
		}
	case TypeNumber:
		switch value.(type) {
		case float64, int, int64:
			// Valid number types
		default:
			return &ValidationError{Path: path, Message: "expected number"}
		}
	case TypeBoolean:
		if _, ok := value.(bool); !ok {
			return &ValidationError{Path: path, Message: "expected boolean"}
		}
	case TypeArray:
		if _, ok := value.([]interface{}); !ok {
			return &ValidationError{Path: path, Message: "expected array"}
		}
	case TypeObject:
		if _, ok := value.(map[string]interface{}); !ok {
			return &ValidationError{Path: path, Message: "expected object"}
		}
	}
	return nil
}

// validateObject validates an object against a schema
func (v *ValidationUtil) validateObject(value interface{}, schema *Schema, path string) error {
	obj, ok := value.(map[string]interface{})
	if !ok {
		return &ValidationError{Path: path, Message: "expected object"}
	}

	// Check required properties
	for _, required := range schema.Required {
		if _, exists := obj[required]; !exists {
			return &ValidationError{
				Path:    v.joinPath(path, required),
				Message: "required property missing",
			}
		}
	}

	// Validate each property
	for key, val := range obj {
		propPath := v.joinPath(path, key)
		if propSchema, ok := schema.Properties[key]; ok {
			if err := v.validateWithPath(val, propSchema, propPath); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateArray validates an array against a schema
func (v *ValidationUtil) validateArray(value interface{}, schema *Schema, path string) error {
	arr, ok := value.([]interface{})
	if !ok {
		return &ValidationError{Path: path, Message: "expected array"}
	}

	// Validate each array item
	if schema.Items != nil {
		for i, item := range arr {
			itemPath := fmt.Sprintf("%s[%d]", path, i)
			if err := v.validateWithPath(item, schema.Items, itemPath); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateString validates a string against schema constraints
func (v *ValidationUtil) validateString(value interface{}, schema *Schema, path string) error {
	str, ok := value.(string)
	if !ok {
		return &ValidationError{Path: path, Message: "expected string"}
	}

	if schema.MinLength != nil && len(str) < *schema.MinLength {
		return &ValidationError{
			Path:    path,
			Message: fmt.Sprintf("string length %d is less than minimum %d", len(str), *schema.MinLength),
		}
	}

	if schema.MaxLength != nil && len(str) > *schema.MaxLength {
		return &ValidationError{
			Path:    path,
			Message: fmt.Sprintf("string length %d is greater than maximum %d", len(str), *schema.MaxLength),
		}
	}

	return nil
}

// validateNumber validates a number against schema constraints
func (v *ValidationUtil) validateNumber(value interface{}, schema *Schema, path string) error {
	var num float64

	switch n := value.(type) {
	case float64:
		num = n
	case int:
		num = float64(n)
	case int64:
		num = float64(n)
	default:
		return &ValidationError{Path: path, Message: "expected number"}
	}

	if schema.Minimum != nil && num < *schema.Minimum {
		return &ValidationError{
			Path:    path,
			Message: fmt.Sprintf("number %v is less than minimum %v", num, *schema.Minimum),
		}
	}

	if schema.Maximum != nil && num > *schema.Maximum {
		return &ValidationError{
			Path:    path,
			Message: fmt.Sprintf("number %v is greater than maximum %v", num, *schema.Maximum),
		}
	}

	return nil
}

// joinPath joins path components
func (v *ValidationUtil) joinPath(base, key string) string {
	if base == "" {
		return key
	}
	return base + "." + key
}
