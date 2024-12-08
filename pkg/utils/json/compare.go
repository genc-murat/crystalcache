package json

import (
	"fmt"
)

// CompareResult represents the result of a comparison operation
type CompareResult int

const (
	Less    CompareResult = -1
	Equal   CompareResult = 0
	Greater CompareResult = 1
)

// Compare provides comparison functionality for JSON values
type Compare struct{}

func NewCompare() *Compare {
	return &Compare{}
}

// Equal checks if two JSON values are equal
func (c *Compare) Equal(a, b interface{}) bool {
	switch v := a.(type) {
	case map[string]interface{}:
		return c.compareObjects(v, b)
	case []interface{}:
		return c.compareArrays(v, b)
	default:
		return c.comparePrimitives(a, b)
	}
}

// compareObjects compares two JSON objects
func (c *Compare) compareObjects(a map[string]interface{}, b interface{}) bool {
	bMap, ok := b.(map[string]interface{})
	if !ok {
		return false
	}

	if len(a) != len(bMap) {
		return false
	}

	for key, value := range a {
		bValue, exists := bMap[key]
		if !exists {
			return false
		}
		if !c.Equal(value, bValue) {
			return false
		}
	}

	return true
}

// compareArrays compares two JSON arrays
func (c *Compare) compareArrays(a []interface{}, b interface{}) bool {
	bArr, ok := b.([]interface{})
	if !ok {
		return false
	}

	if len(a) != len(bArr) {
		return false
	}

	for i, value := range a {
		if !c.Equal(value, bArr[i]) {
			return false
		}
	}

	return true
}

// comparePrimitives compares primitive JSON values (string, number, boolean, null)
func (c *Compare) comparePrimitives(a, b interface{}) bool {
	// Handle nil cases
	if a == nil || b == nil {
		return a == b
	}

	// Compare numbers with type conversion if needed
	switch v := a.(type) {
	case float64:
		switch w := b.(type) {
		case float64:
			return v == w
		case int:
			return v == float64(w)
		}
	case int:
		switch w := b.(type) {
		case float64:
			return float64(v) == w
		case int:
			return v == w
		}
	}

	// Direct comparison for other types
	return a == b
}

// Compare numerically compares two JSON values
func (c *Compare) Compare(a, b interface{}) (CompareResult, error) {
	// Handle nil cases
	if a == nil || b == nil {
		if a == b {
			return Equal, nil
		}
		return Less, nil // Consider nil less than non-nil
	}

	switch va := a.(type) {
	case float64:
		return c.compareNumbers(va, b)
	case int:
		return c.compareNumbers(float64(va), b)
	case string:
		return c.compareStrings(va, b)
	case bool:
		return c.compareBooleans(va, b)
	case []interface{}:
		return c.compareArraysNumerically(va, b)
	case map[string]interface{}:
		return c.compareObjectsNumerically(va, b)
	default:
		return Less, fmt.Errorf("unsupported type for comparison: %T", a)
	}
}

// compareNumbers compares numeric values
func (c *Compare) compareNumbers(a float64, b interface{}) (CompareResult, error) {
	switch vb := b.(type) {
	case float64:
		if a < vb {
			return Less, nil
		} else if a > vb {
			return Greater, nil
		}
		return Equal, nil
	case int:
		return c.compareNumbers(a, float64(vb))
	default:
		return Less, fmt.Errorf("cannot compare number with %T", b)
	}
}

// compareStrings compares string values
func (c *Compare) compareStrings(a string, b interface{}) (CompareResult, error) {
	bStr, ok := b.(string)
	if !ok {
		return Less, fmt.Errorf("cannot compare string with %T", b)
	}

	if a < bStr {
		return Less, nil
	} else if a > bStr {
		return Greater, nil
	}
	return Equal, nil
}

// compareBooleans compares boolean values
func (c *Compare) compareBooleans(a bool, b interface{}) (CompareResult, error) {
	bBool, ok := b.(bool)
	if !ok {
		return Less, fmt.Errorf("cannot compare boolean with %T", b)
	}

	if a == bBool {
		return Equal, nil
	}
	if !a && bBool {
		return Less, nil
	}
	return Greater, nil
}

// compareArraysNumerically compares arrays by length and then elements
func (c *Compare) compareArraysNumerically(a []interface{}, b interface{}) (CompareResult, error) {
	bArr, ok := b.([]interface{})
	if !ok {
		return Less, fmt.Errorf("cannot compare array with %T", b)
	}

	// Compare lengths first
	if len(a) < len(bArr) {
		return Less, nil
	} else if len(a) > len(bArr) {
		return Greater, nil
	}

	// Compare elements
	for i := range a {
		result, err := c.Compare(a[i], bArr[i])
		if err != nil {
			return Less, err
		}
		if result != Equal {
			return result, nil
		}
	}

	return Equal, nil
}

// compareObjectsNumerically compares objects by number of keys and then key-value pairs
func (c *Compare) compareObjectsNumerically(a map[string]interface{}, b interface{}) (CompareResult, error) {
	bObj, ok := b.(map[string]interface{})
	if !ok {
		return Less, fmt.Errorf("cannot compare object with %T", b)
	}

	// Compare number of keys first
	if len(a) < len(bObj) {
		return Less, nil
	} else if len(a) > len(bObj) {
		return Greater, nil
	}

	// Get sorted keys
	aKeys := make([]string, 0, len(a))
	for k := range a {
		aKeys = append(aKeys, k)
	}

	bKeys := make([]string, 0, len(bObj))
	for k := range bObj {
		bKeys = append(bKeys, k)
	}

	// Compare keys first
	for i := range aKeys {
		if aKeys[i] < bKeys[i] {
			return Less, nil
		} else if aKeys[i] > bKeys[i] {
			return Greater, nil
		}
	}

	// Compare values
	for _, k := range aKeys {
		result, err := c.Compare(a[k], bObj[k])
		if err != nil {
			return Less, err
		}
		if result != Equal {
			return result, nil
		}
	}

	return Equal, nil
}
