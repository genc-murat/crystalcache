package json

import (
	"reflect"
	"testing"
)

func TestNewMerge(t *testing.T) {
	merge := NewMerge()
	if merge == nil {
		t.Error("NewMerge() should return a non-nil instance")
	}
}

func TestDefaultMergeOptions(t *testing.T) {
	opts := DefaultMergeOptions()
	if !opts.OverwriteExisting {
		t.Error("Default OverwriteExisting should be true")
	}
	if opts.MaxDepth != -1 {
		t.Error("Default MaxDepth should be -1")
	}
	if opts.SkipNullValues {
		t.Error("Default SkipNullValues should be false")
	}
}

func TestDeepMerge(t *testing.T) {
	merge := NewMerge()

	tests := []struct {
		name     string
		target   map[string]interface{}
		source   map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name:     "Empty maps",
			target:   map[string]interface{}{},
			source:   map[string]interface{}{},
			expected: map[string]interface{}{},
		},
		{
			name:     "Basic merge",
			target:   map[string]interface{}{"a": 1},
			source:   map[string]interface{}{"b": 2},
			expected: map[string]interface{}{"a": 1, "b": 2},
		},
		{
			name:     "Overwrite existing",
			target:   map[string]interface{}{"a": 1},
			source:   map[string]interface{}{"a": 2},
			expected: map[string]interface{}{"a": 2},
		},
		{
			name: "Nested maps",
			target: map[string]interface{}{
				"nested": map[string]interface{}{"a": 1},
			},
			source: map[string]interface{}{
				"nested": map[string]interface{}{"b": 2},
			},
			expected: map[string]interface{}{
				"nested": map[string]interface{}{"a": 1, "b": 2},
			},
		},
		{
			name: "Array merge",
			target: map[string]interface{}{
				"arr": []interface{}{1, 2},
			},
			source: map[string]interface{}{
				"arr": []interface{}{3, 4},
			},
			expected: map[string]interface{}{
				"arr": []interface{}{1, 2, 3, 4},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := merge.DeepMerge(tt.target, tt.source)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("DeepMerge() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDeepMergeWithOptions(t *testing.T) {
	merge := NewMerge()

	tests := []struct {
		name     string
		target   map[string]interface{}
		source   map[string]interface{}
		opts     *MergeOptions
		expected map[string]interface{}
	}{
		{
			name:   "Skip null values",
			target: map[string]interface{}{"a": 1},
			source: map[string]interface{}{"b": nil, "c": 2},
			opts: &MergeOptions{
				OverwriteExisting: true,
				MaxDepth:          -1,
				SkipNullValues:    true,
			},
			expected: map[string]interface{}{"a": 1, "c": 2},
		},
		{
			name: "Max depth limit",
			target: map[string]interface{}{
				"nested": map[string]interface{}{
					"deep": map[string]interface{}{"a": 1},
				},
			},
			source: map[string]interface{}{
				"nested": map[string]interface{}{
					"deep": map[string]interface{}{"b": 2},
				},
			},
			opts: &MergeOptions{
				OverwriteExisting: true,
				MaxDepth:          1,
				SkipNullValues:    false,
			},
			expected: map[string]interface{}{
				"nested": map[string]interface{}{
					"deep": map[string]interface{}{"b": 2},
				},
			},
		},
		{
			name:   "Don't overwrite existing",
			target: map[string]interface{}{"a": 1},
			source: map[string]interface{}{"a": 2, "b": 3},
			opts: &MergeOptions{
				OverwriteExisting: false,
				MaxDepth:          -1,
				SkipNullValues:    false,
			},
			expected: map[string]interface{}{"a": 1, "b": 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := merge.DeepMergeWithOptions(tt.target, tt.source, tt.opts)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("DeepMergeWithOptions() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMergeMultiple(t *testing.T) {
	merge := NewMerge()

	obj1 := map[string]interface{}{"a": 1}
	obj2 := map[string]interface{}{"b": 2}
	obj3 := map[string]interface{}{"c": 3}

	result := merge.MergeMultiple(obj1, obj2, obj3)
	expected := map[string]interface{}{"a": 1, "b": 2, "c": 3}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("MergeMultiple() = %v, want %v", result, expected)
	}

	// Test empty input
	emptyResult := merge.MergeMultiple()
	if len(emptyResult) != 0 {
		t.Error("MergeMultiple() with no arguments should return empty map")
	}
}

func TestMergePatch(t *testing.T) {
	merge := NewMerge()

	tests := []struct {
		name     string
		target   map[string]interface{}
		patch    map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name:     "Remove field",
			target:   map[string]interface{}{"a": 1, "b": 2},
			patch:    map[string]interface{}{"a": nil},
			expected: map[string]interface{}{"b": 2},
		},
		{
			name: "Nested patch",
			target: map[string]interface{}{
				"nested": map[string]interface{}{"a": 1, "b": 2},
			},
			patch: map[string]interface{}{
				"nested": map[string]interface{}{"b": 3, "c": 4},
			},
			expected: map[string]interface{}{
				"nested": map[string]interface{}{"a": 1, "b": 3, "c": 4},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := merge.MergePatch(tt.target, tt.patch)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("MergePatch() = %v, want %v", result, tt.expected)
			}
		})
	}
}
