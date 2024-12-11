package json

import (
	"reflect"
	"testing"
)

func TestSearchUtil_Search(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		keyword  string
		opts     *SearchOptions
		expected []SearchResult
	}{
		{
			name: "Match keys and values (case-insensitive)",
			jsonData: `
			{
				"name": "example",
				"info": {
					"details": "example details",
					"tags": ["example", "json", "search"]
				}
			}`,
			keyword: "example",
			opts: &SearchOptions{
				CaseSensitive: false,
				IncludeKeys:   true,
				IncludeValues: true,
			},
			expected: []SearchResult{
				{Path: "name", Key: "name", Value: "example", IsKey: true},
				{Path: "name", Value: "example", IsKey: false},
				{Path: "info.details", Key: "details", Value: "example details", IsKey: true},
				{Path: "info.details", Value: "example details", IsKey: false},
				{Path: "info.tags[0]", Value: "example", IsKey: false},
			},
		},
		{
			name: "Case-sensitive match",
			jsonData: `
			{
				"Name": "Example",
				"info": {
					"Details": "Example details",
					"tags": ["example", "json", "search"]
				}
			}`,
			keyword: "Example",
			opts: &SearchOptions{
				CaseSensitive: true,
				IncludeKeys:   true,
				IncludeValues: true,
			},
			expected: []SearchResult{
				{Path: "Name", Key: "Name", Value: "Example", IsKey: true},
				{Path: "Name", Value: "Example", IsKey: false},
				{Path: "info.Details", Key: "Details", Value: "Example details", IsKey: true},
				{Path: "info.Details", Value: "Example details", IsKey: false},
			},
		},
		{
			name: "Match only keys",
			jsonData: `
			{
				"name": "value",
				"nested": {
					"name": "another value"
				}
			}`,
			keyword: "name",
			opts: &SearchOptions{
				CaseSensitive: false,
				IncludeKeys:   true,
				IncludeValues: false,
			},
			expected: []SearchResult{
				{Path: "name", Key: "name", Value: "value", IsKey: true},
				{Path: "nested.name", Key: "name", Value: "another value", IsKey: true},
			},
		},
		{
			name: "Match only values",
			jsonData: `
			{
				"key1": "value1",
				"key2": {
					"key3": "value1"
				}
			}`,
			keyword: "value1",
			opts: &SearchOptions{
				CaseSensitive: false,
				IncludeKeys:   false,
				IncludeValues: true,
			},
			expected: []SearchResult{
				{Path: "key1", Value: "value1", IsKey: false},
				{Path: "key2.key3", Value: "value1", IsKey: false},
			},
		},
	}

	util := NewSearchUtil()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := util.Search(tt.jsonData, tt.keyword, tt.opts)
			if !reflect.DeepEqual(results, tt.expected) {
				t.Errorf("Expected %+v, got %+v", tt.expected, results)
			}
		})
	}
}
