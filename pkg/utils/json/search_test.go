package json

import (
	"reflect"
	"sort"
	"testing"
)

func TestSearchUtil(t *testing.T) {
	searchUtil := NewSearchUtil()

	// Sample test data structure
	testData := map[string]interface{}{
		"user": map[string]interface{}{
			"personalInfo": map[string]interface{}{
				"firstName": "Murat",
				"lastName":  "Genc",
				"email":     "gencmurat@gencmurat.com",
			},
			"preferences": map[string]interface{}{
				"theme": "dark",
				"notifications": map[string]interface{}{
					"email": true,
					"push":  false,
				},
			},
			"history": []interface{}{
				map[string]interface{}{
					"date":   "2024-01-01",
					"action": "login",
				},
				map[string]interface{}{
					"date":   "2024-01-02",
					"action": "update_profile",
				},
			},
		},
		"settings": map[string]interface{}{
			"email": "admin@gencmurat.com",
		},
	}

	t.Run("Basic Search", func(t *testing.T) {
		tests := []struct {
			name     string
			keyword  string
			opts     *SearchOptions
			expected []string
		}{
			{
				name:    "Simple Value Search",
				keyword: "Murat",
				opts:    DefaultSearchOptions(),
				expected: []string{
					"user.personalInfo.firstName",
				},
			},
			{
				name:    "Case Insensitive Search",
				keyword: "john",
				opts:    DefaultSearchOptions(),
				expected: []string{
					"user.personalInfo.firstName",
				},
			},
			{
				name:    "Case Sensitive Search",
				keyword: "john",
				opts: &SearchOptions{
					CaseSensitive: true,
					IncludeKeys:   true,
					IncludeValues: true,
					MaxDepth:      -1,
				},
				expected: []string{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				results := searchUtil.Search(testData, tt.keyword, tt.opts)
				sort.Strings(results)
				sort.Strings(tt.expected)
				if !reflect.DeepEqual(results, tt.expected) {
					t.Errorf("Search() = %v, want %v", results, tt.expected)
				}
			})
		}
	})

	t.Run("Search Options", func(t *testing.T) {
		tests := []struct {
			name     string
			keyword  string
			opts     *SearchOptions
			expected []string
		}{
			{
				name:    "Keys Only Search",
				keyword: "email",
				opts: &SearchOptions{
					CaseSensitive: false,
					IncludeKeys:   true,
					IncludeValues: false,
					MaxDepth:      -1,
				},
				expected: []string{
					"user.personalInfo.email",
					"user.preferences.notifications.email",
					"settings.email",
				},
			},
			{
				name:    "Values Only Search",
				keyword: "dark",
				opts: &SearchOptions{
					CaseSensitive: false,
					IncludeKeys:   false,
					IncludeValues: true,
					MaxDepth:      -1,
				},
				expected: []string{
					"user.preferences.theme",
				},
			},
			{
				name:    "Limited Depth Search",
				keyword: "email",
				opts: &SearchOptions{
					CaseSensitive: false,
					IncludeKeys:   true,
					IncludeValues: true,
					MaxDepth:      2,
				},
				expected: []string{
					"settings.email",
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				results := searchUtil.Search(testData, tt.keyword, tt.opts)
				sort.Strings(results)
				sort.Strings(tt.expected)
				if !reflect.DeepEqual(results, tt.expected) {
					t.Errorf("Search() = %v, want %v", results, tt.expected)
				}
			})
		}
	})

	t.Run("Edge Cases", func(t *testing.T) {
		tests := []struct {
			name     string
			data     interface{}
			keyword  string
			opts     *SearchOptions
			expected []string
		}{
			{
				name:     "Nil Data",
				data:     nil,
				keyword:  "test",
				opts:     DefaultSearchOptions(),
				expected: []string{},
			},
			{
				name:     "Empty Object",
				data:     map[string]interface{}{},
				keyword:  "test",
				opts:     DefaultSearchOptions(),
				expected: []string{},
			},
			{
				name:     "Empty Array",
				data:     []interface{}{},
				keyword:  "test",
				opts:     DefaultSearchOptions(),
				expected: []string{},
			},
			{
				name:     "Empty String Search",
				data:     testData,
				keyword:  "",
				opts:     DefaultSearchOptions(),
				expected: []string{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				results := searchUtil.Search(tt.data, tt.keyword, tt.opts)
				if !reflect.DeepEqual(results, tt.expected) {
					t.Errorf("Search() = %v, want %v", results, tt.expected)
				}
			})
		}
	})
}
