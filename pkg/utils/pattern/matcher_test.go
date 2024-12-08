package pattern_test

import (
	"testing"

	"github.com/genc-murat/crystalcache/pkg/utils/pattern"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		pattern string
		str     string
		want    bool
	}{
		{"*", "hello", true},
		{"he?lo", "hello", true},
		{"he?lo", "healo", true},
		{"he*o", "hello", true},
		{"[aeiou]", "a", true},
		{"[aeiou]", "b", false},
		{"h[aeiou]llo", "hello", true},
		{"h[aeiou]llo", "hillo", true},
		{"h[aeiou]llo", "hxllo", false},
		{"hello", "hello", true},
		{"hello", "world", false},
		{"\\*", "*", true},
		{"\\?", "?", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.str, func(t *testing.T) {
			got := pattern.Match(tt.pattern, tt.str)
			if got != tt.want {
				t.Errorf("Match(%q, %q) = %v; want %v", tt.pattern, tt.str, got, tt.want)
			}
		})
	}
}

func TestMatchCached(t *testing.T) {
	matcher := pattern.NewMatcher()

	tests := []struct {
		pattern string
		str     string
		want    bool
	}{
		{"*", "hello", true},
		{"he?lo", "hello", true},
		{"he*o", "hello", true},
		{"hello", "hello", true},
		{"hello", "world", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.str, func(t *testing.T) {
			got := matcher.MatchCached(tt.pattern, tt.str)
			if got != tt.want {
				t.Errorf("MatchCached(%q, %q) = %v; want %v", tt.pattern, tt.str, got, tt.want)
			}
		})
	}
}

func TestExtractLiteralPrefix(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		{"hello*", "hello"},
		{"he?lo", "he"},
		{"[abc]ello", ""},
		{"\\*hello", "*hello"},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got := pattern.ExtractLiteralPrefix(tt.pattern)
			if got != tt.want {
				t.Errorf("ExtractLiteralPrefix(%q) = %q; want %q", tt.pattern, got, tt.want)
			}
		})
	}
}

func TestIsPattern(t *testing.T) {
	tests := []struct {
		str  string
		want bool
	}{
		{"hello", false},
		{"he?lo", true},
		{"he*o", true},
		{"[aeiou]", true},
		{"\\*hello", true},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			got := pattern.IsPattern(tt.str)
			if got != tt.want {
				t.Errorf("IsPattern(%q) = %v; want %v", tt.str, got, tt.want)
			}
		})
	}
}

func TestMatchMultiple(t *testing.T) {
	tests := []struct {
		patterns []string
		str      string
		want     bool
	}{
		{[]string{"hello", "world"}, "hello", true},
		{[]string{"he*o", "world"}, "hello", true},
		{[]string{"[aeiou]", "world"}, "e", true},
		{[]string{"hello", "world"}, "test", false},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			got := pattern.MatchMultiple(tt.patterns, tt.str)
			if got != tt.want {
				t.Errorf("MatchMultiple(%v, %q) = %v; want %v", tt.patterns, tt.str, got, tt.want)
			}
		})
	}
}
