package pattern

import (
	"regexp"
	"strings"
)

// Matcher provides pattern matching functionality similar to Redis' pattern matching
type Matcher struct {
	// compiled holds pre-compiled regex patterns for better performance
	compiled map[string]*regexp.Regexp
}

// NewMatcher creates a new pattern matcher with internal caching
func NewMatcher() *Matcher {
	return &Matcher{
		compiled: make(map[string]*regexp.Regexp),
	}
}

// Match checks if a string matches a Redis-style pattern
// Supported wildcards:
// * - matches any sequence of characters
// ? - matches any single character
// [...] - matches any single character within the brackets
// \x - escape character x
func Match(pattern, str string) bool {
	if pattern == "*" {
		return true
	}

	// Convert Redis pattern to regex pattern
	regexPattern := convertRedisToRegex(pattern)

	// Compile and match
	regex, err := regexp.Compile("^" + regexPattern + "$")
	if err != nil {
		return false
	}

	return regex.MatchString(str)
}

// MatchCached is like Match but caches compiled patterns for better performance
func (m *Matcher) MatchCached(pattern, str string) bool {
	if pattern == "*" {
		return true
	}

	regex, ok := m.compiled[pattern]
	if !ok {
		regexPattern := convertRedisToRegex(pattern)
		var err error
		regex, err = regexp.Compile("^" + regexPattern + "$")
		if err != nil {
			return false
		}
		m.compiled[pattern] = regex
	}

	return regex.MatchString(str)
}

// convertRedisToRegex converts Redis glob-style pattern to regular expression
func convertRedisToRegex(pattern string) string {
	var result strings.Builder
	result.Grow(len(pattern) * 2) // Pre-allocate space

	inCharClass := false
	escaped := false

	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]

		if escaped {
			// If character is escaped, add it literally
			result.WriteByte(ch)
			escaped = false
			continue
		}

		switch ch {
		case '\\':
			if i < len(pattern)-1 {
				escaped = true
			} else {
				result.WriteString("\\\\")
			}
		case '*':
			if !inCharClass {
				result.WriteString(".*")
			} else {
				result.WriteByte(ch)
			}
		case '?':
			if !inCharClass {
				result.WriteByte('.')
			} else {
				result.WriteByte(ch)
			}
		case '[':
			inCharClass = true
			result.WriteByte(ch)
		case ']':
			inCharClass = false
			result.WriteByte(ch)
		case '^', '$', '.', '+', '|', '(', ')', '{', '}':
			if !inCharClass {
				result.WriteByte('\\')
			}
			result.WriteByte(ch)
		default:
			result.WriteByte(ch)
		}
	}

	return result.String()
}

// ExtractLiteralPrefix returns the literal prefix of a pattern before any wildcards
// This can be used for optimization in search operations
func ExtractLiteralPrefix(pattern string) string {
	var prefix strings.Builder
	escaped := false

	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]

		if escaped {
			prefix.WriteByte(ch)
			escaped = false
			continue
		}

		switch ch {
		case '\\':
			if i < len(pattern)-1 {
				escaped = true
			} else {
				return prefix.String()
			}
		case '*', '?', '[':
			return prefix.String()
		default:
			prefix.WriteByte(ch)
		}
	}

	return prefix.String()
}

// IsPattern checks if a string contains pattern metacharacters
func IsPattern(str string) bool {
	escaped := false
	for i := 0; i < len(str); i++ {
		if escaped {
			escaped = false
			continue
		}

		switch str[i] {
		case '\\':
			escaped = true
		case '*', '?', '[', ']':
			return true
		}
	}
	return false
}

// MatchMultiple checks if a string matches any of the provided patterns
func MatchMultiple(patterns []string, str string) bool {
	for _, pattern := range patterns {
		if Match(pattern, str) {
			return true
		}
	}
	return false
}
