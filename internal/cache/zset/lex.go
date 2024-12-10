package zset

import (
	"strings"
)

type LexOps struct {
	basicOps *BasicOps
}

func NewLexOps(basicOps *BasicOps) *LexOps {
	return &LexOps{
		basicOps: basicOps,
	}
}

// parseLexBound parses lexicographical range bounds
func (l *LexOps) parseLexBound(bound string) (string, bool, bool) {
	isInclusive := true
	isInf := false

	// Check for infinity
	if bound == "+" || bound == "-" {
		isInf = true
		return bound, isInclusive, isInf
	}

	// Parse bound type
	if strings.HasPrefix(bound, "(") {
		isInclusive = false
		bound = bound[1:]
	} else if strings.HasPrefix(bound, "[") {
		bound = bound[1:]
	}

	return bound, isInclusive, isInf
}

// ZLexCount returns the number of elements in sorted set between min and max
func (l *LexOps) ZLexCount(key string, min, max string) (int, error) {
	minVal, minInc, minInf := l.parseLexBound(min)
	maxVal, maxInc, maxInf := l.parseLexBound(max)

	members := l.basicOps.getSortedMembers(key)
	count := 0

	for _, member := range members {
		if l.isInLexRange(member.Member, minVal, maxVal, minInc, maxInc, minInf, maxInf) {
			count++
		}
	}

	return count, nil
}

// ZRangeByLex returns elements in sorted set between min and max lexicographically
func (l *LexOps) ZRangeByLex(key string, min, max string) []string {
	minVal, minInc, minInf := l.parseLexBound(min)
	maxVal, maxInc, maxInf := l.parseLexBound(max)

	members := l.basicOps.getSortedMembers(key)
	result := make([]string, 0, len(members))

	for _, member := range members {
		if l.isInLexRange(member.Member, minVal, maxVal, minInc, maxInc, minInf, maxInf) {
			result = append(result, member.Member)
		}
	}

	return result
}

// ZRevRangeByLex returns elements in sorted set between max and min lexicographically
func (l *LexOps) ZRevRangeByLex(key string, max, min string) []string {
	result := l.ZRangeByLex(key, min, max)
	// Reverse the result
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// ZRemRangeByLex removes elements in sorted set between min and max lexicographically
func (l *LexOps) ZRemRangeByLex(key string, min, max string) (int, error) {
	minVal, minInc, minInf := l.parseLexBound(min)
	maxVal, maxInc, maxInf := l.parseLexBound(max)

	members := l.basicOps.getSortedMembers(key)
	toRemove := make([]string, 0)

	for _, member := range members {
		if l.isInLexRange(member.Member, minVal, maxVal, minInc, maxInc, minInf, maxInf) {
			toRemove = append(toRemove, member.Member)
		}
	}

	// Remove members
	for _, member := range toRemove {
		if err := l.basicOps.ZRem(key, member); err != nil {
			return 0, err
		}
	}

	return len(toRemove), nil
}

// Helper method to check if member is in lexicographical range
func (l *LexOps) isInLexRange(member, min, max string, minInc, maxInc bool, minInf, maxInf bool) bool {
	// Check min bound
	if !minInf {
		if minInc {
			if member < min {
				return false
			}
		} else {
			if member <= min {
				return false
			}
		}
	}

	// Check max bound
	if !maxInf {
		if maxInc {
			if member > max {
				return false
			}
		} else {
			if member >= max {
				return false
			}
		}
	}

	return true
}
