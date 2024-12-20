package zset

import (
	"fmt"
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

	// Handle infinity bounds
	if bound == "+" || bound == "-" {
		isInf = true
		return bound, isInclusive, isInf
	}

	// Parse bound type
	if len(bound) > 1 {
		if strings.HasPrefix(bound, "(") {
			isInclusive = false
			bound = bound[1:] // Exclude the '(' character
		} else if strings.HasPrefix(bound, "[") {
			bound = bound[1:] // Exclude the '[' character
		}
	}

	return bound, isInclusive, isInf
}

// ZLexCount returns the number of elements in sorted set between min and max
func (l *LexOps) ZLexCount(key string, min, max string) (int, error) {
	// Parse lexicographical bounds
	minVal, minInc, minInf := l.parseLexBound(min)
	maxVal, maxInc, maxInf := l.parseLexBound(max)

	// Retrieve sorted members for the given key
	members, err := l.basicOps.getSortedMembers(key)
	if err != nil || len(members) == 0 {
		// Return 0 immediately if there are no members
		return 0, nil
	}

	// Count members within the lexicographical range
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
	// Parse lexicographical bounds
	minVal, minInc, minInf := l.parseLexBound(min)
	maxVal, maxInc, maxInf := l.parseLexBound(max)

	// Retrieve sorted members for the given key
	members, err := l.basicOps.getSortedMembers(key)
	if err != nil || len(members) == 0 {
		// Return immediately if no members are found
		return []string{}
	}

	// Filter members within the lexicographical range
	result := []string{}
	for _, member := range members {
		if l.isInLexRange(member.Member, minVal, maxVal, minInc, maxInc, minInf, maxInf) {
			result = append(result, member.Member)
		}
	}

	return result
}

// ZRevRangeByLex returns elements in sorted set between max and min lexicographically
func (l *LexOps) ZRevRangeByLex(key string, max, min string) []string {
	// Fetch the range using ZRangeByLex
	result := l.ZRangeByLex(key, min, max)

	// Reverse the slice in-place
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// ZRemRangeByLex removes elements in sorted set between min and max lexicographically
func (l *LexOps) ZRemRangeByLex(key string, min, max string) (int, error) {
	// Parse lexicographical bounds
	minVal, minInc, minInf := l.parseLexBound(min)
	maxVal, maxInc, maxInf := l.parseLexBound(max)

	// Retrieve sorted members for the given key
	members, err := l.basicOps.getSortedMembers(key)
	if err != nil || len(members) == 0 {
		// Return immediately if there are no members
		return 0, nil
	}

	// Filter members to remove
	toRemove := []string{}
	for _, member := range members {
		if l.isInLexRange(member.Member, minVal, maxVal, minInc, maxInc, minInf, maxInf) {
			toRemove = append(toRemove, member.Member)
		}
	}

	// Remove members and count removals
	for _, member := range toRemove {
		if err := l.basicOps.ZRem(key, member); err != nil {
			// Return an error if any removal fails
			return 0, fmt.Errorf("failed to remove member '%s': %w", member, err)
		}
	}

	// Return the number of removed members
	return len(toRemove), nil
}

// Helper method to check if member is in lexicographical range
func (l *LexOps) isInLexRange(member, min, max string, minInc, maxInc, minInf, maxInf bool) bool {
	// Check the minimum bound
	if !minInf {
		if (minInc && member < min) || (!minInc && member <= min) {
			return false
		}
	}

	// Check the maximum bound
	if !maxInf {
		if (maxInc && member > max) || (!maxInc && member >= max) {
			return false
		}
	}

	return true
}
