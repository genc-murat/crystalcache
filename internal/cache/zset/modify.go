package zset

import (
	"strings"
)

type ModifyOps struct {
	basicOps *BasicOps
}

func NewModifyOps(basicOps *BasicOps) *ModifyOps {
	return &ModifyOps{
		basicOps: basicOps,
	}
}

// ZRemRangeByRank removes all elements in the sorted set with rank between start and stop
func (m *ModifyOps) ZRemRangeByRank(key string, start, stop int) (int, error) {
	members, err := m.basicOps.getSortedMembers(key)
	if err != nil || len(members) == 0 {
		return 0, nil
	}

	// Adjust negative indices
	if start < 0 {
		start = len(members) + start
	}
	if stop < 0 {
		stop = len(members) + stop
	}

	// Boundary checks
	if start < 0 {
		start = 0
	}
	if stop >= len(members) {
		stop = len(members) - 1
	}
	if start > stop {
		return 0, nil
	}

	// Remove members in range
	removed := 0
	for i := start; i <= stop; i++ {
		err := m.basicOps.ZRem(key, members[i].Member)
		if err != nil {
			return removed, err
		}
		removed++
	}

	return removed, nil
}

// ZRemRangeByScore removes all elements in the sorted set with score between min and max
func (m *ModifyOps) ZRemRangeByScore(key string, min, max float64) (int, error) {
	members, err := m.basicOps.getSortedMembers(key)
	if err != nil || len(members) == 0 {
		return 0, nil
	}

	membersToRemove := []string{}
	for _, member := range members {
		if member.Score >= min && member.Score <= max {
			membersToRemove = append(membersToRemove, member.Member)
		}
	}

	removed := 0
	for _, memberToRemove := range membersToRemove {
		err := m.basicOps.ZRem(key, memberToRemove)
		if err != nil {
			return removed, err
		}
		removed++
	}

	return removed, nil
}

// ZRemRangeByRankCount removes a specified number of elements from the sorted set at given ranks
func (m *ModifyOps) ZRemRangeByRankCount(key string, start, stop, count int) (int, error) {
	members, err := m.basicOps.getSortedMembers(key)
	if err != nil || len(members) == 0 {
		return 0, nil
	}

	// Adjust indices for easier slicing
	adjustedStart := start
	adjustedStop := stop
	if adjustedStart < 0 {
		adjustedStart = len(members) + adjustedStart
	}
	if adjustedStop < 0 {
		adjustedStop = len(members) + adjustedStop
	}

	// Clamp indices to valid range
	if adjustedStart < 0 {
		adjustedStart = 0
	}
	if adjustedStop >= len(members) {
		adjustedStop = len(members) - 1
	}

	// Handle cases with no overlap
	if adjustedStart > adjustedStop {
		return 0, nil
	}

	// Determine the actual number of elements to remove
	numToRemove := count
	available := adjustedStop - adjustedStart + 1
	if numToRemove > available {
		numToRemove = available
	}

	removedCount := 0
	for i := 0; i < numToRemove; i++ {
		memberToRemove := members[adjustedStart+i].Member
		if err := m.basicOps.ZRem(key, memberToRemove); err != nil {
			return removedCount, err
		}
		removedCount++
	}

	return removedCount, nil
}

// Helper methods
func (m *ModifyOps) parseLexBound(bound string) (string, bool, bool) {
	isInclusive := true
	isInf := false

	if bound == "+" || bound == "-" {
		isInf = true
		return bound, isInclusive, isInf
	}

	if strings.HasPrefix(bound, "(") {
		isInclusive = false
		bound = bound[1:]
	} else if strings.HasPrefix(bound, "[") {
		bound = bound[1:]
	}

	return bound, isInclusive, isInf
}

func (m *ModifyOps) isInLexRange(member, min, max string, minInc, maxInc bool, minInf, maxInf bool) bool {
	minCondition := minInf || (minInc && member >= min) || (!minInc && member > min)
	maxCondition := maxInf || (maxInc && member <= max) || (!maxInc && member < max)
	return minCondition && maxCondition
}
