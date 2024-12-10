package zset

import (
	"github.com/genc-murat/crystalcache/internal/core/models"
)

type RangeOps struct {
	basicOps *BasicOps
}

func NewRangeOps(basicOps *BasicOps) *RangeOps {
	return &RangeOps{
		basicOps: basicOps,
	}
}

// adjustRangeIndices normalizes start and stop indices
func (r *RangeOps) adjustRangeIndices(start, stop, length int) (int, int) {
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	return start, stop
}

// reverseMembers reverses a slice of ZSetMembers
func (r *RangeOps) reverseMembers(members []models.ZSetMember) {
	for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
		members[i], members[j] = members[j], members[i]
	}
}

// extractMembers extracts just the member strings from ZSetMembers
func (r *RangeOps) extractMembers(members []models.ZSetMember) []string {
	result := make([]string, len(members))
	for i, member := range members {
		result[i] = member.Member
	}
	return result
}

// Range Operations
func (r *RangeOps) ZRange(key string, start, stop int) []string {
	members := r.basicOps.getSortedMembers(key)
	if len(members) == 0 {
		return []string{}
	}

	start, stop = r.adjustRangeIndices(start, stop, len(members))
	if start > stop {
		return []string{}
	}

	return r.extractMembers(members[start : stop+1])
}

func (r *RangeOps) ZRangeWithScores(key string, start, stop int) []models.ZSetMember {
	members := r.basicOps.getSortedMembers(key)
	if len(members) == 0 {
		return []models.ZSetMember{}
	}

	start, stop = r.adjustRangeIndices(start, stop, len(members))
	if start > stop {
		return []models.ZSetMember{}
	}

	return members[start : stop+1]
}

func (r *RangeOps) ZRevRange(key string, start, stop int) []string {
	members := r.basicOps.getSortedMembers(key)
	if len(members) == 0 {
		return []string{}
	}

	r.reverseMembers(members)
	start, stop = r.adjustRangeIndices(start, stop, len(members))
	if start > stop {
		return []string{}
	}

	return r.extractMembers(members[start : stop+1])
}

func (r *RangeOps) ZRevRangeWithScores(key string, start, stop int) []models.ZSetMember {
	members := r.basicOps.getSortedMembers(key)
	if len(members) == 0 {
		return []models.ZSetMember{}
	}

	r.reverseMembers(members)
	start, stop = r.adjustRangeIndices(start, stop, len(members))
	if start > stop {
		return []models.ZSetMember{}
	}

	return members[start : stop+1]
}

func (r *RangeOps) ZRangeStore(destination string, source string, start, stop int, withScores bool) (int, error) {
	var members []models.ZSetMember
	if withScores {
		members = r.ZRangeWithScores(source, start, stop)
	} else {
		stringMembers := r.ZRange(source, start, stop)
		members = make([]models.ZSetMember, len(stringMembers))
		for i, member := range stringMembers {
			score, _ := r.basicOps.ZScore(source, member)
			members[i] = models.ZSetMember{Member: member, Score: score}
		}
	}

	for _, member := range members {
		if err := r.basicOps.ZAdd(destination, member.Score, member.Member); err != nil {
			return 0, err
		}
	}

	return len(members), nil
}

// ZPopMax removes and returns members with the highest score
func (r *RangeOps) ZPopMax(key string, count int) []models.ZSetMember {
	// Get members in reverse order (highest scores first)
	members := r.ZRevRangeWithScores(key, 0, count-1)
	if len(members) == 0 {
		return []models.ZSetMember{}
	}

	// Remove the members and prepare result
	result := make([]models.ZSetMember, len(members))
	for i, member := range members {
		err := r.basicOps.ZRem(key, member.Member)
		if err != nil {
			// If error occurs during removal, return partial result
			return result[:i]
		}
		result[i] = member
	}

	return result
}

// ZPopMin removes and returns members with the lowest score
func (r *RangeOps) ZPopMin(key string, count int) []models.ZSetMember {
	// Get members in normal order (lowest scores first)
	members := r.ZRangeWithScores(key, 0, count-1)
	if len(members) == 0 {
		return []models.ZSetMember{}
	}

	// Remove the members and prepare result
	result := make([]models.ZSetMember, len(members))
	for i, member := range members {
		err := r.basicOps.ZRem(key, member.Member)
		if err != nil {
			// If error occurs during removal, return partial result
			return result[:i]
		}
		result[i] = member
	}

	return result
}

// Convenience methods for single element operations
func (r *RangeOps) ZPopMaxOne(key string) (models.ZSetMember, bool) {
	members := r.ZPopMax(key, 1)
	if len(members) == 0 {
		return models.ZSetMember{}, false
	}
	return members[0], true
}

func (r *RangeOps) ZPopMinOne(key string) (models.ZSetMember, bool) {
	members := r.ZPopMin(key, 1)
	if len(members) == 0 {
		return models.ZSetMember{}, false
	}
	return members[0], true
}

// Helper method for atomic updates
func (r *RangeOps) compareAndSwapMembers(key string, oldMembers, newMembers []models.ZSetMember) bool {
	currentMembers := r.basicOps.getSortedMembers(key)
	if len(currentMembers) != len(oldMembers) {
		return false
	}

	for i := range currentMembers {
		if currentMembers[i] != oldMembers[i] {
			return false
		}
	}
	return true
}