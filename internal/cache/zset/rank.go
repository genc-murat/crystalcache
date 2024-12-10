// internal/cache/zset/rank.go

package zset

import (
	"github.com/genc-murat/crystalcache/internal/core/models"
)

type RankOps struct {
	basicOps *BasicOps
}

func NewRankOps(basicOps *BasicOps) *RankOps {
	return &RankOps{
		basicOps: basicOps,
	}
}

// ZRank returns the rank of member in the sorted set stored at key.
// The rank (or index) is 0-based, which means that the member with
// the lowest score has rank 0.
func (r *RankOps) ZRank(key string, member string) (int, bool) {
	members := r.basicOps.getSortedMembers(key)

	for i, m := range members {
		if m.Member == member {
			return i, true
		}
	}
	return 0, false
}

// ZRevRank returns the rank of member in the sorted set stored at key,
// with the scores ordered from high to low.
func (r *RankOps) ZRevRank(key string, member string) (int, bool) {
	members := r.basicOps.getSortedMembers(key)

	// Search from the end
	for i := len(members) - 1; i >= 0; i-- {
		if members[i].Member == member {
			return len(members) - 1 - i, true // Convert to reverse rank
		}
	}
	return 0, false
}

// Helper methods to improve performance if needed

// findRankByMember optimizes member search in sorted members
func (r *RankOps) findRankByMember(members []models.ZSetMember, member string) (int, bool) {
	// Could implement binary search if members are sorted by name
	for i, m := range members {
		if m.Member == member {
			return i, true
		}
	}
	return 0, false
}

// Helper for validating member existence
func (r *RankOps) memberExists(key, member string) bool {
	_, exists := r.basicOps.ZScore(key, member)
	return exists
}
