package zset

import (
	"sort"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type RankOps struct {
	basicOps *BasicOps
}

// NewRankOps creates a new instance of RankOps.
func NewRankOps(basicOps *BasicOps) *RankOps {
	return &RankOps{
		basicOps: basicOps,
	}
}

// ZRank returns the rank of a member in the sorted set stored at key.
// The rank (or index) is 0-based, meaning the member with the lowest score has rank 0.
func (r *RankOps) ZRank(key string, member string) (int, bool) {
	members, err := r.basicOps.getSortedMembers(key)
	if err != nil {
		// Handle the error appropriately, e.g., log it.
		// Returning -1 and false to indicate an error or key not found.
		return -1, false
	}

	// Sort members by score (ascending, so lower score gets lower rank)
	sort.Slice(members, func(i, j int) bool {
		return members[i].Score < members[j].Score
	})

	return r.findRankByMember(members, member)
}

// ZRevRank returns the reverse rank of a member in the sorted set stored at key,
// with the scores ordered from high to low.
func (r *RankOps) ZRevRank(key string, member string) (int, bool) {
	members, err := r.basicOps.getSortedMembers(key)
	if err != nil {
		return 0, false
	}

	rank, found := r.findRankByMember(members, member)
	if !found {
		return 0, false
	}
	return len(members) - 1 - rank, true
}

// findRankByMember searches for the rank of a member in the sorted members.
// If the member is found, its index is returned; otherwise, returns false.
func (r *RankOps) findRankByMember(members []models.ZSetMember, member string) (int, bool) {
	for i, m := range members {
		if m.Member == member {
			return i, true
		}
	}
	return 0, false
}

// memberExists checks if a member exists in the sorted set stored at key.
func (r *RankOps) memberExists(key, member string) bool {
	_, exists := r.basicOps.ZScore(key, member)
	return exists
}
