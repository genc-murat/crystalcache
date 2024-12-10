package zset

import (
	"math/rand"
	"sort"
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// BasicOps handles basic operations for sorted sets
type BasicOps struct {
	cache       *sync.Map // Main cache map for zsets
	keyVersions *sync.Map
}

// NewBasicOps creates a new BasicOps instance
func NewBasicOps(cache *sync.Map, keyVersions *sync.Map) *BasicOps {
	return &BasicOps{
		cache:       cache,
		keyVersions: keyVersions,
	}
}

// ZAdd adds a member with score to a sorted set
func (b *BasicOps) ZAdd(key string, score float64, member string) error {
	var zset sync.Map
	actual, _ := b.cache.LoadOrStore(key, &zset)
	actualZSet := actual.(*sync.Map)
	actualZSet.Store(member, score)
	b.incrementKeyVersion(key)
	return nil
}

// ZCard returns the number of members in a sorted set
func (b *BasicOps) ZCard(key string) int {
	value, exists := b.cache.Load(key)
	if !exists {
		return 0
	}

	set := value.(*sync.Map)
	count := 0

	set.Range(func(_, _ interface{}) bool {
		count++
		return true
	})

	return count
}

// ZScore returns the score of a member in a sorted set
func (b *BasicOps) ZScore(key string, member string) (float64, bool) {
	value, exists := b.cache.Load(key)
	if !exists {
		return 0, false
	}

	set := value.(*sync.Map)
	memberValue, exists := set.Load(member)
	if !exists {
		return 0, false
	}

	return memberValue.(float64), true
}

// ZRem removes a member from a sorted set
func (b *BasicOps) ZRem(key string, member string) error {
	value, exists := b.cache.Load(key)
	if !exists {
		return nil
	}

	set := value.(*sync.Map)
	set.Delete(member)

	empty := true
	set.Range(func(_, _ interface{}) bool {
		empty = false
		return false
	})

	if empty {
		b.cache.Delete(key)
	}

	b.incrementKeyVersion(key)
	return nil
}

// incrementKeyVersion metodu da keyVersions kullanacak şekilde güncellendi
func (b *BasicOps) incrementKeyVersion(key string) {
	for {
		var version int64
		oldVersionI, _ := b.keyVersions.LoadOrStore(key, version)
		oldVersion := oldVersionI.(int64)
		if b.keyVersions.CompareAndSwap(key, oldVersion, oldVersion+1) {
			break
		}
	}
}

// getSortedMembers returns sorted list of members with scores
func (b *BasicOps) getSortedMembers(key string) []models.ZSetMember {
	value, exists := b.cache.Load(key)
	if !exists {
		return []models.ZSetMember{}
	}

	set := value.(*sync.Map)
	var members []models.ZSetMember

	set.Range(func(member, score interface{}) bool {
		members = append(members, models.ZSetMember{
			Member: member.(string),
			Score:  score.(float64),
		})
		return true
	})

	sort.Slice(members, func(i, j int) bool {
		if members[i].Score == members[j].Score {
			return members[i].Member < members[j].Member
		}
		return members[i].Score < members[j].Score
	})

	return members
}

// ZRandMember returns random members from a sorted set
func (b *BasicOps) ZRandMember(key string, count int, withScores bool) []models.ZSetMember {
	members := b.getSortedMembers(key)
	if len(members) == 0 || count == 0 {
		return []models.ZSetMember{}
	}

	// Handle negative count (allow duplicates)
	if count < 0 {
		count = -count
		result := make([]models.ZSetMember, count)
		for i := 0; i < count; i++ {
			idx := rand.Intn(len(members))
			result[i] = members[idx]
		}
		return result
	}

	// Handle positive count (no duplicates)
	if count > len(members) {
		count = len(members)
	}

	// Create a copy to shuffle
	result := make([]models.ZSetMember, len(members))
	copy(result, members)

	// Fisher-Yates shuffle
	for i := len(result) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		result[i], result[j] = result[j], result[i]
	}

	return result[:count]
}

// ZRandMemberWithoutScores returns random members without their scores
func (b *BasicOps) ZRandMemberWithoutScores(key string, count int) []string {
	members := b.ZRandMember(key, count, false)
	result := make([]string, len(members))
	for i, member := range members {
		result[i] = member.Member
	}
	return result
}
