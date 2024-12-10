package zset

import (
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type ScoreOps struct {
	basicOps *BasicOps
}

func NewScoreOps(basicOps *BasicOps) *ScoreOps {
	return &ScoreOps{
		basicOps: basicOps,
	}
}

// ZScore returns the score of a member
func (s *ScoreOps) zScore(key string, member string) (float64, bool) {
	return s.basicOps.ZScore(key, member)
}

// ZIncrBy increments the score of a member
func (s *ScoreOps) ZIncrBy(key string, increment float64, member string) (float64, error) {
	value, _ := s.basicOps.cache.LoadOrStore(key, &sync.Map{})
	zset := value.(*sync.Map)

	var newScore float64
	zsetUpdate := sync.Mutex{}
	zsetUpdate.Lock()
	defer zsetUpdate.Unlock()

	// Get current score or initialize to 0
	currentValue, _ := zset.LoadOrStore(member, float64(0))
	currentScore := currentValue.(float64)
	newScore = currentScore + increment

	// Store new score
	zset.Store(member, newScore)
	s.basicOps.incrementKeyVersion(key)

	return newScore, nil
}

// ZMScore returns the scores of multiple members
func (s *ScoreOps) ZMScore(key string, members ...string) []float64 {
	scores := make([]float64, len(members))
	for i, member := range members {
		if score, exists := s.zScore(key, member); exists {
			scores[i] = score
		} else {
			scores[i] = -1 // Sentinel value for non-existent members
		}
	}
	return scores
}

// ZCount returns the number of members with score in the given range
func (s *ScoreOps) ZCount(key string, min, max float64) int {
	value, exists := s.basicOps.cache.Load(key)
	if !exists {
		return 0
	}

	set := value.(*sync.Map)
	count := 0

	set.Range(func(_, score interface{}) bool {
		if s, ok := score.(float64); ok {
			if s >= min && s <= max {
				count++
			}
		}
		return true
	})

	return count
}

// Helper methods

// compareAndSwapScore atomically updates score if it hasn't changed
func (s *ScoreOps) compareAndSwapScore(key string, member string, oldScore, newScore float64) bool {
	value, exists := s.basicOps.cache.Load(key)
	if !exists {
		return false
	}

	set := value.(*sync.Map)
	return set.CompareAndSwap(member, oldScore, newScore)
}

// batchUpdateScores updates multiple scores atomically
func (s *ScoreOps) batchUpdateScores(key string, updates map[string]float64) error {
	value, _ := s.basicOps.cache.LoadOrStore(key, &sync.Map{})
	set := value.(*sync.Map)

	mutex := sync.Mutex{}
	mutex.Lock()
	defer mutex.Unlock()

	for member, score := range updates {
		set.Store(member, score)
	}

	s.basicOps.incrementKeyVersion(key)
	return nil
}

func (r *RangeOps) ZRangeByScore(key string, min, max float64) []string {
	members := r.basicOps.getSortedMembers(key)
	result := make([]string, 0, len(members))

	for _, member := range members {
		if r.isInScoreRange(member.Score, min, max) {
			result = append(result, member.Member)
		}
	}
	return result
}

func (r *RangeOps) ZRangeByScoreWithScores(key string, min, max float64) []models.ZSetMember {
	members := r.basicOps.getSortedMembers(key)
	result := make([]models.ZSetMember, 0, len(members))

	for _, member := range members {
		if r.isInScoreRange(member.Score, min, max) {
			result = append(result, member)
		}
	}
	return result
}

func (r *RangeOps) ZRevRangeByScore(key string, max, min float64) []string {
	members := r.ZRangeByScore(key, min, max)

	// Reverse order
	for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
		members[i], members[j] = members[j], members[i]
	}

	return members
}

// Helper method
func (r *RangeOps) isInScoreRange(score, min, max float64) bool {
	return score >= min && score <= max
}
