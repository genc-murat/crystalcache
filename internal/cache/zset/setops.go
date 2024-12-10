package zset

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type SetOps struct {
	basicOps *BasicOps
}

func NewSetOps(basicOps *BasicOps) *SetOps {
	return &SetOps{
		basicOps: basicOps,
	}
}

// ZUnion returns the union of multiple sorted sets
func (s *SetOps) ZUnion(keys ...string) []models.ZSetMember {
	if len(keys) == 0 {
		return []models.ZSetMember{}
	}

	unionMap := make(map[string]float64)

	for _, key := range keys {
		members := s.basicOps.getSortedMembers(key)
		for _, member := range members {
			if existingScore, ok := unionMap[member.Member]; ok {
				unionMap[member.Member] = math.Max(existingScore, member.Score)
			} else {
				unionMap[member.Member] = member.Score
			}
		}
	}

	// Convert map to sorted slice
	result := make([]models.ZSetMember, 0, len(unionMap))
	for member, score := range unionMap {
		result = append(result, models.ZSetMember{
			Member: member,
			Score:  score,
		})
	}

	s.sortMembers(result)
	return result
}

// ZUnionStore stores the union of sets in destination
func (s *SetOps) ZUnionStore(destination string, keys []string, weights []float64) (int, error) {
	if len(keys) == 0 {
		return 0, errors.New("ERR at least 1 input key is needed")
	}

	if weights == nil {
		weights = make([]float64, len(keys))
		for i := range weights {
			weights[i] = 1
		}
	}
	if len(weights) != len(keys) {
		return 0, errors.New("ERR weights length must match keys length")
	}

	// Calculate weighted union
	unionMap := make(map[string]float64)

	for i, key := range keys {
		members := s.basicOps.getSortedMembers(key)
		for _, member := range members {
			weightedScore := member.Score * weights[i]
			if existingScore, ok := unionMap[member.Member]; ok {
				unionMap[member.Member] = existingScore + weightedScore
			} else {
				unionMap[member.Member] = weightedScore
			}
		}
	}

	// Store results
	for member, score := range unionMap {
		if err := s.basicOps.ZAdd(destination, score, member); err != nil {
			return 0, err
		}
	}

	return len(unionMap), nil
}

// ZInter returns the intersection of multiple sorted sets
func (s *SetOps) ZInter(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	// Get first set members
	members := s.basicOps.getSortedMembers(keys[0])
	if len(members) == 0 {
		return []string{}
	}

	// Create map from first set
	result := make(map[string]bool)
	for _, member := range members {
		result[member.Member] = true
	}

	// Intersect with other sets
	for _, key := range keys[1:] {
		nextMembers := s.basicOps.getSortedMembers(key)
		currentResult := make(map[string]bool)

		for _, member := range nextMembers {
			if result[member.Member] {
				currentResult[member.Member] = true
			}
		}

		result = currentResult
	}

	// Convert to sorted slice
	intersection := make([]string, 0, len(result))
	for member := range result {
		intersection = append(intersection, member)
	}

	return intersection
}

// ZInterStore stores intersection of sets in destination
func (s *SetOps) ZInterStore(destination string, keys []string, weights []float64) (int, error) {
	if len(keys) == 0 {
		return 0, errors.New("ERR at least 1 input key is needed")
	}

	if weights == nil {
		weights = make([]float64, len(keys))
		for i := range weights {
			weights[i] = 1
		}
	}
	if len(weights) != len(keys) {
		return 0, errors.New("ERR weights length must match keys length")
	}

	// Get first set
	firstMembers := s.basicOps.getSortedMembers(keys[0])
	if len(firstMembers) == 0 {
		s.basicOps.cache.Delete(destination)
		return 0, nil
	}

	// Initialize intersection map with first set
	intersection := make(map[string]float64)
	for _, member := range firstMembers {
		intersection[member.Member] = member.Score * weights[0]
	}

	// Intersect with remaining sets
	for i := 1; i < len(keys); i++ {
		currentMembers := s.basicOps.getSortedMembers(keys[i])
		tempIntersection := make(map[string]float64)

		for _, member := range currentMembers {
			if score, exists := intersection[member.Member]; exists {
				tempIntersection[member.Member] = score + (member.Score * weights[i])
			}
		}

		intersection = tempIntersection
	}

	// Store results
	for member, score := range intersection {
		if err := s.basicOps.ZAdd(destination, score, member); err != nil {
			return 0, err
		}
	}

	return len(intersection), nil
}

// ZDiff returns the set difference between the first and subsequent sets
func (s *SetOps) ZDiff(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	firstMembers := s.basicOps.getSortedMembers(keys[0])
	result := make(map[string]bool)
	for _, member := range firstMembers {
		result[member.Member] = true
	}

	for _, key := range keys[1:] {
		members := s.basicOps.getSortedMembers(key)
		for _, member := range members {
			delete(result, member.Member)
		}
	}

	// Convert to sorted slice
	diff := make([]string, 0, len(result))
	for member := range result {
		diff = append(diff, member)
	}

	return diff
}

// ZInterCard returns the number of members in the intersection of multiple sets
func (s *SetOps) ZInterCard(keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, errors.New("ERR wrong number of arguments for 'zintercard' command")
	}

	members := s.ZInter(keys...)
	return len(members), nil
}

// ZDiffStore stores the difference from first set to others in destination
func (s *SetOps) ZDiffStore(destination string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, errors.New("ERR wrong number of arguments for 'zdiffstore' command")
	}

	// Get members and scores from first set
	firstSetI, exists := s.basicOps.cache.Load(keys[0])
	if !exists {
		s.basicOps.cache.Delete(destination)
		return 0, nil
	}
	firstSet := firstSetI.(*sync.Map)

	// Create temporary map for result
	resultMap := &sync.Map{}

	// Copy members and scores from first set
	firstSet.Range(func(member, score interface{}) bool {
		resultMap.Store(member, score)
		return true
	})

	// Remove members that exist in other sets
	for _, key := range keys[1:] {
		if setI, exists := s.basicOps.cache.Load(key); exists {
			set := setI.(*sync.Map)
			set.Range(func(member, _ interface{}) bool {
				resultMap.Delete(member)
				return true
			})
		}
	}

	// Store result in destination
	destinationSet := &sync.Map{}
	var count int
	resultMap.Range(func(member, score interface{}) bool {
		destinationSet.Store(member, score)
		count++
		return true
	})
	s.basicOps.cache.Store(destination, destinationSet)

	// Increment version
	s.basicOps.incrementKeyVersion(destination)

	return count, nil
}

// Helper methods
func (s *SetOps) sortMembers(members []models.ZSetMember) {
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score == members[j].Score {
			return members[i].Member < members[j].Member
		}
		return members[i].Score < members[j].Score
	})
}
