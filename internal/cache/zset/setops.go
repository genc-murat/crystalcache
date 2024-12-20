package zset

import (
	"errors"
	"fmt"
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
func (s *SetOps) ZUnion(keys ...string) ([]models.ZSetMember, error) {
	if len(keys) == 0 {
		return []models.ZSetMember{}, nil
	}

	unionMap := make(map[string]float64)

	for _, key := range keys {
		members, err := s.basicOps.getSortedMembers(key)
		if err != nil {
			// Handle the error appropriately. Consider returning the error
			// to the caller or logging it and continuing with other keys.
			// For now, let's return the error to the caller.
			return nil, fmt.Errorf("error getting sorted members for key '%s': %w", key, err)
		}
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

	// Assuming s.sortMembers sorts by score (descending) then by member (ascending)
	s.sortMembers(result)
	return result, nil
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
		members, err := s.basicOps.getSortedMembers(key)
		if err != nil {
			// Return the error encountered while getting members.
			return 0, fmt.Errorf("error getting sorted members for key '%s': %w", key, err)
		}

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

	// Get members of the first set
	members, err := s.basicOps.getSortedMembers(keys[0])
	if err != nil {
		// Handle the error appropriately, e.g., log it or return an error.
		// For now, returning an empty slice if the first set cannot be retrieved.
		return []string{}
	}
	if len(members) == 0 {
		return []string{}
	}

	// Create map from the first set for efficient lookup
	result := make(map[string]bool)
	for _, member := range members {
		result[member.Member] = true
	}

	// Intersect with other sets
	for _, key := range keys[1:] {
		nextMembers, err := s.basicOps.getSortedMembers(key)
		if err != nil {
			// Handle the error appropriately. If retrieving members for a subsequent set fails,
			// the intersection will be based on the successfully retrieved sets so far.
			// You might want to log this error.
			continue // Skip this key and proceed with the next
		}

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

	sort.Strings(intersection) // Sort the intersection lexicographically
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

	// Clear the destination key before storing new results
	err := s.basicOps.Del(destination)
	if err != nil && !errors.Is(err, errors.New("key does not exist")) {
		return 0, fmt.Errorf("error clearing destination key '%s': %w", destination, err)
	}

	// Get the first set
	firstMembers, err := s.basicOps.getSortedMembers(keys[0])
	if err != nil {
		return 0, fmt.Errorf("error getting sorted members for key '%s': %w", keys[0], err)
	}
	if len(firstMembers) == 0 {
		// If the first set is empty, the intersection is empty.
		return 0, nil
	}

	// Initialize intersection map with members from the first set
	intersection := make(map[string]float64)
	for _, member := range firstMembers {
		intersection[member.Member] = member.Score * weights[0]
	}

	// Intersect with remaining sets
	for i := 1; i < len(keys); i++ {
		currentMembers, err := s.basicOps.getSortedMembers(keys[i])
		if err != nil {
			return 0, fmt.Errorf("error getting sorted members for key '%s': %w", keys[i], err)
		}

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
			return 0, fmt.Errorf("error adding member '%s' with score %f to destination '%s': %w", member, score, destination, err)
		}
	}

	return len(intersection), nil
}

// ZDiff returns the set difference between the first and subsequent sets
func (s *SetOps) ZDiff(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	// Get members of the first set
	firstMembers, err := s.basicOps.getSortedMembers(keys[0])
	if err != nil {
		// Handle the error appropriately. Returning an empty slice for now.
		return []string{}
	}

	result := make(map[string]bool)
	for _, member := range firstMembers {
		result[member.Member] = true
	}

	// Calculate the difference with subsequent sets
	for _, key := range keys[1:] {
		members, err := s.basicOps.getSortedMembers(key)
		if err != nil {
			// Handle the error appropriately. Skipping this key for now.
			continue
		}
		for _, member := range members {
			delete(result, member.Member)
		}
	}

	// Convert to sorted slice
	diff := make([]string, 0, len(result))
	for member := range result {
		diff = append(diff, member)
	}

	sort.Strings(diff) // Sort the difference lexicographically
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
