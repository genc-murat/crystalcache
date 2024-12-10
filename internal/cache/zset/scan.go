package zset

import (
	"errors"
	"sort"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/pkg/utils/pattern"
)

type ScanOps struct {
	basicOps *BasicOps
}

func NewScanOps(basicOps *BasicOps) *ScanOps {
	return &ScanOps{
		basicOps: basicOps,
	}
}

// ZScan iterates over members in a sorted set
func (s *ScanOps) ZScan(key string, cursor int, match string, count int) ([]models.ZSetMember, int) {
	// Get all members first
	members := s.basicOps.getSortedMembers(key)
	if len(members) == 0 {
		return []models.ZSetMember{}, 0
	}

	// Apply pattern matching and collect matches
	var matches []models.ZSetMember
	for _, member := range members {
		if pattern.Match(match, member.Member) {
			matches = append(matches, member)
		}
	}

	// Sort for consistent iteration
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Score == matches[j].Score {
			return matches[i].Member < matches[j].Member
		}
		return matches[i].Score < matches[j].Score
	})

	// Handle cursor
	if cursor >= len(matches) {
		return []models.ZSetMember{}, 0
	}

	// Determine end position
	end := cursor + count
	if end > len(matches) {
		end = len(matches)
	}

	// Calculate next cursor
	nextCursor := end
	if nextCursor >= len(matches) {
		nextCursor = 0
	}

	return matches[cursor:end], nextCursor
}

// Helper methods

// adjustCursor ensures cursor is within valid range
func (s *ScanOps) adjustCursor(cursor, length int) int {
	if cursor < 0 {
		return 0
	}
	if cursor >= length {
		return 0
	}
	return cursor
}

// batchScan processes multiple scan operations in parallel
func (s *ScanOps) batchScan(keys []string, cursor int, match string, count int) map[string][]models.ZSetMember {
	result := make(map[string][]models.ZSetMember)

	for _, key := range keys {
		members, _ := s.ZScan(key, cursor, match, count)
		if len(members) > 0 {
			result[key] = members
		}
	}

	return result
}

// validateScanParams validates scan parameters
func (s *ScanOps) validateScanParams(cursor, count int) error {
	if cursor < 0 {
		return errors.New("ERR invalid cursor")
	}
	if count <= 0 {
		return errors.New("ERR count should be positive")
	}
	return nil
}
