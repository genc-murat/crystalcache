package zset

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// BasicOps handles basic operations for sorted sets
type BasicOps struct {
	cache       *sync.Map // Main cache map for zsets
	keyVersions *sync.Map
	data        map[string][]models.ZSetMember
	mu          sync.Mutex
}

// NewBasicOps creates a new BasicOps instance
func NewBasicOps(cache *sync.Map, keyVersions *sync.Map) *BasicOps {
	return &BasicOps{
		cache:       cache,
		keyVersions: keyVersions,
	}
}

// Del deletes a key from the cache.
func (b *BasicOps) Del(key string) error {
	b.cache.Delete(key)
	return nil
}

// ZAdd adds a member with score to a sorted set
func (b *BasicOps) ZAdd(key string, score float64, member string) error {
	// Load or initialize the zset
	actual, _ := b.cache.LoadOrStore(key, &sync.Map{})

	// Assert that the value is a *sync.Map
	actualZSet, ok := actual.(*sync.Map)
	if !ok {
		return fmt.Errorf("invalid value type for key: %s", key)
	}

	// Add or update the member's score
	actualZSet.Store(member, score)

	// Increment the key version to reflect the change
	b.incrementKeyVersion(key)

	return nil
}

// ZCard returns the number of members in a sorted set
func (b *BasicOps) ZCard(key string) int {
	// Attempt to load the key from the cache
	value, exists := b.cache.Load(key)
	if !exists {
		// Key does not exist, so the cardinality is 0
		return 0
	}

	// Assert the value is of type *sync.Map
	set, ok := value.(*sync.Map)
	if !ok {
		// Handle unexpected type gracefully by returning 0
		return 0
	}

	// Initialize count variable
	count := 0

	// Iterate over the sync.Map to count entries
	set.Range(func(_, _ interface{}) bool {
		count++
		return true // Continue iteration
	})

	return count
}

// ZScore returns the score of a member in a sorted set
func (b *BasicOps) ZScore(key string, member string) (float64, bool) {
	value, exists := b.cache.Load(key)
	if !exists {
		return 0, false // Key not found
	}

	set, ok := value.(*sync.Map)
	if !ok {
		return 0, false // Invalid cache value type
	}

	memberValue, exists := set.Load(member)
	if !exists {
		return 0, false // Member not found
	}

	score, ok := memberValue.(float64)
	if !ok {
		return 0, false // Invalid member value type
	}

	return score, true
}

// ZRem removes a member from a sorted set
func (b *BasicOps) ZRem(key string, member string) error {
	value, exists := b.cache.Load(key)
	if !exists {
		return nil // Key doesn't exist, nothing to remove
	}

	set, ok := value.(*sync.Map)
	if !ok {
		return fmt.Errorf("invalid cache value type for key: %s", key)
	}

	set.Delete(member)

	isEmpty := true
	set.Range(func(_, _ interface{}) bool {
		isEmpty = false
		return false // Stop on the first element
	})

	if isEmpty {
		b.cache.Delete(key)
	}

	b.incrementKeyVersion(key)
	return nil
}

// incrementKeyVersion metodu da keyVersions kullanacak şekilde güncellendi
func (b *BasicOps) incrementKeyVersion(key string) {
	for {
		// Attempt to load the current version or initialize it to 0
		oldVersionI, _ := b.keyVersions.LoadOrStore(key, int64(0))
		oldVersion := oldVersionI.(int64)

		// Atomically compare and swap the value to increment it
		if b.keyVersions.CompareAndSwap(key, oldVersion, oldVersion+1) {
			return
		}

		// If CAS fails, loop again to retry
	}
}

// getSortedMembers retrieves and sorts members of a sorted set from the cache.
func (b *BasicOps) getSortedMembers(key string) ([]models.ZSetMember, error) {
	// Attempt to retrieve the value from the cache
	value, exists := b.cache.Load(key)
	if !exists {
		// Return an empty slice and no error if the key does not exist in the cache.
		// Consider logging this for debugging purposes.
		return []models.ZSetMember{}, nil
	}

	// Assert that the value is of the expected type *sync.Map
	set, ok := value.(*sync.Map)
	if !ok {
		// Return an empty slice and an error indicating the type mismatch.
		return []models.ZSetMember{}, fmt.Errorf("unexpected type for key '%s' in cache: got %T, expected *sync.Map", key, value)
	}

	// Initialize a slice to hold the members
	var members []models.ZSetMember

	// Iterate over the sync.Map to collect members
	set.Range(func(member, score interface{}) bool {
		// Perform type assertions for member and score
		memberStr, memberOk := member.(string)
		scoreFloat, scoreOk := score.(float64)
		if memberOk && scoreOk {
			members = append(members, models.ZSetMember{
				Member: memberStr,
				Score:  scoreFloat,
			})
		} else {
			// Handle cases where the type is incorrect within the sync.Map.
			// Consider logging this as it indicates a potential data integrity issue.
			log.Printf("warning: invalid data type in sorted set '%s': member=%T, score=%T", key, member, score)
		}
		return true
	})

	// Sort members lexicographically by the `Member` field
	sort.Slice(members, func(i, j int) bool {
		return members[i].Member < members[j].Member
	})

	return members, nil
}

func (b *BasicOps) getLexSortedMembers(key string) []models.ZSetMember {
	// Attempt to retrieve the value from the cache
	value, exists := b.cache.Load(key)
	if !exists {
		// Return an empty slice if the key doesn't exist
		return []models.ZSetMember{}
	}

	// Assert the value is of type *sync.Map
	set, ok := value.(*sync.Map)
	if !ok {
		// Handle unexpected type gracefully
		return []models.ZSetMember{}
	}

	// Initialize a slice to store the members
	var members []models.ZSetMember

	// Iterate over the sync.Map and collect members
	set.Range(func(member, score interface{}) bool {
		// Type assertions for member and score
		memberStr, memberOk := member.(string)
		scoreFloat, scoreOk := score.(float64)
		if memberOk && scoreOk {
			members = append(members, models.ZSetMember{
				Member: memberStr,
				Score:  scoreFloat,
			})
		}
		return true
	})

	// Sort the members lexicographically by their `Member` field
	sort.Slice(members, func(i, j int) bool {
		return members[i].Member < members[j].Member
	})

	return members
}

func (b *BasicOps) ZRandMember(key string, count int, withScores bool) []models.ZSetMember {
	members, err := b.getSortedMembers(key)
	if err != nil || len(members) == 0 || count == 0 {
		return []models.ZSetMember{}
	}

	rand.Seed(time.Now().UnixNano()) // Seed the random number generator once

	absCount := count
	allowDuplicates := count < 0
	if allowDuplicates {
		absCount = -count
	}

	numMembers := len(members)

	if allowDuplicates {
		result := make([]models.ZSetMember, absCount)
		for i := 0; i < absCount; i++ {
			result[i] = members[rand.Intn(numMembers)]
		}
		return result
	}

	// Optimization: If count is greater than or equal to the number of members,
	// and we don't allow duplicates, we can just return all members in a random order.
	if absCount >= numMembers {
		rand.Shuffle(numMembers, func(i, j int) {
			members[i], members[j] = members[j], members[i]
		})
		return members
	}

	// Select unique random members efficiently
	result := make([]models.ZSetMember, absCount)
	seenIndices := make(map[int]bool)
	for i := 0; i < absCount; i++ {
		randomIndex := rand.Intn(numMembers)
		for seenIndices[randomIndex] {
			randomIndex = rand.Intn(numMembers)
		}
		result[i] = members[randomIndex]
		seenIndices[randomIndex] = true
	}

	return result
}

// ZRandMemberWithoutScores returns random members without their scores
func (b *BasicOps) ZRandMemberWithoutScores(key string, count int) []string {
	membersWithScores := b.ZRandMember(key, count, false)

	result := make([]string, len(membersWithScores))

	for i, member := range membersWithScores {
		result[i] = member.Member
	}

	return result
}

func (b *BasicOps) setSortedMembersIf(key string, newMembers, oldMembers []models.ZSetMember) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !reflect.DeepEqual(b.data[key], oldMembers) {
		return false
	}
	b.data[key] = newMembers
	return true
}
