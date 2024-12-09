package cache

import (
	"errors"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/pkg/utils/pattern"
)

func (c *MemoryCache) ZRangeByScore(key string, min, max float64) []string {
	members := c.getSortedMembers(key)
	result := make([]string, 0)

	for _, member := range members {
		if member.Score >= min && member.Score <= max {
			result = append(result, member.Member)
		}
	}
	return result
}

func (c *MemoryCache) ZRank(key string, member string) (int, bool) {
	members := c.getSortedMembers(key)
	for i, m := range members {
		if m.Member == member {
			return i, true
		}
	}
	return 0, false
}

func (c *MemoryCache) ZRem(key string, member string) error {
	value, exists := c.zsets.Load(key)
	if !exists {
		return nil // Key yoksa işlem yapmaya gerek yok
	}

	set := value.(*sync.Map)
	set.Delete(member) // Üyeyi sil

	// Eğer set boşsa, key'i tamamen kaldır
	empty := true
	set.Range(func(_, _ interface{}) bool {
		empty = false
		return false // İlk eleman bulunduğunda döngü sonlanır
	})

	if empty {
		c.zsets.Delete(key)
	}

	return nil
}

func (c *MemoryCache) ZScore(key string, member string) (float64, bool) {
	// `key` için `sync.Map` yüklenir
	value, exists := c.zsets.Load(key)
	if !exists {
		return 0, false
	}

	set := value.(*sync.Map) // `sync.Map` olarak dönüştür
	// `member` için skor kontrol edilir
	memberValue, exists := set.Load(member)
	if !exists {
		return 0, false
	}

	return memberValue.(float64), true
}

func (c *MemoryCache) ZCard(key string) int {
	// `key` için `sync.Map` yüklenir
	value, exists := c.zsets.Load(key)
	if !exists {
		return 0 // Eğer key yoksa, 0 döndür
	}

	set := value.(*sync.Map) // `sync.Map` olarak dönüştür
	count := 0

	// `sync.Map` içindeki elemanları say
	set.Range(func(_, _ interface{}) bool {
		count++
		return true // Tüm elemanları kontrol etmeye devam et
	})

	return count
}

func (c *MemoryCache) ZCount(key string, min, max float64) int {
	value, exists := c.zsets.Load(key)
	if !exists {
		return 0 // Eğer key yoksa, sıfır döndür
	}

	set := value.(*sync.Map) // `sync.Map` olarak dönüştür
	count := 0

	// `sync.Map` içindeki elemanları iterate et
	set.Range(func(_, score interface{}) bool {
		if s, ok := score.(float64); ok {
			if s >= min && s <= max {
				count++
			}
		}
		return true // Tüm elemanları kontrol etmeye devam et
	})

	return count
}

func (c *MemoryCache) ZRange(key string, start, stop int) []string {
	members := c.getSortedMembers(key)
	if len(members) == 0 {
		return []string{}
	}

	if start < 0 {
		start = len(members) + start
	}
	if stop < 0 {
		stop = len(members) + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= len(members) {
		stop = len(members) - 1
	}
	if start > stop {
		return []string{}
	}

	result := make([]string, stop-start+1)
	for i := start; i <= stop; i++ {
		result[i-start] = members[i].Member
	}
	return result
}

func (c *MemoryCache) ZRangeWithScores(key string, start, stop int) []models.ZSetMember {
	members := c.getSortedMembers(key)
	if len(members) == 0 {
		return []models.ZSetMember{}
	}

	if start < 0 {
		start = len(members) + start
	}
	if stop < 0 {
		stop = len(members) + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= len(members) {
		stop = len(members) - 1
	}
	if start > stop {
		return []models.ZSetMember{}
	}

	return members[start : stop+1]
}

func (c *MemoryCache) ZAdd(key string, score float64, member string) error {
	var zset sync.Map
	actual, _ := c.zsets.LoadOrStore(key, &zset)
	actualZSet := actual.(*sync.Map)
	actualZSet.Store(member, score)
	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) getSortedMembers(key string) []models.ZSetMember {
	// `key` için `sync.Map` yüklenir
	value, exists := c.zsets.Load(key)
	if !exists {
		return []models.ZSetMember{}
	}

	set := value.(*sync.Map) // `sync.Map` olarak dönüştür

	// Üyeleri toplamak için slice kullanılır
	members := zsetMemberPool.Get().([]models.ZSetMember)
	members = members[:0]

	// `sync.Map` içindeki elemanları iterate et
	set.Range(func(member, score interface{}) bool {
		members = append(members, models.ZSetMember{
			Member: member.(string),
			Score:  score.(float64),
		})
		return true
	})

	// Skorlara göre sıralama yapılır
	sort.Slice(members, func(i, j int) bool {
		if members[i].Score == members[j].Score {
			return members[i].Member < members[j].Member
		}
		return members[i].Score < members[j].Score
	})

	// Sonuç slice'ını oluştur ve geri dön
	result := make([]models.ZSetMember, len(members))
	copy(result, members)

	// Slice'ı havuza geri koy
	zsetMemberPool.Put(members)

	return result
}

func (c *MemoryCache) ZRevRange(key string, start, stop int) []string {
	members := c.getSortedMembers(key)
	if len(members) == 0 {
		return []string{}
	}

	for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
		members[i], members[j] = members[j], members[i]
	}

	if start < 0 {
		start = len(members) + start
	}
	if stop < 0 {
		stop = len(members) + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= len(members) {
		stop = len(members) - 1
	}
	if start > stop {
		return []string{}
	}

	result := make([]string, stop-start+1)
	for i := start; i <= stop; i++ {
		result[i-start] = members[i].Member
	}
	return result
}

func (c *MemoryCache) ZRevRangeWithScores(key string, start, stop int) []models.ZSetMember {
	members := c.getSortedMembers(key)
	if len(members) == 0 {
		return []models.ZSetMember{}
	}

	for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
		members[i], members[j] = members[j], members[i]
	}

	if start < 0 {
		start = len(members) + start
	}
	if stop < 0 {
		stop = len(members) + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= len(members) {
		stop = len(members) - 1
	}
	if start > stop {
		return []models.ZSetMember{}
	}

	return members[start : stop+1]
}

func (c *MemoryCache) ZIncrBy(key string, increment float64, member string) (float64, error) {
	// `sync.Map` ile zset'i yükle veya yeni bir tane oluştur
	value, _ := c.zsets.LoadOrStore(key, &sync.Map{})
	zset, _ := value.(*sync.Map) // Türü `*sync.Map` olarak belirle

	var newScore float64

	// ZSet içinde ilgili `member` puanını güncelle
	zset.LoadOrStore(member, float64(0)) // Eğer yoksa 0 olarak başlat
	zsetUpdate := sync.Mutex{}           // Lokal bir mutex
	zsetUpdate.Lock()
	defer zsetUpdate.Unlock()

	// Mevcut skoru al ve yeni skoru hesapla
	if currentValue, ok := zset.Load(member); ok {
		currentScore := currentValue.(float64)
		newScore = currentScore + increment
	} else {
		newScore = increment
	}

	// Yeni skoru sakla
	zset.Store(member, newScore)

	return newScore, nil
}

func (c *MemoryCache) ZRangeByScoreWithScores(key string, min, max float64) []models.ZSetMember {
	members := c.getSortedMembers(key)
	result := make([]models.ZSetMember, 0)

	for _, member := range members {
		if member.Score >= min && member.Score <= max {
			result = append(result, member)
		}
	}
	return result
}

func (c *MemoryCache) ZInterStore(destination string, keys []string, weights []float64) (int, error) {
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

	var intersection sync.Map

	// İlk seti yükle ve işlem başlat
	firstSetValue, exists := c.zsets.Load(keys[0])
	if !exists {
		return 0, nil
	}
	firstSet := firstSetValue.(*sync.Map)

	firstSet.Range(func(member, score interface{}) bool {
		memberStr := member.(string)
		scoreFloat := score.(float64) * weights[0]
		intersection.Store(memberStr, scoreFloat)
		return true
	})

	// Geriye kalan setlerle kesişim işlemi yap
	for i := 1; i < len(keys); i++ {
		setValue, exists := c.zsets.Load(keys[i])
		if !exists {
			return 0, nil
		}
		set := setValue.(*sync.Map)

		tempIntersection := &sync.Map{} // Yeni bir sync.Map oluştur
		intersection.Range(func(member, existingScore interface{}) bool {
			memberStr := member.(string)
			if scoreValue, ok := set.Load(memberStr); ok {
				newScore := existingScore.(float64) + (scoreValue.(float64) * weights[i])
				tempIntersection.Store(memberStr, newScore)
			}
			return true
		})

		// Yeni intersection'ı tempIntersection ile değiştir
		intersection = *tempIntersection
	}

	// Sonuçları hedef sete kaydet
	destinationSet := &sync.Map{}
	intersection.Range(func(member, score interface{}) bool {
		destinationSet.Store(member, score)
		return true
	})
	c.zsets.Store(destination, destinationSet)

	// Kesim eleman sayısını döndür
	count := 0
	intersection.Range(func(_, _ interface{}) bool {
		count++
		return true
	})

	return count, nil
}

func (c *MemoryCache) ZUnionStore(destination string, keys []string, weights []float64) (int, error) {
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

	var union sync.Map

	for i, key := range keys {
		setValue, exists := c.zsets.Load(key)
		if !exists {
			continue
		}
		set := setValue.(*sync.Map)

		set.Range(func(member, score interface{}) bool {
			memberStr := member.(string)
			scoreFloat := score.(float64) * weights[i]

			if existingValue, ok := union.Load(memberStr); ok {
				union.Store(memberStr, existingValue.(float64)+scoreFloat)
			} else {
				union.Store(memberStr, scoreFloat)
			}
			return true
		})
	}

	// Hedef seti oluştur ve `union` sonuçlarını kaydet
	destinationSet := &sync.Map{}
	union.Range(func(member, score interface{}) bool {
		destinationSet.Store(member, score)
		return true
	})
	c.zsets.Store(destination, destinationSet)

	// Toplam eleman sayısını hesapla
	count := 0
	union.Range(func(_, _ interface{}) bool {
		count++
		return true
	})

	return count, nil
}

// ZDiff returns the members that exist in the first set but not in the subsequent sets
func (c *MemoryCache) ZDiff(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	// Get first set
	firstSetI, exists := c.zsets.Load(keys[0])
	if !exists {
		return []string{}
	}
	firstSet := firstSetI.(*sync.Map)

	// Create result map to track members
	result := make(map[string]bool)
	firstSet.Range(func(member, _ interface{}) bool {
		result[member.(string)] = true
		return true
	})

	// Remove members that exist in other sets
	for _, key := range keys[1:] {
		if setI, exists := c.zsets.Load(key); exists {
			set := setI.(*sync.Map)
			set.Range(func(member, _ interface{}) bool {
				delete(result, member.(string))
				return true
			})
		}
	}

	// Convert result to sorted slice
	diff := make([]string, 0, len(result))
	for member := range result {
		diff = append(diff, member)
	}
	sort.Strings(diff)
	return diff
}

// ZDiffStore stores the difference of the sets in a new set at destination
func (c *MemoryCache) ZDiffStore(destination string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, errors.New("ERR wrong number of arguments for 'zdiffstore' command")
	}

	// Get members and scores from first set
	firstSetI, exists := c.zsets.Load(keys[0])
	if !exists {
		c.zsets.Delete(destination)
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
		if setI, exists := c.zsets.Load(key); exists {
			set := setI.(*sync.Map)
			set.Range(func(member, _ interface{}) bool {
				resultMap.Delete(member)
				return true
			})
		}
	}

	// Store result in destination
	c.zsets.Store(destination, resultMap)

	// Count members in result
	count := 0
	resultMap.Range(func(_, _ interface{}) bool {
		count++
		return true
	})

	c.incrementKeyVersion(destination)
	return count, nil
}

// ZInter returns the members that exist in all the sets
func (c *MemoryCache) ZInter(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	// Get first set
	firstSetI, exists := c.zsets.Load(keys[0])
	if !exists {
		return []string{}
	}
	firstSet := firstSetI.(*sync.Map)

	// Create result map to track members
	result := make(map[string]bool)
	firstSet.Range(func(member, _ interface{}) bool {
		result[member.(string)] = true
		return true
	})

	// Keep only members that exist in all sets
	for _, key := range keys[1:] {
		if setI, exists := c.zsets.Load(key); exists {
			set := setI.(*sync.Map)
			tempResult := make(map[string]bool)

			set.Range(func(member, _ interface{}) bool {
				memberStr := member.(string)
				if result[memberStr] {
					tempResult[memberStr] = true
				}
				return true
			})

			result = tempResult
		} else {
			return []string{} // If any set doesn't exist, return empty result
		}
	}

	// Convert result to sorted slice
	intersection := make([]string, 0, len(result))
	for member := range result {
		intersection = append(intersection, member)
	}
	sort.Strings(intersection)
	return intersection
}

// ZInterCard returns the number of members in the intersection of the sets
func (c *MemoryCache) ZInterCard(keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, errors.New("ERR wrong number of arguments for 'zintercard' command")
	}

	members := c.ZInter(keys...)
	return len(members), nil
}

// ZLexCount returns the number of elements in the sorted set between min and max lexicographical range
func (c *MemoryCache) ZLexCount(key, min, max string) (int, error) {
	setI, exists := c.zsets.Load(key)
	if !exists {
		return 0, nil
	}
	set := setI.(*sync.Map)

	// Parse range specifications
	minInclusive := true
	maxInclusive := true
	if strings.HasPrefix(min, "(") {
		minInclusive = false
		min = min[1:]
	} else if strings.HasPrefix(min, "[") {
		min = min[1:]
	}
	if strings.HasPrefix(max, "(") {
		maxInclusive = false
		max = max[1:]
	} else if strings.HasPrefix(max, "[") {
		max = max[1:]
	}

	// Special cases for infinity
	minIsInf := min == "-"
	maxIsInf := max == "+"

	count := 0
	set.Range(func(member, _ interface{}) bool {
		memberStr := member.(string)

		// Check if member is within range
		if minIsInf || (minInclusive && memberStr >= min) || (!minInclusive && memberStr > min) {
			if maxIsInf || (maxInclusive && memberStr <= max) || (!maxInclusive && memberStr < max) {
				count++
			}
		}
		return true
	})

	return count, nil
}

func (c *MemoryCache) ZRangeByLex(key string, min, max string) []string {
	setI, exists := c.zsets.Load(key)
	if !exists {
		return []string{}
	}
	set := setI.(*sync.Map)

	// Parse range specifications
	minInclusive := true
	maxInclusive := true
	if strings.HasPrefix(min, "(") {
		minInclusive = false
		min = min[1:]
	} else if strings.HasPrefix(min, "[") {
		min = min[1:]
	}
	if strings.HasPrefix(max, "(") {
		maxInclusive = false
		max = max[1:]
	} else if strings.HasPrefix(max, "[") {
		max = max[1:]
	}

	// Special cases for infinity
	minIsInf := min == "-"
	maxIsInf := max == "+"

	// Collect matching members
	var members []string
	set.Range(func(member, _ interface{}) bool {
		memberStr := member.(string)

		// Check if member is within range
		if minIsInf || (minInclusive && memberStr >= min) || (!minInclusive && memberStr > min) {
			if maxIsInf || (maxInclusive && memberStr <= max) || (!maxInclusive && memberStr < max) {
				members = append(members, memberStr)
			}
		}
		return true
	})

	// Sort lexicographically
	sort.Strings(members)
	return members
}

func (c *MemoryCache) ZRangeStore(destination string, source string, start, stop int, withScores bool) (int, error) {
	// Get source members in range
	var members []models.ZSetMember
	if withScores {
		members = c.ZRangeWithScores(source, start, stop)
	} else {
		stringMembers := c.ZRange(source, start, stop)
		members = make([]models.ZSetMember, len(stringMembers))
		for i, member := range stringMembers {
			score, _ := c.ZScore(source, member)
			members[i] = models.ZSetMember{Member: member, Score: score}
		}
	}

	// Store results in destination
	newSet := &sync.Map{}
	for _, member := range members {
		newSet.Store(member.Member, member.Score)
	}
	c.zsets.Store(destination, newSet)
	c.incrementKeyVersion(destination)

	return len(members), nil
}

func (c *MemoryCache) ZRemRangeByLex(key string, min, max string) (int, error) {
	setI, exists := c.zsets.Load(key)
	if !exists {
		return 0, nil
	}
	set := setI.(*sync.Map)

	// Parse range specifications
	minInclusive := true
	maxInclusive := true
	if strings.HasPrefix(min, "(") {
		minInclusive = false
		min = min[1:]
	} else if strings.HasPrefix(min, "[") {
		min = min[1:]
	}
	if strings.HasPrefix(max, "(") {
		maxInclusive = false
		max = max[1:]
	} else if strings.HasPrefix(max, "[") {
		max = max[1:]
	}

	minIsInf := min == "-"
	maxIsInf := max == "+"

	toRemove := make([]string, 0)
	set.Range(func(member, _ interface{}) bool {
		memberStr := member.(string)
		if minIsInf || (minInclusive && memberStr >= min) || (!minInclusive && memberStr > min) {
			if maxIsInf || (maxInclusive && memberStr <= max) || (!maxInclusive && memberStr < max) {
				toRemove = append(toRemove, memberStr)
			}
		}
		return true
	})

	for _, member := range toRemove {
		set.Delete(member)
	}

	if len(toRemove) > 0 {
		c.incrementKeyVersion(key)
	}

	return len(toRemove), nil
}

func (c *MemoryCache) ZRemRangeByRank(key string, start, stop int) (int, error) {
	members := c.ZRange(key, start, stop)
	if len(members) == 0 {
		return 0, nil
	}

	setI, exists := c.zsets.Load(key)
	if !exists {
		return 0, nil
	}
	set := setI.(*sync.Map)

	for _, member := range members {
		set.Delete(member)
	}

	c.incrementKeyVersion(key)
	return len(members), nil
}

func (c *MemoryCache) ZRemRangeByScore(key string, min, max float64) (int, error) {
	members := c.ZRangeByScore(key, min, max)
	if len(members) == 0 {
		return 0, nil
	}

	setI, exists := c.zsets.Load(key)
	if !exists {
		return 0, nil
	}
	set := setI.(*sync.Map)

	for _, member := range members {
		set.Delete(member)
	}

	c.incrementKeyVersion(key)
	return len(members), nil
}

func (c *MemoryCache) ZRevRangeByLex(key string, max, min string) []string {
	setI, exists := c.zsets.Load(key)
	if !exists {
		return []string{}
	}
	set := setI.(*sync.Map)

	// Parse range specifications
	minInclusive := true
	maxInclusive := true
	if strings.HasPrefix(min, "(") {
		minInclusive = false
		min = min[1:]
	} else if strings.HasPrefix(min, "[") {
		min = min[1:]
	}
	if strings.HasPrefix(max, "(") {
		maxInclusive = false
		max = max[1:]
	} else if strings.HasPrefix(max, "[") {
		max = max[1:]
	}

	// Special cases for infinity
	minIsInf := min == "-"
	maxIsInf := max == "+"

	// Collect matching members
	var members []string
	set.Range(func(member, _ interface{}) bool {
		memberStr := member.(string)

		// Check if member is within range
		if minIsInf || (minInclusive && memberStr >= min) || (!minInclusive && memberStr > min) {
			if maxIsInf || (maxInclusive && memberStr <= max) || (!maxInclusive && memberStr < max) {
				members = append(members, memberStr)
			}
		}
		return true
	})

	// Sort lexicographically in reverse order
	sort.Sort(sort.Reverse(sort.StringSlice(members)))
	return members
}

func (c *MemoryCache) ZRevRangeByScore(key string, max, min float64) []string {
	members := c.ZRangeByScore(key, min, max)
	// Reverse the order
	for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
		members[i], members[j] = members[j], members[i]
	}
	return members
}

func (c *MemoryCache) ZRevRank(key string, member string) (int, bool) {
	members := c.ZRange(key, 0, -1)
	// Search from the end
	for i := len(members) - 1; i >= 0; i-- {
		if members[i] == member {
			return len(members) - 1 - i, true
		}
	}
	return 0, false
}

func (c *MemoryCache) ZScan(key string, cursor int, match string, count int) ([]models.ZSetMember, int) {
	setI, exists := c.zsets.Load(key)
	if !exists {
		return []models.ZSetMember{}, 0
	}
	set := setI.(*sync.Map)

	// Get all members first
	var members []models.ZSetMember
	set.Range(func(member, score interface{}) bool {
		if pattern.Match(match, member.(string)) {
			members = append(members, models.ZSetMember{
				Member: member.(string),
				Score:  score.(float64),
			})
		}
		return true
	})

	// Sort for consistent iteration
	sort.Slice(members, func(i, j int) bool {
		return members[i].Member < members[j].Member
	})

	if len(members) == 0 {
		return []models.ZSetMember{}, 0
	}

	// Handle cursor and count
	if cursor >= len(members) {
		return []models.ZSetMember{}, 0
	}

	end := cursor + count
	if end > len(members) {
		end = len(members)
	}

	nextCursor := end
	if nextCursor >= len(members) {
		nextCursor = 0
	}

	return members[cursor:end], nextCursor
}

func (c *MemoryCache) ZUnion(keys ...string) []models.ZSetMember {
	if len(keys) == 0 {
		return []models.ZSetMember{}
	}

	// Use map to accumulate scores
	unionMap := make(map[string]float64)

	for _, key := range keys {
		if setI, exists := c.zsets.Load(key); exists {
			set := setI.(*sync.Map)
			set.Range(func(member, score interface{}) bool {
				memberStr := member.(string)
				scoreFloat := score.(float64)

				if existingScore, ok := unionMap[memberStr]; ok {
					unionMap[memberStr] = math.Max(existingScore, scoreFloat)
				} else {
					unionMap[memberStr] = scoreFloat
				}
				return true
			})
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

	// Sort by score and member
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score == result[j].Score {
			return result[i].Member < result[j].Member
		}
		return result[i].Score < result[j].Score
	})

	return result
}
