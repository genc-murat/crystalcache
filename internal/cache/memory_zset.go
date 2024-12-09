package cache

import (
	"errors"
	"sort"
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
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
