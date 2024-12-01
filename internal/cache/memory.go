package cache

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MemoryCache struct {
	sets    map[string]string
	hsets   map[string]map[string]string
	lists   map[string][]string
	sets_   map[string]map[string]bool
	expires map[string]time.Time
	setsMu  sync.RWMutex
	hsetsMu sync.RWMutex
	listsMu sync.RWMutex
	setsMu_ sync.RWMutex
}

func NewMemoryCache() *MemoryCache {
	mc := &MemoryCache{
		sets:    make(map[string]string),
		hsets:   make(map[string]map[string]string),
		lists:   make(map[string][]string),
		sets_:   make(map[string]map[string]bool),
		expires: make(map[string]time.Time),
	}

	// Expire check goroutine'u
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			mc.cleanExpired()
		}
	}()

	return mc
}

func matchPattern(pattern, str string) bool {
	if pattern == "*" {
		return true
	}

	// Convert Redis pattern to regex pattern
	regexPattern := strings.Builder{}
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*':
			regexPattern.WriteString(".*")
		case '?':
			regexPattern.WriteString(".")
		case '[', ']', '(', ')', '{', '}', '.', '+', '|', '^', '$':
			regexPattern.WriteString("\\")
			regexPattern.WriteByte(pattern[i])
		default:
			regexPattern.WriteByte(pattern[i])
		}
	}

	regex, err := regexp.Compile("^" + regexPattern.String() + "$")
	if err != nil {
		return false
	}

	return regex.MatchString(str)
}

func (c *MemoryCache) cleanExpired() {
	now := time.Now()
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	for key, expireTime := range c.expires {
		if now.After(expireTime) {
			delete(c.sets, key)
			delete(c.expires, key)
		}
	}
}

func (c *MemoryCache) Incr(key string) (int, error) {
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	val, exists := c.sets[key]
	if !exists {
		c.sets[key] = "1"
		return 1, nil
	}

	num, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer")
	}

	num++
	c.sets[key] = strconv.Itoa(num)
	return num, nil
}

func (c *MemoryCache) Expire(key string, seconds int) error {
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	if _, exists := c.sets[key]; !exists {
		return nil // Redis returns 0 if key doesn't exist
	}

	c.expires[key] = time.Now().Add(time.Duration(seconds) * time.Second)
	return nil
}

func (c *MemoryCache) Del(key string) (bool, error) {
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	if _, exists := c.sets[key]; !exists {
		return false, nil
	}

	delete(c.sets, key)
	delete(c.expires, key)
	return true, nil
}

func (c *MemoryCache) Set(key string, value string) error {
	c.setsMu.Lock()
	defer c.setsMu.Unlock()
	c.sets[key] = value
	return nil
}

func (c *MemoryCache) Get(key string) (string, bool) {
	c.setsMu.RLock()
	defer c.setsMu.RUnlock()

	// Expire kontrolü
	if expireTime, hasExpire := c.expires[key]; hasExpire && time.Now().After(expireTime) {
		delete(c.sets, key)
		delete(c.expires, key)
		return "", false
	}

	value, ok := c.sets[key]
	return value, ok
}

func (c *MemoryCache) HSet(hash string, key string, value string) error {
	c.hsetsMu.Lock()
	defer c.hsetsMu.Unlock()

	if _, ok := c.hsets[hash]; !ok {
		c.hsets[hash] = make(map[string]string)
	}
	c.hsets[hash][key] = value
	return nil
}

func (c *MemoryCache) HGet(hash string, key string) (string, bool) {
	c.hsetsMu.RLock()
	defer c.hsetsMu.RUnlock()

	if hashMap, ok := c.hsets[hash]; ok {
		value, exists := hashMap[key]
		return value, exists
	}
	return "", false
}

func (c *MemoryCache) HGetAll(hash string) map[string]string {
	c.hsetsMu.RLock()
	defer c.hsetsMu.RUnlock()

	if hashMap, ok := c.hsets[hash]; ok {
		// Orijinal map'i değiştirmemek için kopya oluşturuyoruz
		result := make(map[string]string, len(hashMap))
		for k, v := range hashMap {
			result[k] = v
		}
		return result
	}
	return make(map[string]string)
}

func (c *MemoryCache) Keys(pattern string) []string {
	c.setsMu.RLock()
	defer c.setsMu.RUnlock()

	var keys []string
	for key := range c.sets {
		if matchPattern(pattern, key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys) // Sonuçları sıralıyoruz
	return keys
}

func (c *MemoryCache) TTL(key string) int {
	c.setsMu.RLock()
	defer c.setsMu.RUnlock()

	// Key yoksa -2 dön
	if _, exists := c.sets[key]; !exists {
		return -2
	}

	// Expire yoksa -1 dön
	expireTime, hasExpire := c.expires[key]
	if !hasExpire {
		return -1
	}

	// Kalan süreyi hesapla
	ttl := int(time.Until(expireTime).Seconds())
	if ttl < 0 {
		return -2 // Expire olmuş
	}
	return ttl
}

func (c *MemoryCache) LPush(key string, value string) (int, error) {
	c.listsMu.Lock()
	defer c.listsMu.Unlock()

	if _, exists := c.lists[key]; !exists {
		c.lists[key] = make([]string, 0)
	}

	c.lists[key] = append([]string{value}, c.lists[key]...)
	return len(c.lists[key]), nil
}

func (c *MemoryCache) RPush(key string, value string) (int, error) {
	c.listsMu.Lock()
	defer c.listsMu.Unlock()

	if _, exists := c.lists[key]; !exists {
		c.lists[key] = make([]string, 0)
	}

	c.lists[key] = append(c.lists[key], value)
	return len(c.lists[key]), nil
}

func (c *MemoryCache) LRange(key string, start, stop int) ([]string, error) {
	c.listsMu.RLock()
	defer c.listsMu.RUnlock()

	list, exists := c.lists[key]
	if !exists {
		return []string{}, nil
	}

	length := len(list)

	// Redis'teki gibi negatif indeksleri handle et
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Sınırları kontrol et
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return []string{}, nil
	}

	return list[start : stop+1], nil
}

func (c *MemoryCache) SAdd(key string, member string) (bool, error) {
	c.setsMu_.Lock()
	defer c.setsMu_.Unlock()

	if _, exists := c.sets_[key]; !exists {
		c.sets_[key] = make(map[string]bool)
	}

	// Eğer eleman zaten varsa false dön
	if c.sets_[key][member] {
		return false, nil
	}

	c.sets_[key][member] = true
	return true, nil
}

func (c *MemoryCache) SMembers(key string) ([]string, error) {
	c.setsMu_.RLock()
	defer c.setsMu_.RUnlock()

	set, exists := c.sets_[key]
	if !exists {
		return []string{}, nil
	}

	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}
	sort.Strings(members) // Sonuçları sıralıyoruz
	return members, nil
}
