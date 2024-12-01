package cache

import (
	"fmt"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type MemoryCache struct {
	sets         map[string]string
	hsets        map[string]map[string]string
	lists        map[string][]string
	sets_        map[string]map[string]bool
	expires      map[string]time.Time
	setsMu       sync.RWMutex
	hsetsMu      sync.RWMutex
	listsMu      sync.RWMutex
	setsMu_      sync.RWMutex
	stats        *Stats
	transactions map[int64]*models.Transaction // goroutine ID'ye göre transaction takibi
	txMu         sync.RWMutex
	keyVersions  map[string]int64 // Her key için versiyon numarası
	versionMu    sync.RWMutex
}

func NewMemoryCache() *MemoryCache {
	mc := &MemoryCache{
		sets:         make(map[string]string),
		hsets:        make(map[string]map[string]string),
		lists:        make(map[string][]string),
		sets_:        make(map[string]map[string]bool),
		expires:      make(map[string]time.Time),
		stats:        NewStats(),
		keyVersions:  make(map[string]int64),
		transactions: make(map[int64]*models.Transaction),
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
	c.incrementKeyVersion(key) // Versiyon güncelleme
	return true, nil
}

func (c *MemoryCache) Set(key string, value string) error {
	c.setsMu.Lock()
	defer c.setsMu.Unlock()

	c.sets[key] = value
	c.incrementKeyVersion(key)
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

	if _, exists := c.hsets[hash]; !exists {
		c.hsets[hash] = make(map[string]string)
	}
	c.hsets[hash][key] = value
	c.incrementKeyVersion(hash) // Hash key'in versiyonunu güncelle
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
	c.incrementKeyVersion(key) // Versiyon güncelleme
	return len(c.lists[key]), nil
}

func (c *MemoryCache) RPush(key string, value string) (int, error) {
	c.listsMu.Lock()
	defer c.listsMu.Unlock()

	if _, exists := c.lists[key]; !exists {
		c.lists[key] = make([]string, 0)
	}
	c.lists[key] = append(c.lists[key], value)
	c.incrementKeyVersion(key) // Versiyon güncelleme
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

	if c.sets_[key][member] {
		return false, nil
	}

	c.sets_[key][member] = true
	c.incrementKeyVersion(key) // Versiyon güncelleme
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

func (c *MemoryCache) LLen(key string) int {
	c.listsMu.RLock()
	defer c.listsMu.RUnlock()

	return len(c.lists[key])
}

func (c *MemoryCache) LPop(key string) (string, bool) {
	c.listsMu.Lock()
	defer c.listsMu.Unlock()

	list, exists := c.lists[key]
	if !exists || len(list) == 0 {
		return "", false
	}

	value := list[0]
	c.lists[key] = list[1:]
	c.incrementKeyVersion(key) // Versiyon güncelleme

	if len(c.lists[key]) == 0 {
		delete(c.lists, key)
	}
	return value, true
}

func (c *MemoryCache) RPop(key string) (string, bool) {
	c.listsMu.Lock()
	defer c.listsMu.Unlock()

	list, exists := c.lists[key]
	if !exists || len(list) == 0 {
		return "", false
	}

	lastIdx := len(list) - 1
	value := list[lastIdx]
	c.lists[key] = list[:lastIdx]
	c.incrementKeyVersion(key) // Versiyon güncelleme

	if len(c.lists[key]) == 0 {
		delete(c.lists, key)
	}
	return value, true
}

func (c *MemoryCache) SCard(key string) int {
	c.setsMu_.RLock()
	defer c.setsMu_.RUnlock()

	set, exists := c.sets_[key]
	if !exists {
		return 0
	}

	return len(set)
}

func (c *MemoryCache) SRem(key string, member string) (bool, error) {
	c.setsMu_.Lock()
	defer c.setsMu_.Unlock()

	set, exists := c.sets_[key]
	if !exists {
		return false, nil
	}

	if _, exists := set[member]; !exists {
		return false, nil
	}

	delete(set, member)
	c.incrementKeyVersion(key) // Versiyon güncelleme

	if len(set) == 0 {
		delete(c.sets_, key)
	}
	return true, nil
}

func (c *MemoryCache) SIsMember(key string, member string) bool {
	c.setsMu_.RLock()
	defer c.setsMu_.RUnlock()

	set, exists := c.sets_[key]
	if !exists {
		return false
	}

	return set[member]
}

func (c *MemoryCache) LSet(key string, index int, value string) error {
	c.listsMu.Lock()
	defer c.listsMu.Unlock()

	list, exists := c.lists[key]
	if !exists {
		return fmt.Errorf("ERR no such key")
	}

	if index < 0 {
		index = len(list) + index
	}

	if index < 0 || index >= len(list) {
		return fmt.Errorf("ERR index out of range")
	}

	list[index] = value
	return nil
}

func (c *MemoryCache) SInter(keys ...string) []string {
	c.setsMu_.RLock()
	defer c.setsMu_.RUnlock()

	if len(keys) == 0 {
		return []string{}
	}

	// İlk set'i sonuç olarak al
	result := make(map[string]bool)
	firstSet, exists := c.sets_[keys[0]]
	if !exists {
		return []string{}
	}

	// İlk set'in elemanlarını result'a kopyala
	for member := range firstSet {
		result[member] = true
	}

	// Diğer setlerle kesişimi bul
	for _, key := range keys[1:] {
		set, exists := c.sets_[key]
		if !exists {
			return []string{} // Herhangi bir set yoksa boş dön
		}

		// Sadece tüm setlerde olan elemanları tut
		for member := range result {
			if !set[member] {
				delete(result, member)
			}
		}
	}

	// Map'i slice'a çevir ve sırala
	intersection := make([]string, 0, len(result))
	for member := range result {
		intersection = append(intersection, member)
	}
	sort.Strings(intersection)
	return intersection
}

func (c *MemoryCache) SUnion(keys ...string) []string {
	c.setsMu_.RLock()
	defer c.setsMu_.RUnlock()

	result := make(map[string]bool)

	// Tüm setlerdeki elemanları birleştir
	for _, key := range keys {
		if set, exists := c.sets_[key]; exists {
			for member := range set {
				result[member] = true
			}
		}
	}

	// Map'i slice'a çevir ve sırala
	union := make([]string, 0, len(result))
	for member := range result {
		union = append(union, member)
	}
	sort.Strings(union)
	return union
}

func (c *MemoryCache) Type(key string) string {
	c.setsMu.RLock()
	if _, exists := c.sets[key]; exists {
		c.setsMu.RUnlock()
		return "string"
	}
	c.setsMu.RUnlock()

	c.hsetsMu.RLock()
	if _, exists := c.hsets[key]; exists {
		c.hsetsMu.RUnlock()
		return "hash"
	}
	c.hsetsMu.RUnlock()

	c.listsMu.RLock()
	if _, exists := c.lists[key]; exists {
		c.listsMu.RUnlock()
		return "list"
	}
	c.listsMu.RUnlock()

	c.setsMu_.RLock()
	if _, exists := c.sets_[key]; exists {
		c.setsMu_.RUnlock()
		return "set"
	}
	c.setsMu_.RUnlock()

	return "none"
}

func (c *MemoryCache) Exists(key string) bool {
	return c.Type(key) != "none"
}

func (c *MemoryCache) FlushAll() {
	c.setsMu.Lock()
	c.hsetsMu.Lock()
	c.listsMu.Lock()
	c.setsMu_.Lock()
	defer c.setsMu.Unlock()
	defer c.hsetsMu.Unlock()
	defer c.listsMu.Unlock()
	defer c.setsMu_.Unlock()

	c.sets = make(map[string]string)
	c.hsets = make(map[string]map[string]string)
	c.lists = make(map[string][]string)
	c.sets_ = make(map[string]map[string]bool)
	c.expires = make(map[string]time.Time)
}

func (c *MemoryCache) DBSize() int {
	total := 0

	c.setsMu.RLock()
	total += len(c.sets)
	c.setsMu.RUnlock()

	c.hsetsMu.RLock()
	total += len(c.hsets)
	c.hsetsMu.RUnlock()

	c.listsMu.RLock()
	total += len(c.lists)
	c.listsMu.RUnlock()

	c.setsMu_.RLock()
	total += len(c.sets_)
	c.setsMu_.RUnlock()

	return total
}

type Stats struct {
	startTime time.Time
	cmdCount  int64
	mu        sync.RWMutex
}

func NewStats() *Stats {
	return &Stats{
		startTime: time.Now(),
	}
}

func (s *Stats) IncrCommandCount() {
	atomic.AddInt64(&s.cmdCount, 1)
}

func (c *MemoryCache) SDiff(keys ...string) []string {
	c.setsMu_.RLock()
	defer c.setsMu_.RUnlock()

	if len(keys) == 0 {
		return []string{}
	}

	// İlk set'i sonuç olarak al
	result := make(map[string]bool)
	firstSet, exists := c.sets_[keys[0]]
	if !exists {
		return []string{}
	}

	// İlk set'in elemanlarını result'a kopyala
	for member := range firstSet {
		result[member] = true
	}

	// Diğer setlerdeki elemanları çıkar
	for _, key := range keys[1:] {
		if set, exists := c.sets_[key]; exists {
			for member := range set {
				delete(result, member)
			}
		}
	}

	// Map'i slice'a çevir ve sırala
	diff := make([]string, 0, len(result))
	for member := range result {
		diff = append(diff, member)
	}
	sort.Strings(diff)
	return diff
}

func (c *MemoryCache) LRem(key string, count int, value string) (int, error) {
	c.listsMu.Lock()
	defer c.listsMu.Unlock()

	list, exists := c.lists[key]
	if !exists {
		return 0, nil
	}

	removed := 0
	newList := make([]string, 0, len(list))

	if count > 0 {
		// Baştan count kadar eleman sil
		for _, v := range list {
			if v == value && removed < count {
				removed++
				continue
			}
			newList = append(newList, v)
		}
	} else if count < 0 {
		// Sondan |count| kadar eleman sil
		matches := make([]int, 0)
		for i, v := range list {
			if v == value {
				matches = append(matches, i)
			}
		}

		removeIndices := make(map[int]bool)
		for i := 0; i < len(matches) && i < -count; i++ {
			removeIndices[matches[len(matches)-1-i]] = true
		}

		for i, v := range list {
			if !removeIndices[i] {
				newList = append(newList, v)
			} else {
				removed++
			}
		}
	} else {
		// count == 0: tüm eşleşmeleri sil
		for _, v := range list {
			if v != value {
				newList = append(newList, v)
			} else {
				removed++
			}
		}
	}

	if removed > 0 {
		c.incrementKeyVersion(key) // Versiyon güncelleme
	}

	if len(newList) == 0 {
		delete(c.lists, key)
	} else {
		c.lists[key] = newList
	}

	return removed, nil
}

func (c *MemoryCache) Rename(oldKey, newKey string) error {
	c.setsMu.Lock()
	c.hsetsMu.Lock()
	c.listsMu.Lock()
	c.setsMu_.Lock()
	defer c.setsMu.Unlock()
	defer c.hsetsMu.Unlock()
	defer c.listsMu.Unlock()
	defer c.setsMu_.Unlock()

	// String tipinde
	if val, exists := c.sets[oldKey]; exists {
		c.sets[newKey] = val
		delete(c.sets, oldKey)
		if expTime, hasExp := c.expires[oldKey]; hasExp {
			c.expires[newKey] = expTime
			delete(c.expires, oldKey)
		}
		c.incrementKeyVersion(oldKey) // Eski key'in versiyonunu güncelle
		c.incrementKeyVersion(newKey) // Yeni key'in versiyonunu güncelle
		return nil
	}

	// Hash tipinde
	if val, exists := c.hsets[oldKey]; exists {
		c.hsets[newKey] = val
		delete(c.hsets, oldKey)
		c.incrementKeyVersion(oldKey)
		c.incrementKeyVersion(newKey)
		return nil
	}

	// List tipinde
	if val, exists := c.lists[oldKey]; exists {
		c.lists[newKey] = val
		delete(c.lists, oldKey)
		c.incrementKeyVersion(oldKey)
		c.incrementKeyVersion(newKey)
		return nil
	}

	// Set tipinde
	if val, exists := c.sets_[oldKey]; exists {
		c.sets_[newKey] = val
		delete(c.sets_, oldKey)
		c.incrementKeyVersion(oldKey)
		c.incrementKeyVersion(newKey)
		return nil
	}

	// Key bulunamadı
	return fmt.Errorf("ERR no such key")
}

func (c *MemoryCache) Info() map[string]string {
	info := make(map[string]string)

	// Server bilgileri
	info["uptime_in_seconds"] = fmt.Sprintf("%d", int(time.Since(c.stats.startTime).Seconds()))
	info["total_commands_processed"] = fmt.Sprintf("%d", atomic.LoadInt64(&c.stats.cmdCount))

	// Memory kullanımı
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	info["used_memory"] = fmt.Sprintf("%d", memStats.Alloc)
	info["used_memory_peak"] = fmt.Sprintf("%d", memStats.TotalAlloc)

	// Keyler
	info["total_keys"] = fmt.Sprintf("%d", c.DBSize())

	c.setsMu.RLock()
	info["string_keys"] = fmt.Sprintf("%d", len(c.sets))
	c.setsMu.RUnlock()

	c.hsetsMu.RLock()
	info["hash_keys"] = fmt.Sprintf("%d", len(c.hsets))
	c.hsetsMu.RUnlock()

	c.listsMu.RLock()
	info["list_keys"] = fmt.Sprintf("%d", len(c.lists))
	c.listsMu.RUnlock()

	c.setsMu_.RLock()
	info["set_keys"] = fmt.Sprintf("%d", len(c.sets_))
	c.setsMu_.RUnlock()

	return info
}

func (c *MemoryCache) IncrCommandCount() {
	if c.stats != nil {
		c.stats.IncrCommandCount()
	}
}

func getGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, _ := strconv.ParseInt(idField, 10, 64)
	return id
}

func (c *MemoryCache) Multi() error {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	gid := getGoroutineID()
	tx, exists := c.transactions[gid]
	if exists && tx.InMulti {
		return fmt.Errorf("ERR MULTI calls can not be nested")
	}

	if !exists {
		tx = &models.Transaction{
			Watches: make(map[string]int64),
		}
		c.transactions[gid] = tx
	}

	tx.Commands = make([]models.Command, 0)
	tx.InMulti = true
	return nil
}

func (c *MemoryCache) Exec() ([]models.Value, error) {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	gid := getGoroutineID()
	tx, exists := c.transactions[gid]
	if !exists || !tx.InMulti {
		return nil, fmt.Errorf("ERR EXEC without MULTI")
	}

	// Watch'ları kontrol et
	if !c.checkWatches(tx) {
		delete(c.transactions, gid)
		return nil, nil // Redis NULL response for failed transactions
	}

	// Transaction'ı temizle
	defer delete(c.transactions, gid)

	// Tüm komutları sırayla çalıştır
	results := make([]models.Value, 0, len(tx.Commands))

	// Global mutex kullanarak atomikliği sağla
	c.setsMu.Lock()
	c.hsetsMu.Lock()
	c.listsMu.Lock()
	c.setsMu_.Lock()
	defer c.setsMu.Unlock()
	defer c.hsetsMu.Unlock()
	defer c.listsMu.Unlock()
	defer c.setsMu_.Unlock()

	for _, cmd := range tx.Commands {
		var result models.Value
		switch cmd.Name {
		case "SET":
			err := c.Set(cmd.Args[0].Bulk, cmd.Args[1].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "string", Str: "OK"}
			}

		case "HSET":
			err := c.HSet(cmd.Args[0].Bulk, cmd.Args[1].Bulk, cmd.Args[2].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "string", Str: "OK"}
			}

		case "LPUSH":
			length, err := c.LPush(cmd.Args[0].Bulk, cmd.Args[1].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "integer", Num: length}
			}

		case "RPUSH":
			length, err := c.RPush(cmd.Args[0].Bulk, cmd.Args[1].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "integer", Num: length}
			}

		case "SADD":
			added, err := c.SAdd(cmd.Args[0].Bulk, cmd.Args[1].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else if added {
				result = models.Value{Type: "integer", Num: 1}
			} else {
				result = models.Value{Type: "integer", Num: 0}
			}

		case "DEL":
			deleted, err := c.Del(cmd.Args[0].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else if deleted {
				result = models.Value{Type: "integer", Num: 1}
			} else {
				result = models.Value{Type: "integer", Num: 0}
			}

		case "LREM":
			count, _ := strconv.Atoi(cmd.Args[1].Bulk)
			removed, err := c.LRem(cmd.Args[0].Bulk, count, cmd.Args[2].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "integer", Num: removed}
			}

		case "LSET":
			index, _ := strconv.Atoi(cmd.Args[1].Bulk)
			err := c.LSet(cmd.Args[0].Bulk, index, cmd.Args[2].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "string", Str: "OK"}
			}

		case "EXPIRE":
			seconds, _ := strconv.Atoi(cmd.Args[1].Bulk)
			err := c.Expire(cmd.Args[0].Bulk, seconds)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "integer", Num: 1}
			}

		case "RENAME":
			err := c.Rename(cmd.Args[0].Bulk, cmd.Args[1].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "string", Str: "OK"}
			}

		case "SREM":
			removed, err := c.SRem(cmd.Args[0].Bulk, cmd.Args[1].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else if removed {
				result = models.Value{Type: "integer", Num: 1}
			} else {
				result = models.Value{Type: "integer", Num: 0}
			}

		default:
			result = models.Value{Type: "error", Str: "ERR unknown command " + cmd.Name}
		}

		// Her komutun versiyonunu artır
		c.incrementKeyVersion(cmd.Args[0].Bulk)
		results = append(results, result)
	}

	return results, nil
}

func (c *MemoryCache) Discard() error {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	gid := getGoroutineID()
	if _, exists := c.transactions[gid]; !exists {
		return fmt.Errorf("ERR DISCARD without MULTI")
	}

	delete(c.transactions, gid)
	return nil
}

func (c *MemoryCache) AddToTransaction(cmd models.Command) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	gid := getGoroutineID()
	tx, exists := c.transactions[gid]
	if !exists || !tx.InMulti {
		return fmt.Errorf("ERR no MULTI context")
	}

	tx.Commands = append(tx.Commands, cmd)
	return nil
}

func (c *MemoryCache) IsInTransaction() bool {
	c.txMu.RLock()
	defer c.txMu.RUnlock()

	gid := getGoroutineID()
	tx, exists := c.transactions[gid]
	return exists && tx.InMulti
}

func (c *MemoryCache) incrementKeyVersion(key string) {
	c.versionMu.Lock()
	defer c.versionMu.Unlock()
	c.keyVersions[key]++
}

func (c *MemoryCache) GetKeyVersion(key string) int64 {
	c.versionMu.RLock()
	defer c.versionMu.RUnlock()
	return c.keyVersions[key]
}

func (c *MemoryCache) Watch(keys ...string) error {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	gid := getGoroutineID()
	tx, exists := c.transactions[gid]
	if !exists {
		tx = &models.Transaction{
			Watches: make(map[string]int64),
		}
		c.transactions[gid] = tx
	}

	// Her key için mevcut versiyonu kaydet
	for _, key := range keys {
		tx.Watches[key] = c.GetKeyVersion(key)
	}

	return nil
}

func (c *MemoryCache) Unwatch() error {
	c.txMu.Lock()
	defer c.txMu.Unlock()

	gid := getGoroutineID()
	tx, exists := c.transactions[gid]
	if !exists {
		return nil // UNWATCH is a no-op if no WATCH is set
	}

	tx.Watches = make(map[string]int64)
	return nil
}

func (c *MemoryCache) checkWatches(tx *models.Transaction) bool {
	c.versionMu.RLock()
	defer c.versionMu.RUnlock()

	for key, version := range tx.Watches {
		if currentVersion := c.keyVersions[key]; currentVersion != version {
			return false
		}
	}
	return true
}
