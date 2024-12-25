package cache

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/genc-murat/crystalcache/internal/cache/bitmap"
	"github.com/genc-murat/crystalcache/internal/cache/zset"
	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/pkg/utils/pattern"
)

type MemoryCache struct {
	sets          *sync.Map // string key-value pairs
	hsets         *sync.Map // hash maps
	lists         *sync.Map // lists
	sets_         *sync.Map // sets
	expires       *sync.Map // expiration times
	stats         *Stats
	transactions  *sync.Map
	keyVersions   *sync.Map
	zsets         *sync.Map
	jsonData      *sync.Map
	streams       *sync.Map // stream entries
	streamGroups  *sync.Map // stream consumer groups
	bitmaps       *sync.Map
	geoData       *sync.Map
	suggestions   *sync.Map // suggestion dictionaries
	cms           *sync.Map // Count-Min Sketches
	hlls          *sync.Map
	bloomFilter   *models.BloomFilter
	cuckooFilters *sync.Map
	tdigests      *sync.Map // T-Digest storage
	bfilters      *sync.Map
	topks         *sync.Map
	lastDefrag    time.Time
	defragMu      sync.Mutex
	timeSeries    *sync.Map

	zsetManager   *zset.Manager
	bitmapManager *bitmap.Manager
}

func NewMemoryCache() *MemoryCache {
	config := models.BloomFilterConfig{
		ExpectedItems:     1000000,
		FalsePositiveRate: 0.01,
	}

	mc := &MemoryCache{
		sets:          &sync.Map{},
		hsets:         &sync.Map{},
		lists:         &sync.Map{},
		sets_:         &sync.Map{},
		expires:       &sync.Map{},
		stats:         NewStats(),
		transactions:  &sync.Map{},
		keyVersions:   &sync.Map{},
		zsets:         &sync.Map{},
		jsonData:      &sync.Map{},
		streams:       &sync.Map{},
		streamGroups:  &sync.Map{},
		bitmaps:       &sync.Map{},
		geoData:       &sync.Map{},
		suggestions:   &sync.Map{},
		cms:           &sync.Map{},
		cuckooFilters: &sync.Map{},
		hlls:          &sync.Map{},
		bloomFilter:   models.NewBloomFilter(config),
		bfilters:      &sync.Map{},
		tdigests:      &sync.Map{},
		topks:         &sync.Map{},
		timeSeries:    &sync.Map{},
	}

	// Start background cleanup
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			mc.cleanExpired()
		}
	}()

	mc.zsetManager = zset.NewManager(mc.zsets, mc.keyVersions)
	mc.bitmapManager = bitmap.NewManager(mc.bitmaps, mc.keyVersions)

	return mc
}

func (c *MemoryCache) cleanExpired() {
	now := time.Now()
	c.expires.Range(func(key, expireTime interface{}) bool {
		if expTime, ok := expireTime.(time.Time); ok && now.After(expTime) {
			c.sets.Delete(key)
			c.expires.Delete(key)
		}
		return true
	})
}

func (c *MemoryCache) SetJSON(key string, value interface{}) error {
	c.jsonData.Store(key, value)
	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) GetJSON(key string) (interface{}, bool) {
	return c.jsonData.Load(key)
}

func (c *MemoryCache) DeleteJSON(key string) bool {
	_, existed := c.jsonData.LoadAndDelete(key)
	if existed {
		c.incrementKeyVersion(key)
	}
	return existed
}

func (c *MemoryCache) Incr(key string) (int, error) {
	for {
		val, exists := c.sets.Load(key)
		if !exists {
			if c.sets.CompareAndSwap(key, nil, "1") {
				return 1, nil
			}
			continue
		}

		num, err := strconv.Atoi(val.(string))
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer")
		}

		num++
		if c.sets.CompareAndSwap(key, val, strconv.Itoa(num)) {
			return num, nil
		}
	}
}

func (c *MemoryCache) Expire(key string, seconds int) error {
	// Check if key exists in the cache
	if _, exists := c.sets.Load(key); !exists {
		return nil
	}

	// Calculate expiration time
	expirationTime := time.Now().Add(time.Duration(seconds) * time.Second)

	// Store expiration time
	c.expires.Store(key, expirationTime)

	// Start a background goroutine to handle expiration if seconds > 0
	if seconds > 0 {
		go func() {
			timer := time.NewTimer(time.Duration(seconds) * time.Second)
			defer timer.Stop()

			<-timer.C

			// Check if the key still exists with the same expiration time
			if expTime, exists := c.expires.Load(key); exists {
				if expTime.(time.Time).Equal(expirationTime) {
					c.sets.Delete(key)
					c.expires.Delete(key)

					// Increment expired keys counter
					if c.stats != nil {
						atomic.AddInt64(&c.stats.expiredKeys, 1)
					}
				}
			}
		}()
	}

	return nil
}

func (c *MemoryCache) Del(key string) (bool, error) {
	deleted := false

	if _, ok := c.sets.LoadAndDelete(key); ok {
		c.expires.Delete(key)
		deleted = true
	}

	if _, ok := c.sets_.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.hsets.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.lists.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.jsonData.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.zsets.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.geoData.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.suggestions.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.cms.LoadAndDelete(key); ok {
		deleted = true
	}

	if deleted {
		c.incrementKeyVersion(key)
	}

	if _, ok := c.cuckooFilters.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.hlls.LoadAndDelete(key); ok {
		deleted = true
	}

	if _, ok := c.tdigests.LoadAndDelete(key); ok {
		deleted = true
	}

	return deleted, nil
}

func (c *MemoryCache) Set(key string, value string) error {
	c.bloomFilter.Add([]byte(key))
	c.sets.Store(key, value)
	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) Get(key string) (string, bool) {
	if !c.bloomFilter.Contains([]byte(key)) {
		return "", false
	}

	if expireTime, ok := c.expires.Load(key); ok {
		if expTime, ok := expireTime.(time.Time); ok && time.Now().After(expTime) {
			c.sets.Delete(key)
			c.expires.Delete(key)
			return "", false
		}
	}

	if value, ok := c.sets.Load(key); ok {
		return value.(string), true
	}
	return "", false
}

// TTL implementation with sync.Map
func (c *MemoryCache) TTL(key string) int {
	// Check if key exists
	if _, exists := c.sets.Load(key); !exists {
		return -2
	}

	// Check expiration
	expireTimeI, hasExpire := c.expires.Load(key)
	if !hasExpire {
		return -1
	}

	expireTime := expireTimeI.(time.Time)
	ttl := int(time.Until(expireTime).Seconds())
	if ttl < 0 {
		// Key has expired, clean it up
		go func() {
			c.sets.Delete(key)
			c.expires.Delete(key)
			if c.stats != nil {
				atomic.AddInt64(&c.stats.expiredKeys, 1)
			}
		}()
		return -2
	}
	return ttl
}

// Helper function to create a new string slice pool
func newStringSlicePool(initialCap int) *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			slice := make([]string, 0, initialCap)
			return &slice
		},
	}
}

// Optional: Add batch operations for better performance
func (c *MemoryCache) BatchKeys(patterns []string) map[string][]string {
	results := make(map[string][]string)

	// Use multiple goroutines for parallel pattern matching
	var wg sync.WaitGroup
	resultMu := sync.Mutex{}

	for _, pattern := range patterns {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()

			matches := c.Keys(p)

			resultMu.Lock()
			results[p] = matches
			resultMu.Unlock()
		}(pattern)
	}

	wg.Wait()
	return results
}

// Optional: Add TTL batch operations
func (c *MemoryCache) BatchTTL(keys []string) map[string]int {
	results := make(map[string]int)

	var wg sync.WaitGroup
	resultMu := sync.Mutex{}

	for _, key := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()

			ttl := c.TTL(k)

			resultMu.Lock()
			results[k] = ttl
			resultMu.Unlock()
		}(key)
	}

	wg.Wait()
	return results
}

type ListOp struct {
	Op    string
	Key   string
	Value string
	Index int
}

type ListOpResult struct {
	Value interface{}
	Error error
}

func (c *MemoryCache) execListOp(op ListOp) (interface{}, error) {
	// Get list from pool
	listPool := sync.Pool{
		New: func() interface{} {
			list := make([]string, 0, 16) // Initial capacity of 16
			return &list
		},
	}

	for i := 0; i < maxRetries; i++ {
		var list []string
		oldListI, loaded := c.lists.LoadOrStore(op.Key, listPool.Get())
		oldList := oldListI.(*[]string)

		// If not loaded (new key), initialize empty list
		if !loaded {
			list = make([]string, 0, 16)
		} else {
			// Create new list with appropriate capacity
			initialCap := cap(*oldList)
			if initialCap < len(*oldList)+1 {
				initialCap *= 2
			}
			list = make([]string, 0, initialCap)
		}

		result, newList, err := c.processListOp(op, oldList, &list)
		if err != nil {
			if !loaded {
				listPool.Put(oldListI)
			}
			return nil, err
		}

		// Try to update the list
		if c.lists.CompareAndSwap(op.Key, oldListI, newList) {
			// Return old list to pool if it was newly created
			if !loaded {
				listPool.Put(oldListI)
			}

			// If list is empty after operation, remove it
			if len(*newList) == 0 {
				c.lists.Delete(op.Key)
				listPool.Put(newList)
			}

			c.incrementKeyVersion(op.Key)

			// Update stats
			if c.stats != nil {
				atomic.AddInt64(&c.stats.cmdCount, 1)
			}

			return result, nil
		}

		// If CAS failed, return new list to pool
		listPool.Put(&list)
	}

	return nil, fmt.Errorf("failed to execute list operation after %d retries", maxRetries)
}

func (c *MemoryCache) processListOp(op ListOp, oldList *[]string, newList *[]string) (interface{}, *[]string, error) {
	switch op.Op {
	case "LPUSH":
		*newList = append(*newList, op.Value)
		*newList = append(*newList, *oldList...)
		return len(*newList), newList, nil

	case "RPUSH":
		*newList = append(*newList, *oldList...)
		*newList = append(*newList, op.Value)
		return len(*newList), newList, nil

	case "LPOP":
		if len(*oldList) == 0 {
			return "", nil, nil
		}
		result := (*oldList)[0]
		*newList = append(*newList, (*oldList)[1:]...)
		return result, newList, nil

	case "RPOP":
		if len(*oldList) == 0 {
			return "", nil, nil
		}
		lastIdx := len(*oldList) - 1
		result := (*oldList)[lastIdx]
		*newList = append(*newList, (*oldList)[:lastIdx]...)
		return result, newList, nil

	case "LSET":
		if op.Index < 0 {
			op.Index = len(*oldList) + op.Index
		}
		if op.Index < 0 || op.Index >= len(*oldList) {
			return nil, nil, fmt.Errorf("ERR index out of range")
		}
		*newList = append(*newList, *oldList...)
		(*newList)[op.Index] = op.Value
		return "OK", newList, nil

	default:
		return nil, nil, fmt.Errorf("ERR unknown operation %s", op.Op)
	}
}

const maxRetries = 3

// Helper functions for common list operations
func (c *MemoryCache) getListLength(key string) int {
	if listI, ok := c.lists.Load(key); ok {
		list := listI.(*[]string)
		return len(*list)
	}
	return 0
}

func (c *MemoryCache) checkListExists(key string) bool {
	_, ok := c.lists.Load(key)
	return ok
}

func (c *MemoryCache) execBatchListOps(ops []ListOp) []ListOpResult {
	results := make([]ListOpResult, len(ops))

	// Execute operations sequentially to maintain order
	for i, op := range ops {
		value, err := c.execListOp(op)
		results[i] = ListOpResult{
			Value: value,
			Error: err,
		}
	}

	return results
}

func (c *MemoryCache) LPush(key string, value string) (int, error) {
	result, err := c.execListOp(ListOp{Op: "LPUSH", Key: key, Value: value})
	if err != nil {
		return 0, err
	}
	return result.(int), nil
}

func (c *MemoryCache) RPush(key string, value string) (int, error) {
	result, err := c.execListOp(ListOp{Op: "RPUSH", Key: key, Value: value})
	if err != nil {
		return 0, err
	}
	return result.(int), nil
}

func (c *MemoryCache) LRange(key string, start, stop int) ([]string, error) {
	// Load the list from sync.Map
	listI, exists := c.lists.Load(key)
	if !exists {
		return []string{}, nil
	}

	// Type assertion
	list := listI.(*[]string)
	length := len(*list)

	// Adjust negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Boundary checks
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return []string{}, nil
	}

	// Create result slice with exact capacity needed
	result := make([]string, stop-start+1)
	copy(result, (*list)[start:stop+1])

	// Update stats if needed
	if c.stats != nil {
		atomic.AddInt64(&c.stats.cmdCount, 1)
	}

	return result, nil
}

func (c *MemoryCache) BatchLRange(ranges map[string][2]int) map[string][]string {
	results := make(map[string][]string)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for key, r := range ranges {
		wg.Add(1)
		go func(k string, start, stop int) {
			defer wg.Done()

			if result, err := c.LRange(k, start, stop); err == nil {
				mu.Lock()
				results[k] = result
				mu.Unlock()
			}
		}(key, r[0], r[1])
	}

	wg.Wait()
	return results
}

// List Operations
func (c *MemoryCache) LLen(key string) int {
	if listI, ok := c.lists.Load(key); ok {
		list := listI.(*[]string)
		return len(*list)
	}
	return 0
}

func (c *MemoryCache) LPop(key string) (string, bool) {
	for {
		listI, exists := c.lists.Load(key)
		if !exists {
			return "", false
		}

		list := listI.(*[]string)
		if len(*list) == 0 {
			c.lists.Delete(key)
			return "", false
		}

		value := (*list)[0]
		newList := make([]string, len(*list)-1)
		copy(newList, (*list)[1:])

		if c.lists.CompareAndSwap(key, listI, &newList) {
			c.incrementKeyVersion(key)

			if len(newList) == 0 {
				c.lists.Delete(key)
			}

			return value, true
		}
	}
}

func (c *MemoryCache) RPop(key string) (string, bool) {
	for {
		listI, exists := c.lists.Load(key)
		if !exists {
			return "", false
		}

		list := listI.(*[]string)
		if len(*list) == 0 {
			c.lists.Delete(key)
			return "", false
		}

		lastIdx := len(*list) - 1
		value := (*list)[lastIdx]
		newList := make([]string, lastIdx)
		copy(newList, (*list)[:lastIdx])

		if c.lists.CompareAndSwap(key, listI, &newList) {
			c.incrementKeyVersion(key)

			if len(newList) == 0 {
				c.lists.Delete(key)
			}

			return value, true
		}
	}
}

func (c *MemoryCache) LSet(key string, index int, value string) error {
	for {
		listI, exists := c.lists.Load(key)
		if !exists {
			return fmt.Errorf("ERR no such key")
		}

		list := listI.(*[]string)
		if index < 0 {
			index = len(*list) + index
		}

		if index < 0 || index >= len(*list) {
			return fmt.Errorf("ERR index out of range")
		}

		newList := make([]string, len(*list))
		copy(newList, *list)
		newList[index] = value

		if c.lists.CompareAndSwap(key, listI, &newList) {
			c.incrementKeyVersion(key)
			return nil
		}
	}
}

func (c *MemoryCache) Type(key string) string {
	if _, exists := c.jsonData.Load(key); exists {
		return "json"
	}
	if _, exists := c.sets.Load(key); exists {
		return "string"
	}
	if _, exists := c.hsets.Load(key); exists {
		return "hash"
	}
	if _, exists := c.lists.Load(key); exists {
		return "list"
	}
	if _, exists := c.sets_.Load(key); exists {
		return "set"
	}
	if _, exists := c.zsets.Load(key); exists {
		return "zset"
	}
	if _, exists := c.bitmaps.Load(key); exists {
		return "bitmap"
	}
	if _, exists := c.streams.Load(key); exists {
		return "stream"
	}
	if _, exists := c.geoData.Load(key); exists {
		return "geo"
	}
	if _, exists := c.suggestions.Load(key); exists {
		return "suggestion"
	}
	if _, exists := c.cms.Load(key); exists {
		return "cms"
	}
	if _, exists := c.cuckooFilters.Load(key); exists {
		return "cuckoo"
	}
	if _, exists := c.hlls.Load(key); exists {
		return "hll"
	}
	if _, exists := c.tdigests.Load(key); exists {
		return "tdigest"
	}
	if _, exists := c.bfilters.Load(key); exists {
		return "bf"
	}
	if _, exists := c.topks.Load(key); exists {
		return "topk"
	}
	return "none"
}

// Optional: Add batch operations support
type BatchResult struct {
	Value interface{}
	Error error
}

func (c *MemoryCache) BatchOp(ops []struct {
	Op    string
	Key   string
	Value interface{}
}) []BatchResult {
	results := make([]BatchResult, len(ops))
	var wg sync.WaitGroup

	for i, op := range ops {
		wg.Add(1)
		go func(idx int, operation struct {
			Op    string
			Key   string
			Value interface{}
		}) {
			defer wg.Done()

			switch operation.Op {
			case "SMEMBERS":
				if members, err := c.SMembers(operation.Key); err == nil {
					results[idx] = BatchResult{Value: members}
				} else {
					results[idx] = BatchResult{Error: err}
				}

			case "SET":
				if err := c.Set(operation.Key, operation.Value.(string)); err == nil {
					results[idx] = BatchResult{Value: "OK"}
				} else {
					results[idx] = BatchResult{Error: err}
				}

			case "GET":
				if value, exists := c.Get(operation.Key); exists {
					results[idx] = BatchResult{Value: value}
				} else {
					results[idx] = BatchResult{Value: nil}
				}

			case "HSET":
				if field, ok := operation.Value.(map[string]string); ok {
					for k, v := range field {
						if err := c.HSet(operation.Key, k, v); err != nil {
							results[idx] = BatchResult{Error: err}
							return
						}
					}
					results[idx] = BatchResult{Value: "OK"}
				} else {
					results[idx] = BatchResult{Error: fmt.Errorf("invalid HSET value type")}
				}

			case "HGET":
				if value, exists := c.HGet(operation.Key, operation.Value.(string)); exists {
					results[idx] = BatchResult{Value: value}
				} else {
					results[idx] = BatchResult{Value: nil}
				}

			case "DEL":
				if deleted, err := c.Del(operation.Key); err == nil {
					results[idx] = BatchResult{Value: deleted}
				} else {
					results[idx] = BatchResult{Error: err}
				}

			case "SADD":
				if member, ok := operation.Value.(string); ok {
					if added, err := c.SAdd(operation.Key, member); err == nil {
						results[idx] = BatchResult{Value: added}
					} else {
						results[idx] = BatchResult{Error: err}
					}
				} else {
					results[idx] = BatchResult{Error: fmt.Errorf("invalid SADD value type")}
				}

			case "SREM":
				if member, ok := operation.Value.(string); ok {
					if removed, err := c.SRem(operation.Key, member); err == nil {
						results[idx] = BatchResult{Value: removed}
					} else {
						results[idx] = BatchResult{Error: err}
					}
				} else {
					results[idx] = BatchResult{Error: fmt.Errorf("invalid SREM value type")}
				}

			case "EXPIRE":
				if seconds, ok := operation.Value.(int); ok {
					if err := c.Expire(operation.Key, seconds); err == nil {
						results[idx] = BatchResult{Value: true}
					} else {
						results[idx] = BatchResult{Error: err}
					}
				} else {
					results[idx] = BatchResult{Error: fmt.Errorf("invalid EXPIRE value type")}
				}

			default:
				results[idx] = BatchResult{Error: fmt.Errorf("unsupported operation: %s", operation.Op)}
			}
		}(i, op)
	}

	wg.Wait()
	return results
}

func (c *MemoryCache) Exists(key string) bool {
	if _, exists := c.jsonData.Load(key); exists {
		return true
	}
	return c.Type(key) != "none"
}

func (c *MemoryCache) FlushAll() {
	// Create new sync.Maps
	newSets := syncMapPool.Get().(*sync.Map)
	newHsets := syncMapPool.Get().(*sync.Map)
	newLists := syncMapPool.Get().(*sync.Map)
	newSets_ := syncMapPool.Get().(*sync.Map)
	newExpires := syncMapPool.Get().(*sync.Map)

	// Get old maps to return to pool
	oldSets := c.sets
	oldHsets := c.hsets
	oldLists := c.lists
	oldSets_ := c.sets_
	oldExpires := c.expires

	// Atomic swap to new maps
	c.sets = newSets
	c.hsets = newHsets
	c.lists = newLists
	c.sets_ = newSets_
	c.expires = newExpires

	// Clear and return old maps to pool in background
	go func() {
		if oldSets != nil {
			oldSets.Range(func(key, _ interface{}) bool {
				oldSets.Delete(key)
				return true
			})
			syncMapPool.Put(oldSets)
		}

		if oldHsets != nil {
			oldHsets.Range(func(key, _ interface{}) bool {
				oldHsets.Delete(key)
				return true
			})
			syncMapPool.Put(oldHsets)
		}

		if oldLists != nil {
			oldLists.Range(func(key, _ interface{}) bool {
				oldLists.Delete(key)
				return true
			})
			syncMapPool.Put(oldLists)
		}

		if oldSets_ != nil {
			oldSets_.Range(func(key, _ interface{}) bool {
				oldSets_.Delete(key)
				return true
			})
			syncMapPool.Put(oldSets_)
		}

		if oldExpires != nil {
			oldExpires.Range(func(key, _ interface{}) bool {
				oldExpires.Delete(key)
				return true
			})
			syncMapPool.Put(oldExpires)
		}
	}()

	// Update stats
	if c.stats != nil {
		atomic.AddInt64(&c.stats.cmdCount, 1)
	}
}

// DBSize returns the total number of keys in the cache
func (c *MemoryCache) DBSize() int {
	var total int64

	countMap := func(m *sync.Map) {
		m.Range(func(_, _ interface{}) bool {
			atomic.AddInt64(&total, 1)
			return true
		})
	}

	countMap(c.sets)
	countMap(c.hsets)
	countMap(c.lists)
	countMap(c.sets_)
	countMap(c.zsets)
	countMap(c.streams)
	countMap(c.bitmaps)
	countMap(c.jsonData)

	return int(total)
}

type Stats struct {
	startTime   time.Time
	cmdCount    int64
	evictedKeys int64
	expiredKeys int64
	totalKeys   int64
	hits        int64
	misses      int64
}

func NewStats() *Stats {
	return &Stats{
		startTime: time.Now(),
	}
}

func (s *Stats) IncrEvictedKeys() {
	atomic.AddInt64(&s.evictedKeys, 1)
}

func (s *Stats) IncrExpiredKeys() {
	atomic.AddInt64(&s.expiredKeys, 1)
}

func (s *Stats) IncrHits() {
	atomic.AddInt64(&s.hits, 1)
}

func (s *Stats) IncrMisses() {
	atomic.AddInt64(&s.misses, 1)
}

func (s *Stats) GetStats() map[string]int64 {
	return map[string]int64{
		"cmd_count":    atomic.LoadInt64(&s.cmdCount),
		"evicted_keys": atomic.LoadInt64(&s.evictedKeys),
		"expired_keys": atomic.LoadInt64(&s.expiredKeys),
		"total_keys":   atomic.LoadInt64(&s.totalKeys),
		"hits":         atomic.LoadInt64(&s.hits),
		"misses":       atomic.LoadInt64(&s.misses),
	}
}

func (c *MemoryCache) IncrCommandCount() {
	if c.stats != nil {
		atomic.AddInt64(&c.stats.cmdCount, 1)
	}
}

func (c *MemoryCache) LRem(key string, count int, value string) (int, error) {
	listI, exists := c.lists.Load(key)
	if !exists {
		return 0, nil
	}

	list := listI.(*[]string)
	removed := 0
	newList := make([]string, 0, len(*list))

	if count > 0 {
		for _, v := range *list {
			if v == value && removed < count {
				removed++
				continue
			}
			newList = append(newList, v)
		}
	} else if count < 0 {
		matches := make([]int, 0)
		for i, v := range *list {
			if v == value {
				matches = append(matches, i)
			}
		}

		removeIndices := make(map[int]bool)
		for i := 0; i < len(matches) && i < -count; i++ {
			removeIndices[matches[len(matches)-1-i]] = true
		}

		for i, v := range *list {
			if !removeIndices[i] {
				newList = append(newList, v)
			} else {
				removed++
			}
		}
	} else {
		for _, v := range *list {
			if v != value {
				newList = append(newList, v)
			} else {
				removed++
			}
		}
	}

	if removed > 0 {
		c.incrementKeyVersion(key)
	}

	if len(newList) == 0 {
		c.lists.Delete(key)
	} else {
		c.lists.Store(key, &newList)
	}

	return removed, nil
}

func (c *MemoryCache) Rename(oldKey, newKey string) error {
	val, exists := c.sets.LoadAndDelete(oldKey)
	if exists {
		c.sets.Store(newKey, val)
		if expTime, hasExp := c.expires.LoadAndDelete(oldKey); hasExp {
			c.expires.Store(newKey, expTime)
		}
		c.incrementKeyVersion(oldKey)
		c.incrementKeyVersion(newKey)
		return nil
	}

	val, exists = c.hsets.LoadAndDelete(oldKey)
	if exists {
		c.hsets.Store(newKey, val)
		c.incrementKeyVersion(oldKey)
		c.incrementKeyVersion(newKey)
		return nil
	}

	val, exists = c.lists.LoadAndDelete(oldKey)
	if exists {
		c.lists.Store(newKey, val)
		c.incrementKeyVersion(oldKey)
		c.incrementKeyVersion(newKey)
		return nil
	}

	val, exists = c.sets_.LoadAndDelete(oldKey)
	if exists {
		c.sets_.Store(newKey, val)
		c.incrementKeyVersion(oldKey)
		c.incrementKeyVersion(newKey)
		return nil
	}

	return fmt.Errorf("ERR no such key")
}

func (c *MemoryCache) Info() map[string]string {
	stats := make(map[string]string)

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	stats["uptime_in_seconds"] = fmt.Sprintf("%d", int(time.Since(c.stats.startTime).Seconds()))
	stats["total_commands_processed"] = fmt.Sprintf("%d", atomic.LoadInt64(&c.stats.cmdCount))
	stats["redis_version"] = "7.2.0"
	stats["redis_mode"] = "standalone"

	// Memory stats
	stats["used_memory"] = fmt.Sprintf("%d", memStats.Alloc)
	stats["used_memory_human"] = fmt.Sprintf("%.2fMB", float64(memStats.Alloc)/(1024*1024))
	stats["used_memory_peak"] = fmt.Sprintf("%d", memStats.TotalAlloc)
	stats["used_memory_peak_human"] = fmt.Sprintf("%.2fMB", float64(memStats.TotalAlloc)/(1024*1024))
	stats["used_memory_rss_human"] = fmt.Sprintf("%.2fMB", float64(memStats.HeapAlloc)/(1024*1024))
	stats["mem_fragmentation_ratio"] = fmt.Sprintf("%.2f", float64(memStats.Sys-memStats.Alloc)/float64(memStats.Alloc))
	stats["mem_fragmentation_bytes"] = fmt.Sprintf("%d", memStats.Sys-memStats.Alloc)
	stats["total_system_memory_human"] = fmt.Sprintf("%.2fMB", float64(memStats.Sys)/(1024*1024))
	stats["mem_allocator"] = "go"

	// Keys count
	var stringKeys, hashKeys, listKeys, setKeys, jsonKeys,
		streamKeys, bitmapKeys, zsetKeys, suggestionKeys,
		geoKeys, cmsKeys, cuckooKeys, tdigestKeys, bloomFilterKeys,
		timeseriesKeys int

	c.sets.Range(func(_, _ interface{}) bool {
		stringKeys++
		return true
	})
	stats["string_keys"] = fmt.Sprintf("%d", stringKeys)

	c.hsets.Range(func(_, _ interface{}) bool {
		hashKeys++
		return true
	})
	stats["hash_keys"] = fmt.Sprintf("%d", hashKeys)

	c.lists.Range(func(_, _ interface{}) bool {
		listKeys++
		return true
	})
	stats["list_keys"] = fmt.Sprintf("%d", listKeys)

	c.sets_.Range(func(_, _ interface{}) bool {
		setKeys++
		return true
	})
	stats["set_keys"] = fmt.Sprintf("%d", setKeys)

	c.jsonData.Range(func(_, _ interface{}) bool {
		jsonKeys++
		return true
	})
	stats["json_keys"] = fmt.Sprintf("%d", jsonKeys)

	c.streams.Range(func(_, _ interface{}) bool {
		streamKeys++
		return true
	})
	stats["stream_keys"] = fmt.Sprintf("%d", streamKeys)

	c.bitmaps.Range(func(_, _ interface{}) bool {
		bitmapKeys++
		return true
	})
	stats["bitmap_keys"] = fmt.Sprintf("%d", bitmapKeys)

	c.zsets.Range(func(_, _ interface{}) bool {
		zsetKeys++
		return true
	})
	stats["zset_keys"] = fmt.Sprintf("%d", zsetKeys)

	c.suggestions.Range(func(_, _ interface{}) bool {
		suggestionKeys++
		return true
	})
	stats["suggestion_keys"] = fmt.Sprintf("%d", suggestionKeys)

	c.geoData.Range(func(_, _ interface{}) bool {
		geoKeys++
		return true
	})
	stats["geo_keys"] = fmt.Sprintf("%d", geoKeys)

	c.cms.Range(func(_, _ interface{}) bool {
		cmsKeys++
		return true
	})
	stats["cms_keys"] = fmt.Sprintf("%d", cmsKeys)

	c.cuckooFilters.Range(func(_, _ interface{}) bool {
		cuckooKeys++
		return true
	})
	stats["cuckoo_keys"] = fmt.Sprintf("%d", cuckooKeys)

	var hllKeys int
	c.hlls.Range(func(_, _ interface{}) bool {
		hllKeys++
		return true
	})
	stats["hll_keys"] = fmt.Sprintf("%d", hllKeys)

	c.tdigests.Range(func(_, _ interface{}) bool {
		tdigestKeys++
		return true
	})
	stats["tdigest_keys"] = fmt.Sprintf("%d", tdigestKeys)

	// Count Bloom Filter keys
	c.bfilters.Range(func(_, _ interface{}) bool {
		bloomFilterKeys++
		return true
	})
	stats["bloomfilter_keys"] = fmt.Sprintf("%d", bloomFilterKeys)

	var topkKeys int
	c.topks.Range(func(_, _ interface{}) bool {
		topkKeys++
		return true
	})
	stats["topk_keys"] = fmt.Sprintf("%d", topkKeys)

	// Count TimeSeries keys
	c.timeSeries.Range(func(_, _ interface{}) bool {
		timeseriesKeys++
		return true
	})
	stats["timeseries_keys"] = fmt.Sprintf("%d", timeseriesKeys)

	// Total keys
	totalKeys := stringKeys + hashKeys + listKeys + setKeys + jsonKeys +
		streamKeys + bitmapKeys + zsetKeys + suggestionKeys +
		geoKeys + cmsKeys + cuckooKeys + hllKeys + tdigestKeys +
		bloomFilterKeys + topkKeys + timeseriesKeys
	stats["total_keys"] = fmt.Sprintf("%d", totalKeys)

	// Modules and Features
	stats["json_native_storage"] = "enabled"
	stats["json_version"] = "1.0"
	stats["modules"] = "json_native,geo,suggestion,cms,cuckoo,tdigest,bloomfilter,topk,timeseries"
	stats["timeseries_version"] = "1.0"

	// Module specific versions and info
	stats["geo_version"] = "1.0"
	stats["suggestion_version"] = "1.0"
	stats["cms_version"] = "1.0"
	stats["cuckoo_version"] = "1.0"
	stats["tdigest_version"] = "1.0"
	stats["bloomfilter_version"] = "1.0"
	stats["timeseries_version"] = "1.0"

	// Additional module capabilities
	stats["geo_search"] = "enabled"
	stats["suggestion_fuzzy"] = "enabled"
	stats["cms_merge"] = "enabled"
	stats["cuckoo_capacity"] = "enabled"
	stats["tdigest_compression"] = "enabled"
	stats["bloomfilter_scaling"] = "enabled"
	stats["timeseries_compaction"] = "enabled"

	// Module versions and details
	moduleDetails := []string{
		"name=json_native,ver=1.0,api=1.0",
		"name=geo,ver=1.0,api=1.0",
		"name=suggestion,ver=1.0,api=1.0",
		"name=cms,ver=1.0,api=1.0",
		"name=cuckoo,ver=1.0,api=1.0",
		"name=tdigest,ver=1.0,api=1.0",
		"name=bloomfilter,ver=1.0,api=1.0",
		"name=topk,ver=1.0,api=1.0",
		"name=timeseries,ver=1.0,api=1.0",
	}
	stats["module_list"] = strings.Join(moduleDetails, ",")

	return stats
}

func getGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, _ := strconv.ParseInt(idField, 10, 64)
	return id
}

func (c *MemoryCache) Multi() error {
	gid := getGoroutineID()
	if tx, ok := c.transactions.Load(gid); ok {
		if tx.(*models.Transaction).InMulti {
			return fmt.Errorf("ERR MULTI calls can not be nested")
		}
	}

	c.transactions.Store(gid, &models.Transaction{
		Commands: make([]models.Command, 0),
		Watches:  make(map[string]int64),
		InMulti:  true,
	})
	return nil
}

func (c *MemoryCache) Exec() ([]models.Value, error) {
	// Transaction için mutex kilidi
	c.defragMu.Lock()
	defer c.defragMu.Unlock()

	// Goroutine ID al
	gid := getGoroutineID()
	txI, exists := c.transactions.Load(gid) // sync.Map'den yükle
	if !exists {
		return nil, fmt.Errorf("ERR EXEC without MULTI")
	}

	tx := txI.(*models.Transaction) // Tip dönüşümü yap

	// Eğer MULTI aktif değilse
	if !tx.InMulti {
		return nil, fmt.Errorf("ERR EXEC without MULTI")
	}

	// Watch kontrolü
	if !c.checkWatches(tx) {
		c.transactions.Delete(gid) // İzlenen transaction'ı sil
		return nil, nil
	}

	// İşlem bittiğinde transaction silinir
	defer c.transactions.Delete(gid)

	results := make([]models.Value, 0, len(tx.Commands))

	// Tüm komutları sırayla çalıştır
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

		default:
			result = models.Value{Type: "error", Str: fmt.Sprintf("ERR unknown command %s", cmd.Name)}
		}

		c.incrementKeyVersion(cmd.Args[0].Bulk) // Versiyon güncelle
		results = append(results, result)
	}

	return results, nil
}

func (c *MemoryCache) Discard() error {
	gid := getGoroutineID() // Goroutine ID al
	if _, exists := c.transactions.Load(gid); !exists {
		return fmt.Errorf("ERR DISCARD without MULTI")
	}

	c.transactions.Delete(gid) // Transaction'u kaldır
	return nil
}

func (c *MemoryCache) AddToTransaction(cmd models.Command) error {
	gid := getGoroutineID()
	txI, exists := c.transactions.Load(gid) // Transaction'ı getir
	if !exists {
		return fmt.Errorf("ERR no MULTI context")
	}

	tx := txI.(*models.Transaction) // Tip dönüşümü yap
	if !tx.InMulti {
		return fmt.Errorf("ERR no MULTI context")
	}

	tx.Commands = append(tx.Commands, cmd) // Yeni komut ekle
	c.transactions.Store(gid, tx)          // Güncellenmiş transaction'ı geri koy
	return nil
}

func (c *MemoryCache) IsInTransaction() bool {
	gid := getGoroutineID()
	txI, exists := c.transactions.Load(gid) // Transaction'ı getir
	if !exists {
		return false
	}

	tx := txI.(*models.Transaction) // Tip dönüşümü yap
	return tx.InMulti               // MULTI modunda mı kontrol et
}

func (c *MemoryCache) incrementKeyVersion(key string) {
	for {
		var version int64
		oldVersionI, _ := c.keyVersions.LoadOrStore(key, version)
		oldVersion := oldVersionI.(int64)
		if c.keyVersions.CompareAndSwap(key, oldVersion, oldVersion+1) {
			break
		}
	}
}

func (c *MemoryCache) GetKeyVersion(key string) int64 {
	value, exists := c.keyVersions.Load(key) // sync.Map'den yükle
	if !exists {
		return 0 // Varsayılan versiyon
	}
	return value.(int64) // Tip dönüşümü yap
}

func (c *MemoryCache) Watch(keys ...string) error {
	gid := getGoroutineID() // İş parçacığı ID'sini al

	// Transaction'u yükle veya oluştur
	txI, _ := c.transactions.LoadOrStore(gid, &models.Transaction{
		Watches: make(map[string]int64),
	})

	tx := txI.(*models.Transaction) // Tip dönüşümü

	for _, key := range keys {
		version := c.GetKeyVersion(key) // Anahtar versiyonunu al
		tx.Watches[key] = version       // Transaction'a ekle
	}

	return nil
}

func (c *MemoryCache) Unwatch() error {
	gid := getGoroutineID()                 // Goroutine ID'yi al
	txI, exists := c.transactions.Load(gid) // Transaction'u yükle
	if !exists {
		return nil // Eğer transaction yoksa işlem yapma
	}

	tx := txI.(*models.Transaction)     // Tip dönüşümü
	tx.Watches = make(map[string]int64) // İzlenen anahtarları sıfırla
	return nil
}

func (c *MemoryCache) checkWatches(tx *models.Transaction) bool {
	for key, version := range tx.Watches {
		value, exists := c.keyVersions.Load(key) // `sync.Map`'ten mevcut versiyonu yükle
		if !exists || value.(int64) != version { // Eğer anahtar yoksa veya versiyon eşleşmiyorsa
			return false
		}
	}
	return true
}

func (c *MemoryCache) Pipeline() *models.Pipeline {
	return &models.Pipeline{
		Commands: make([]models.PipelineCommand, 0),
	}
}

func (c *MemoryCache) ExecPipeline(pl *models.Pipeline) []models.Value {
	results := make([]models.Value, 0, len(pl.Commands))

	for _, cmd := range pl.Commands {
		switch cmd.Name {
		case "SET":
			// SET komutu
			if err := c.Set(cmd.Args[0].Bulk, cmd.Args[1].Bulk); err != nil {
				results = append(results, models.Value{Type: "error", Str: err.Error()})
			} else {
				results = append(results, models.Value{Type: "string", Str: "OK"})
			}

		case "GET":
			// GET komutu
			value, exists := c.Get(cmd.Args[0].Bulk)
			if !exists {
				results = append(results, models.Value{Type: "null"})
			} else {
				results = append(results, models.Value{Type: "bulk", Bulk: value})
			}

		case "HSET":
			// HSET komutu
			if err := c.HSet(cmd.Args[0].Bulk, cmd.Args[1].Bulk, cmd.Args[2].Bulk); err != nil {
				results = append(results, models.Value{Type: "error", Str: err.Error()})
			} else {
				results = append(results, models.Value{Type: "string", Str: "OK"})
			}

		case "HGET":
			// HGET komutu
			value, exists := c.HGet(cmd.Args[0].Bulk, cmd.Args[1].Bulk)
			if !exists {
				results = append(results, models.Value{Type: "null"})
			} else {
				results = append(results, models.Value{Type: "bulk", Bulk: value})
			}
		}

		// Her bir komut için key versiyonunu artır
		c.incrementKeyVersion(cmd.Args[0].Bulk)
	}

	return results
}

func (c *MemoryCache) GetBloomFilterStats() models.BloomFilterStats {
	return c.bloomFilter.Stats()
}

func (c *MemoryCache) defragSyncMap(oldMap *sync.Map) *sync.Map {
	newMap := &sync.Map{}
	oldMap.Range(func(key, value interface{}) bool {
		newMap.Store(key, value)
		return true
	})
	return newMap
}

func (c *MemoryCache) Defragment() {
	c.defragMu.Lock()
	defer c.defragMu.Unlock()

	c.defragStrings()
	c.defragHashes()
	c.defragLists()
	c.defragSets()
	c.defragJSON()
	c.defragStreams()
	c.defragStreamGroups()
	c.defragBitmaps()

	c.defragGeoData()
	c.defragCMS()
	c.defragSuggestions()
	c.defragCuckooFilters()
	c.defragHLL()
	c.defragTDigests()
	c.defragBloomFilters()
	c.defragTopK()

	c.lastDefrag = time.Now()

	// Force GC after defragmentation
	runtime.GC()
}

func (c *MemoryCache) defragJSON() {
	c.jsonData = c.defragSyncMap(c.jsonData)
}

func (c *MemoryCache) defragStreams() {
	newStreams := &sync.Map{}
	c.streams.Range(func(key, streamI interface{}) bool {
		stream := streamI.(*sync.Map)
		newStream := c.defragSyncMap(stream)
		newStreams.Store(key, newStream)
		return true
	})
	c.streams = newStreams
}

func (c *MemoryCache) defragStreamGroups() {
	newGroups := &sync.Map{}
	c.streamGroups.Range(func(key, groupI interface{}) bool {
		group := groupI.(*sync.Map)
		newGroup := c.defragSyncMap(group)
		newGroups.Store(key, newGroup)
		return true
	})
	c.streamGroups = newGroups
}

func (c *MemoryCache) defragBitmaps() {
	newBitmaps := &sync.Map{}
	c.bitmaps.Range(func(key, bitmapI interface{}) bool {
		bitmap := bitmapI.([]byte)
		if cap(bitmap) > 2*len(bitmap) {
			newBitmap := make([]byte, len(bitmap))
			copy(newBitmap, bitmap)
			newBitmaps.Store(key, newBitmap)
		} else {
			newBitmaps.Store(key, bitmap)
		}
		return true
	})
	c.bitmaps = newBitmaps
}

func (c *MemoryCache) defragStrings() {
	c.sets = c.defragSyncMap(c.sets)
}

func (c *MemoryCache) defragHashes() {
	newHsets := &sync.Map{}
	c.hsets.Range(func(hashKey, hashMapI interface{}) bool {
		hashMap := hashMapI.(*sync.Map)
		newHashMap := c.defragSyncMap(hashMap)
		newHsets.Store(hashKey, newHashMap)
		return true
	})
	c.hsets = newHsets
}

func (c *MemoryCache) defragLists() {
	newLists := &sync.Map{}
	c.lists.Range(func(key, listI interface{}) bool {
		list := listI.(*[]string)
		if cap(*list) > 2*len(*list) {
			newList := make([]string, len(*list))
			copy(newList, *list)
			newLists.Store(key, &newList)
		} else {
			newLists.Store(key, list)
		}
		return true
	})
	c.lists = newLists
}

func (c *MemoryCache) defragSets() {
	newSets := &sync.Map{}
	c.sets_.Range(func(key, setI interface{}) bool {
		set := setI.(*sync.Map)
		newSet := c.defragSyncMap(set)
		newSets.Store(key, newSet)
		return true
	})
	c.sets_ = newSets
}

// Helper function to get memory stats for monitoring defragmentation
func (c *MemoryCache) GetDefragStats() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]interface{}{
		"last_defrag":   c.lastDefrag,
		"heap_alloc":    m.HeapAlloc,
		"heap_idle":     m.HeapIdle,
		"heap_released": m.HeapReleased,
		"heap_objects":  m.HeapObjects,
		"gc_cycles":     m.NumGC,
	}
}

// StartDefragmentation starts automatic defragmentation based on memory threshold
func (c *MemoryCache) StartDefragmentation(interval time.Duration, threshold float64) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			stats := c.GetMemoryStats()
			fragPercent := float64(stats.FragmentedBytes) / float64(stats.TotalMemory)

			if fragPercent > threshold {
				log.Printf("Starting defragmentation. Fragmentation: %.2f%%", fragPercent*100)
				c.Defragment()

				// Log stats after defragmentation
				newStats := c.GetMemoryStats() // Using GetMemoryStats for simplicity, can create GetDefragStats if needed
				log.Printf("Defragmentation completed. New heap objects: %v", newStats.HeapObjects())
			}
		}
	}()
}

// GetMemoryStats returns memory statistics
func (c *MemoryCache) GetMemoryStats() models.MemoryStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return models.MemoryStats{
		TotalMemory:      int64(ms.Sys),
		UsedMemory:       int64(ms.Alloc),
		FragmentedBytes:  int64(ms.Sys - ms.Alloc),
		LastDefrag:       c.lastDefrag,
		HeapObjectsCount: ms.HeapObjects,
	}
}

// Keys implements Redis KEYS command with optimized pattern matching
func (c *MemoryCache) Keys(matchPattern string) []string {
	// Get slice from pool
	keys := stringSlicePool.Get().([]string)
	keys = keys[:0]

	// Collect matching keys
	c.sets.Range(func(key, _ interface{}) bool {
		k := key.(string)
		if pattern.Match(matchPattern, k) {
			keys = append(keys, k)
		}
		return true
	})

	sort.Strings(keys)

	// Create final result
	result := make([]string, len(keys))
	copy(result, keys)

	// Return slice to pool
	stringSlicePool.Put(keys)

	return result
}

// Scan implements Redis SCAN command with optimized iteration over all key types
func (c *MemoryCache) Scan(cursor int, matchPattern string, count int) ([]string, int) {
	// Get keys slice from pool
	allKeys := stringSlicePool.Get().([]string)
	allKeys = allKeys[:0]

	// Collect keys from all data structures
	collectKeys := func(m *sync.Map) {
		m.Range(func(key, _ interface{}) bool {
			allKeys = append(allKeys, key.(string))
			return true
		})
	}

	// Collect keys from all data structures
	collectKeys(c.sets)
	collectKeys(c.hsets)
	collectKeys(c.lists)
	collectKeys(c.sets_)
	collectKeys(c.zsets) // zsets eklendi
	collectKeys(c.streams)
	collectKeys(c.bitmaps)
	collectKeys(c.jsonData)

	// Sort for consistent iteration
	sort.Strings(allKeys)

	if len(allKeys) == 0 {
		stringSlicePool.Put(allKeys)
		return []string{}, 0
	}

	// Normalize cursor
	if cursor < 0 || cursor >= len(allKeys) {
		cursor = 0
	}

	// Get matches slice from pool
	matches := stringSlicePool.Get().([]string)
	matches = matches[:0]

	// Collect matching keys
	nextCursor := cursor
	for i := cursor; i < len(allKeys) && len(matches) < count; i++ {
		if pattern.Match(matchPattern, allKeys[i]) {
			matches = append(matches, allKeys[i])
		}
		nextCursor = i + 1
	}

	// Reset cursor if we've reached the end
	if nextCursor >= len(allKeys) {
		nextCursor = 0
	}

	// Create final result
	result := make([]string, len(matches))
	copy(result, matches)

	// Return slices to pool
	stringSlicePool.Put(allKeys)
	stringSlicePool.Put(matches)

	log.Printf("[DEBUG] SCAN found %d keys, nextCursor: %d", len(result), nextCursor)
	return result, nextCursor
}

// ExpireAt sets an absolute Unix timestamp when the key should expire
func (c *MemoryCache) ExpireAt(key string, timestamp int64) error {
	// Check if key exists
	if !c.Exists(key) {
		return nil
	}

	// Convert Unix timestamp to time.Time
	expireTime := time.Unix(timestamp, 0)

	// Store expiration time
	c.expires.Store(key, expireTime)

	// Start a background goroutine to handle expiration
	go func() {
		timer := time.NewTimer(time.Until(expireTime))
		defer timer.Stop()

		<-timer.C

		// Check if the key still exists with the same expiration time
		if expTime, exists := c.expires.Load(key); exists {
			if expTime.(time.Time).Equal(expireTime) {
				c.Del(key)
				// Increment expired keys counter
				if c.stats != nil {
					atomic.AddInt64(&c.stats.expiredKeys, 1)
				}
			}
		}
	}()

	return nil
}

// ExpireTime returns the absolute Unix timestamp when the key will expire
// Returns:
//   - timestamp: Unix timestamp when the key will expire
//   - -1: if the key exists but has no associated expiry
//   - -2: if the key does not exist
func (c *MemoryCache) ExpireTime(key string) (int64, error) {
	// Check if key exists
	if !c.Exists(key) {
		return -2, nil
	}

	// Check if key has expiration
	expireTimeI, exists := c.expires.Load(key)
	if !exists {
		return -1, nil
	}

	expireTime := expireTimeI.(time.Time)
	// If the key has already expired, remove it and return -2
	if time.Now().After(expireTime) {
		go func() {
			c.Del(key)
			if c.stats != nil {
				atomic.AddInt64(&c.stats.expiredKeys, 1)
			}
		}()
		return -2, nil
	}

	return expireTime.Unix(), nil
}

func (c *MemoryCache) LIndex(key string, index int) (string, bool) {
	// Load the list from sync.Map
	listI, exists := c.lists.Load(key)
	if !exists {
		return "", false
	}

	list := listI.(*[]string)
	length := len(*list)

	// Handle negative indices by converting to positive
	if index < 0 {
		index = length + index
	}

	// Check bounds
	if index < 0 || index >= length {
		return "", false
	}

	// Return element at index
	return (*list)[index], true
}

func (c *MemoryCache) LInsert(key string, before bool, pivot string, value string) (int, error) {
	for {
		// Load or create the list
		listI, exists := c.lists.Load(key)
		if !exists {
			return 0, nil // Return 0 if key doesn't exist
		}

		list := listI.(*[]string)
		pivotIndex := -1

		// Find pivot element
		for i, element := range *list {
			if element == pivot {
				pivotIndex = i
				break
			}
		}

		// If pivot wasn't found, return -1
		if pivotIndex == -1 {
			return -1, nil
		}

		// Create new list with appropriate capacity
		newList := make([]string, len(*list)+1)

		if before {
			// Copy elements before pivot
			copy(newList, (*list)[:pivotIndex])
			// Insert new value
			newList[pivotIndex] = value
			// Copy remaining elements
			copy(newList[pivotIndex+1:], (*list)[pivotIndex:])
		} else {
			// Copy elements up to and including pivot
			copy(newList, (*list)[:pivotIndex+1])
			// Insert new value
			newList[pivotIndex+1] = value
			// Copy remaining elements
			copy(newList[pivotIndex+2:], (*list)[pivotIndex+1:])
		}

		// Try to update the list atomically
		if c.lists.CompareAndSwap(key, listI, &newList) {
			c.incrementKeyVersion(key)
			return len(newList), nil
		}
	}
}

// LPOS returns the index of the first matching element in a list
func (c *MemoryCache) LPos(key string, element string) (int, bool) {
	listI, exists := c.lists.Load(key)
	if !exists {
		return 0, false
	}

	list := listI.(*[]string)
	for i, value := range *list {
		if value == element {
			return i, true
		}
	}
	return 0, false
}

// LPUSHX inserts elements at the head of the list only if the list exists
func (c *MemoryCache) LPushX(key string, value string) (int, error) {
	// First check if list exists
	if !c.Exists(key) {
		return 0, nil
	}

	// If it exists, use standard LPush
	return c.LPush(key, value)
}

// RPUSHX inserts elements at the tail of the list only if the list exists
func (c *MemoryCache) RPushX(key string, value string) (int, error) {
	// First check if list exists
	if !c.Exists(key) {
		return 0, nil
	}

	// If it exists, use standard RPush
	return c.RPush(key, value)
}

// LTRIM trims a list to the specified range
func (c *MemoryCache) LTrim(key string, start int, stop int) error {
	for {
		listI, exists := c.lists.Load(key)
		if !exists {
			return nil
		}

		list := listI.(*[]string)
		length := len(*list)

		// Convert negative indices to positive
		if start < 0 {
			start = length + start
		}
		if stop < 0 {
			stop = length + stop
		}

		// Boundary checks
		if start < 0 {
			start = 0
		}
		if stop >= length {
			stop = length - 1
		}
		if start > stop {
			// Empty the list if start > stop
			c.lists.Delete(key)
			c.incrementKeyVersion(key)
			return nil
		}

		// Create new list with trimmed values
		newList := make([]string, stop-start+1)
		copy(newList, (*list)[start:stop+1])

		// Try to update atomically
		if c.lists.CompareAndSwap(key, listI, &newList) {
			c.incrementKeyVersion(key)

			// If list is empty after trim, remove it
			if len(newList) == 0 {
				c.lists.Delete(key)
			}
			return nil
		}
	}
}

func (c *MemoryCache) XAdd(key string, id string, fields map[string]string) error {
	streamI, _ := c.streams.LoadOrStore(key, &sync.Map{})
	streamMap := streamI.(*sync.Map)

	entry := &models.StreamEntry{
		ID:     id,
		Fields: fields,
	}

	streamMap.Store(id, entry)
	c.incrementKeyVersion(key)

	return nil
}

func (c *MemoryCache) XACK(key, group string, ids ...string) (int64, error) {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return 0, nil
	}
	stream := streamI.(*sync.Map)

	acked := int64(0)
	for _, id := range ids {
		if _, exists := stream.Load(id); exists {
			acked++
		}
	}

	c.incrementKeyVersion(key)
	return acked, nil
}

func (c *MemoryCache) XDEL(key string, ids ...string) (int64, error) {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return 0, nil
	}
	stream := streamI.(*sync.Map)

	deleted := int64(0)
	for _, id := range ids {
		if _, ok := stream.LoadAndDelete(id); ok {
			deleted++
		}
	}

	c.incrementKeyVersion(key)
	return deleted, nil
}

func (c *MemoryCache) XAutoClaim(key, group, consumer string, minIdleTime int64, start string, count int) ([]string, []models.StreamEntry, string, error) {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return nil, nil, "0-0", nil
	}

	stream := streamI.(*sync.Map)
	var claimed []string
	var entries []models.StreamEntry
	var cursor = start

	stream.Range(func(key, value interface{}) bool {
		id := key.(string)
		if id > start && len(claimed) < count {
			entry := value.(*models.StreamEntry)
			claimed = append(claimed, id)
			entries = append(entries, *entry)
			cursor = id
		}
		return len(claimed) < count
	})

	c.incrementKeyVersion(key)
	return claimed, entries, cursor, nil
}

func (c *MemoryCache) XClaim(key, group, consumer string, minIdleTime int64, ids ...string) ([]models.StreamEntry, error) {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return nil, nil
	}

	stream := streamI.(*sync.Map)
	var entries []models.StreamEntry

	for _, id := range ids {
		if entryI, ok := stream.Load(id); ok {
			entries = append(entries, *entryI.(*models.StreamEntry))
		}
	}

	c.incrementKeyVersion(key)
	return entries, nil
}

func (c *MemoryCache) XLEN(key string) int64 {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return 0
	}

	stream := streamI.(*sync.Map)
	var count int64
	stream.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

func (c *MemoryCache) XPENDING(key, group string) (int64, error) {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return 0, nil
	}

	stream := streamI.(*sync.Map)
	var count int64
	stream.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count, nil
}

func (c *MemoryCache) XRANGE(key, start, end string, count int) ([]models.StreamEntry, error) {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return nil, nil
	}

	stream := streamI.(*sync.Map)
	var entries []models.StreamEntry

	stream.Range(func(k, v interface{}) bool {
		id := k.(string)
		if (start == "-" || id >= start) && (end == "+" || id <= end) {
			entries = append(entries, *v.(*models.StreamEntry))
			if count > 0 && len(entries) >= count {
				return false
			}
		}
		return true
	})

	return entries, nil
}

func (c *MemoryCache) XREAD(keys []string, ids []string, count int) (map[string][]models.StreamEntry, error) {
	result := make(map[string][]models.StreamEntry)

	if len(keys) != len(ids) {
		return nil, fmt.Errorf("XREAD: keys and ids slices must have the same length")
	}

	for i, key := range keys {
		log.Printf("XREAD: Processing key=%s, startID=%s", key, ids[i]) // Added logging

		streamI, exists := c.streams.Load(key)
		if !exists {
			log.Printf("XREAD: Stream not found for key=%s", key) // Added logging
			continue
		}

		streamMap, ok := streamI.(*sync.Map)
		if !ok {
			return nil, fmt.Errorf("XREAD: Unexpected type for stream: %T", streamI)
		}

		var entries []models.StreamEntry
		startID := ids[i]

		streamMap.Range(func(k, v interface{}) bool {
			id, ok := k.(string)
			if !ok {
				log.Printf("XREAD: Unexpected key type in stream: %T", k) // Added logging
				return true                                               // Continue to the next entry
			}

			entryPtr, ok := v.(*models.StreamEntry)
			if !ok {
				log.Printf("XREAD: Unexpected value type in stream: %T for key=%s", v, id) // Added logging
				return true                                                                // Continue to the next entry
			}
			entry := *entryPtr

			if id > startID {
				entries = append(entries, entry)
				if count > 0 && len(entries) >= count {
					return false
				}
			}
			return true
		})

		if len(entries) > 0 {
			result[key] = entries
			log.Printf("XREAD: Found %d entries for key=%s", len(entries), key) // Added logging
		} else {
			log.Printf("XREAD: No entries found for key=%s after ID=%s", key, startID) // Added logging
		}
	}

	return result, nil
}

func (c *MemoryCache) XREVRANGE(key, start, end string, count int) ([]models.StreamEntry, error) {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return nil, nil
	}

	stream := streamI.(*sync.Map)
	var entries []models.StreamEntry

	// Collect entries
	stream.Range(func(k, v interface{}) bool {
		id := k.(string)
		if (start == "+" || id <= start) && (end == "-" || id >= end) {
			entries = append(entries, *v.(*models.StreamEntry))
			if count > 0 && len(entries) >= count {
				return false
			}
		}
		return true
	})

	// Reverse the order
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}

	return entries, nil
}

func (c *MemoryCache) XSETID(key string, id string) error {
	if _, exists := c.streams.Load(key); !exists {
		return fmt.Errorf("ERR no such key")
	}
	c.incrementKeyVersion(key)
	return nil
}
func (c *MemoryCache) XTRIM(key string, strategy string, threshold int64) (int64, error) {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return 0, nil
	}

	stream := streamI.(*sync.Map)
	var entries []struct {
		id    string
		entry *models.StreamEntry
	}

	// Collect all entries
	stream.Range(func(k, v interface{}) bool {
		entries = append(entries, struct {
			id    string
			entry *models.StreamEntry
		}{
			id:    k.(string),
			entry: v.(*models.StreamEntry),
		})
		return true
	})

	// Sort by ID
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].id < entries[j].id
	})

	var trimmed int64
	if len(entries) > int(threshold) {
		for i := 0; i < len(entries)-int(threshold); i++ {
			stream.Delete(entries[i].id)
			trimmed++
		}
	}

	if trimmed > 0 {
		c.incrementKeyVersion(key)
	}

	return trimmed, nil
}

func (c *MemoryCache) XInfoGroups(key string) ([]models.StreamGroup, error) {
	if _, exists := c.streams.Load(key); !exists {
		return nil, fmt.Errorf("ERR no such key")
	}
	return []models.StreamGroup{}, nil
}

func (c *MemoryCache) XInfoConsumers(key, group string) ([]models.StreamConsumer, error) {
	if _, exists := c.streams.Load(key); !exists {
		return nil, fmt.Errorf("ERR no such key")
	}
	return []models.StreamConsumer{}, nil
}

func (c *MemoryCache) XInfoStream(key string) (*models.StreamInfo, error) {
	streamI, exists := c.streams.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR no such key")
	}

	stream := streamI.(*sync.Map)
	var length int64
	stream.Range(func(_, _ interface{}) bool {
		length++
		return true
	})

	info := &models.StreamInfo{
		Length: length,
		Groups: 0,
	}
	return info, nil
}

func (c *MemoryCache) XGroupCreate(key, group, id string) error {
	if _, exists := c.streams.Load(key); !exists {
		return fmt.Errorf("ERR no such key")
	}

	groupsI, _ := c.streamGroups.LoadOrStore(key, &sync.Map{})
	groups := groupsI.(*sync.Map)

	if _, exists := groups.Load(group); exists {
		return fmt.Errorf("ERR BUSYGROUP Consumer Group name already exists")
	}

	newGroup := &models.StreamConsumerGroup{
		Consumers: make(map[string]*models.StreamConsumer),
		LastID:    id,
		Pending:   make(map[string]*models.PendingMessage),
	}

	groups.Store(group, newGroup)
	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) XGroupCreateConsumer(key, group, consumer string) (int64, error) {
	groupsI, exists := c.streamGroups.Load(key)
	if !exists {
		return 0, fmt.Errorf("ERR no such key")
	}

	groups := groupsI.(*sync.Map)
	groupI, exists := groups.Load(group)
	if !exists {
		return 0, fmt.Errorf("ERR no such group")
	}

	streamGroup := groupI.(*models.StreamConsumerGroup)
	if _, exists := streamGroup.Consumers[consumer]; exists {
		return 0, nil
	}

	streamGroup.Consumers[consumer] = &models.StreamConsumer{
		Name:     consumer,
		Pending:  0,
		IdleTime: 0,
	}

	c.incrementKeyVersion(key)
	return 1, nil
}

func (c *MemoryCache) XGroupDelConsumer(key, group, consumer string) (int64, error) {
	groupsI, exists := c.streamGroups.Load(key)
	if !exists {
		return 0, fmt.Errorf("ERR no such key")
	}

	groups := groupsI.(*sync.Map)
	groupI, exists := groups.Load(group)
	if !exists {
		return 0, fmt.Errorf("ERR no such group")
	}

	streamGroup := groupI.(*models.StreamConsumerGroup)
	if _, exists := streamGroup.Consumers[consumer]; !exists {
		return 0, nil
	}

	pendingCount := int64(0)
	for _, msg := range streamGroup.Pending {
		if msg.Consumer == consumer {
			pendingCount++
		}
	}

	delete(streamGroup.Consumers, consumer)
	c.incrementKeyVersion(key)
	return pendingCount, nil
}

func (c *MemoryCache) XGroupDestroy(key, group string) (int64, error) {
	groupsI, exists := c.streamGroups.Load(key)
	if !exists {
		return 0, fmt.Errorf("ERR no such key")
	}

	groups := groupsI.(*sync.Map)
	if _, exists := groups.LoadAndDelete(group); !exists {
		return 0, nil
	}

	c.incrementKeyVersion(key)
	return 1, nil
}

func (c *MemoryCache) XGroupSetID(key, group, id string) error {
	groupsI, exists := c.streamGroups.Load(key)
	if !exists {
		return fmt.Errorf("ERR no such key")
	}

	groups := groupsI.(*sync.Map)
	groupI, exists := groups.Load(group)
	if !exists {
		return fmt.Errorf("ERR no such group")
	}

	streamGroup := groupI.(*models.StreamConsumerGroup)
	streamGroup.LastID = id

	c.incrementKeyVersion(key)
	return nil
}

func (c *MemoryCache) WithRetry(strategy models.RetryStrategy) ports.Cache {
	return NewRetryDecorator(c, strategy)
}
