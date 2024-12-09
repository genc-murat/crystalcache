package cache

import (
	"errors"
	"fmt"
	"log"
	"math/bits"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/pkg/utils/hash"
	"github.com/genc-murat/crystalcache/pkg/utils/pattern"
)

type MemoryCache struct {
	sets         *sync.Map // string key-value pairs
	hsets        *sync.Map // hash maps
	lists        *sync.Map // lists
	sets_        *sync.Map // sets
	expires      *sync.Map // expiration times
	stats        *Stats
	transactions *sync.Map
	keyVersions  *sync.Map
	zsets        *sync.Map
	hlls         *sync.Map
	jsonData     *sync.Map
	streams      *sync.Map // stream entries
	streamGroups *sync.Map // stream consumer groups
	bitmaps      *sync.Map
	bloomFilter  *models.BloomFilter
	lastDefrag   time.Time
	defragMu     sync.Mutex
}

func NewMemoryCache() *MemoryCache {
	config := models.BloomFilterConfig{
		ExpectedItems:     1000000,
		FalsePositiveRate: 0.01,
	}

	mc := &MemoryCache{
		sets:         &sync.Map{},
		hsets:        &sync.Map{},
		lists:        &sync.Map{},
		sets_:        &sync.Map{},
		expires:      &sync.Map{},
		stats:        NewStats(),
		transactions: &sync.Map{},
		keyVersions:  &sync.Map{},
		zsets:        &sync.Map{},
		hlls:         &sync.Map{},
		jsonData:     &sync.Map{},
		streams:      &sync.Map{},
		streamGroups: &sync.Map{},
		bitmaps:      &sync.Map{},
		bloomFilter:  models.NewBloomFilter(config),
	}

	// Start background cleanup
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			mc.cleanExpired()
		}
	}()

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

	if deleted {
		c.incrementKeyVersion(key)
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

func (c *MemoryCache) HSet(hash string, key string, value string) error {
	var hashMap sync.Map
	actual, _ := c.hsets.LoadOrStore(hash, &hashMap)
	actualMap := actual.(*sync.Map)
	actualMap.Store(key, value)
	c.incrementKeyVersion(hash)
	return nil
}

func (c *MemoryCache) HGet(hash string, key string) (string, bool) {
	if hashMapI, ok := c.hsets.Load(hash); ok {
		hashMap := hashMapI.(*sync.Map)
		if value, ok := hashMap.Load(key); ok {
			return value.(string), true
		}
	}
	return "", false
}

func (c *MemoryCache) HGetAll(hash string) map[string]string {
	result := make(map[string]string)
	if hashMapI, ok := c.hsets.Load(hash); ok {
		hashMap := hashMapI.(*sync.Map)
		hashMap.Range(func(key, value interface{}) bool {
			result[key.(string)] = value.(string)
			return true
		})
	}
	return result
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

func (c *MemoryCache) SAdd(key string, member string) (bool, error) {
	var set sync.Map
	actual, _ := c.sets_.LoadOrStore(key, &set)
	actualSet := actual.(*sync.Map)

	_, loaded := actualSet.LoadOrStore(member, true)
	if !loaded {
		c.incrementKeyVersion(key)
		return true, nil
	}
	return false, nil
}

// Set Operations
func (c *MemoryCache) SMembers(key string) ([]string, error) {
	members := make([]string, 0)
	if setI, ok := c.sets_.Load(key); ok {
		set := setI.(*sync.Map)
		set.Range(func(key, _ interface{}) bool {
			members = append(members, key.(string))
			return true
		})
	}
	sort.Strings(members)
	return members, nil
}

func (c *MemoryCache) SCard(key string) int {
	count := 0
	if setI, ok := c.sets_.Load(key); ok {
		set := setI.(*sync.Map)
		set.Range(func(_, _ interface{}) bool {
			count++
			return true
		})
	}
	return count
}

func (c *MemoryCache) SRem(key string, member string) (bool, error) {
	if setI, ok := c.sets_.Load(key); ok {
		set := setI.(*sync.Map)
		if _, exists := set.LoadAndDelete(member); exists {
			c.incrementKeyVersion(key)

			// Check if set is empty
			empty := true
			set.Range(func(_, _ interface{}) bool {
				empty = false
				return false
			})

			if empty {
				c.sets_.Delete(key)
			}

			return true, nil
		}
	}
	return false, nil
}

func (c *MemoryCache) SIsMember(key string, member string) bool {
	if setI, ok := c.sets_.Load(key); ok {
		set := setI.(*sync.Map)
		_, exists := set.Load(member)
		return exists
	}
	return false
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

// Set Intersection and Union
func (c *MemoryCache) SInter(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	// Get first set
	firstSetI, exists := c.sets_.Load(keys[0])
	if !exists {
		return []string{}
	}

	result := make(map[string]bool)
	firstSet := firstSetI.(*sync.Map)
	firstSet.Range(func(key, _ interface{}) bool {
		result[key.(string)] = true
		return true
	})

	// Intersect with other sets
	for _, key := range keys[1:] {
		setI, exists := c.sets_.Load(key)
		if !exists {
			return []string{}
		}

		set := setI.(*sync.Map)
		toDelete := make([]string, 0)

		for member := range result {
			if _, exists := set.Load(member); !exists {
				toDelete = append(toDelete, member)
			}
		}

		for _, member := range toDelete {
			delete(result, member)
		}
	}

	intersection := make([]string, 0, len(result))
	for member := range result {
		intersection = append(intersection, member)
	}
	sort.Strings(intersection)
	return intersection
}

func (c *MemoryCache) SUnion(keys ...string) []string {
	result := make(map[string]bool)

	for _, key := range keys {
		if setI, exists := c.sets_.Load(key); exists {
			set := setI.(*sync.Map)
			set.Range(func(key, _ interface{}) bool {
				result[key.(string)] = true
				return true
			})
		}
	}

	union := make([]string, 0, len(result))
	for member := range result {
		union = append(union, member)
	}
	sort.Strings(union)
	return union
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

func (c *MemoryCache) SDiff(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	firstSetI, exists := c.sets_.Load(keys[0])
	if !exists {
		return []string{}
	}

	result := make(map[string]bool)
	firstSet := firstSetI.(*sync.Map)
	firstSet.Range(func(key, _ interface{}) bool {
		result[key.(string)] = true
		return true
	})

	for _, key := range keys[1:] {
		setI, exists := c.sets_.Load(key)
		if !exists {
			continue
		}

		set := setI.(*sync.Map)
		set.Range(func(member, _ interface{}) bool {
			delete(result, member.(string))
			return true
		})
	}

	diff := make([]string, 0, len(result))
	for member := range result {
		diff = append(diff, member)
	}
	sort.Strings(diff)
	return diff
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

	stats["used_memory"] = fmt.Sprintf("%d", memStats.Alloc)
	stats["used_memory_human"] = fmt.Sprintf("%.2fMB", float64(memStats.Alloc)/(1024*1024))
	stats["used_memory_peak"] = fmt.Sprintf("%d", memStats.TotalAlloc)
	stats["used_memory_peak_human"] = fmt.Sprintf("%.2fMB", float64(memStats.TotalAlloc)/(1024*1024))
	stats["used_memory_rss_human"] = fmt.Sprintf("%.2fMB", float64(memStats.HeapAlloc)/(1024*1024))
	stats["mem_fragmentation_ratio"] = fmt.Sprintf("%.2f", float64(memStats.Sys-memStats.Alloc)/float64(memStats.Alloc))
	stats["mem_fragmentation_bytes"] = fmt.Sprintf("%d", memStats.Sys-memStats.Alloc)
	stats["total_system_memory_human"] = fmt.Sprintf("%.2fMB", float64(memStats.Sys)/(1024*1024))
	stats["mem_allocator"] = "go"

	var stringKeys, hashKeys, listKeys, setKeys, jsonKeys, streamKeys, bitmapKeys int

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

	stats["total_keys"] = fmt.Sprintf("%d", stringKeys+hashKeys+listKeys+setKeys+jsonKeys+streamKeys+bitmapKeys)

	stats["json_native_storage"] = "enabled"
	stats["json_version"] = "1.0"
	stats["modules"] = "json_native"

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

func (c *MemoryCache) PFAdd(key string, elements ...string) (bool, error) {
	value, _ := c.hlls.LoadOrStore(key, models.NewHyperLogLog())
	hll := value.(*models.HyperLogLog)

	modified := false
	for _, element := range elements {
		// Use the renamed function
		hashValue := hash.Hash64([]byte(element))
		if hll.Add(hashValue) {
			modified = true
		}
	}

	return modified, nil
}

func (c *MemoryCache) PFCount(keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	if len(keys) == 1 {
		if value, exists := c.hlls.Load(keys[0]); exists {
			hll := value.(*models.HyperLogLog)
			return hll.Size, nil
		}
		return 0, nil
	}

	merged := models.NewHyperLogLog()
	for _, key := range keys {
		if value, exists := c.hlls.Load(key); exists {
			hll := value.(*models.HyperLogLog)
			merged.Merge(hll)
		}
	}

	return merged.Size, nil
}

func (c *MemoryCache) PFMerge(destKey string, sourceKeys ...string) error {
	merged := models.NewHyperLogLog()

	for _, key := range sourceKeys {
		if value, exists := c.hlls.Load(key); exists {
			hll := value.(*models.HyperLogLog)
			merged.Merge(hll)
		}
	}

	c.hlls.Store(destKey, merged)
	return nil
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

func (c *MemoryCache) Defragment() {
	c.defragMu.Lock()
	defer c.defragMu.Unlock()

	// Existing defragmentation
	c.defragStrings()
	c.defragHashes()
	c.defragLists()
	c.defragSets()

	// New defragmentation
	c.defragJSON()
	c.defragStreams()
	c.defragStreamGroups()
	c.defragBitmaps()

	c.lastDefrag = time.Now()

	// Force GC after defragmentation
	runtime.GC()
}

func (c *MemoryCache) defragJSON() {
	newJSON := &sync.Map{}

	c.jsonData.Range(func(key, value interface{}) bool {
		newJSON.Store(key, value)
		return true
	})

	c.jsonData = newJSON
}

func (c *MemoryCache) defragStreams() {
	newStreams := &sync.Map{}

	c.streams.Range(func(key, streamI interface{}) bool {
		stream := streamI.(*sync.Map)
		newStream := &sync.Map{}

		// Copy all stream entries
		stream.Range(func(entryID, entryData interface{}) bool {
			newStream.Store(entryID, entryData)
			return true
		})

		newStreams.Store(key, newStream)
		return true
	})

	c.streams = newStreams
}

func (c *MemoryCache) defragStreamGroups() {
	newGroups := &sync.Map{}

	c.streamGroups.Range(func(key, groupI interface{}) bool {
		group := groupI.(*sync.Map)
		newGroup := &sync.Map{}

		// Copy all consumer group data
		group.Range(func(consumerKey, consumerData interface{}) bool {
			newGroup.Store(consumerKey, consumerData)
			return true
		})

		newGroups.Store(key, newGroup)
		return true
	})

	c.streamGroups = newGroups
}

func (c *MemoryCache) defragBitmaps() {
	newBitmaps := &sync.Map{}

	c.bitmaps.Range(func(key, bitmapI interface{}) bool {
		bitmap := bitmapI.([]byte)

		// Only defrag if capacity is more than twice the length
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
	// Create new sync.Map for strings
	newSets := &sync.Map{}

	// Copy all key-value pairs to new map
	c.sets.Range(func(key, value interface{}) bool {
		newSets.Store(key, value)
		return true
	})

	// Replace old map with new one
	c.sets = newSets
}

func (c *MemoryCache) defragHashes() {
	// Create new sync.Map for hashes
	newHsets := &sync.Map{}

	// Iterate through all hash maps
	c.hsets.Range(func(hashKey, hashMapI interface{}) bool {
		hashMap := hashMapI.(*sync.Map)
		newHashMap := &sync.Map{}

		// Copy all fields to new hash map
		hashMap.Range(func(fieldKey, fieldValue interface{}) bool {
			newHashMap.Store(fieldKey, fieldValue)
			return true
		})

		// Store new hash map in new hsets
		newHsets.Store(hashKey, newHashMap)
		return true
	})

	// Replace old map with new one
	c.hsets = newHsets
}

func (c *MemoryCache) defragLists() {
	// Create new sync.Map for lists
	newLists := &sync.Map{}

	// Iterate through all lists
	c.lists.Range(func(key, listI interface{}) bool {
		list := listI.(*[]string)

		// Only defrag if capacity is more than twice the length
		if cap(*list) > 2*len(*list) {
			newList := make([]string, len(*list))
			copy(newList, *list)
			newLists.Store(key, &newList)
		} else {
			newLists.Store(key, list)
		}
		return true
	})

	// Replace old map with new one
	c.lists = newLists
}

func (c *MemoryCache) defragSets() {
	// Create new sync.Map for sets
	newSets := &sync.Map{}

	// Iterate through all sets
	c.sets_.Range(func(key, setI interface{}) bool {
		set := setI.(*sync.Map)
		newSet := &sync.Map{}

		// Copy all members to new set
		set.Range(func(member, _ interface{}) bool {
			newSet.Store(member, true)
			return true
		})

		// Store new set in new sets
		newSets.Store(key, newSet)
		return true
	})

	// Replace old map with new one
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
				newStats := c.GetDefragStats()
				log.Printf("Defragmentation completed. New heap objects: %v", newStats["heap_objects"])
			}
		}
	}()
}

// Memory istatistiklerini getir
func (c *MemoryCache) GetMemoryStats() models.MemoryStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return models.MemoryStats{
		TotalMemory:     int64(ms.Sys),
		UsedMemory:      int64(ms.Alloc),
		FragmentedBytes: int64(ms.Sys - ms.Alloc),
		LastDefrag:      c.lastDefrag,
	}
}

// HScan implements Redis HSCAN command with optimized pattern matching
func (c *MemoryCache) HScan(hash string, cursor int, matchPattern string, count int) ([]string, int) {
	hashMapI, exists := c.hsets.Load(hash)
	if !exists {
		return []string{}, 0
	}
	hashMap := hashMapI.(*sync.Map)

	// Get fields slice from pool
	fields := stringSlicePool.Get().([]string)
	fields = fields[:0] // Reset slice keeping capacity

	// Collect matching fields
	hashMap.Range(func(key, _ interface{}) bool {
		field := key.(string)
		if pattern.Match(matchPattern, field) {
			fields = append(fields, field)
		}
		return true
	})
	sort.Strings(fields)

	// Check cursor bounds
	if cursor >= len(fields) {
		stringSlicePool.Put(fields)
		return []string{}, 0
	}

	// Get result slice from pool
	result := stringSlicePool.Get().([]string)
	result = result[:0]

	// Collect results with field-value pairs
	nextCursor := cursor
	for i := cursor; i < len(fields) && len(result) < count*2; i++ {
		field := fields[i]
		if value, ok := hashMap.Load(field); ok {
			result = append(result, field, value.(string))
		}
		nextCursor = i + 1
	}

	// Reset cursor if we've reached the end
	if nextCursor >= len(fields) {
		nextCursor = 0
	}

	// Create final result
	finalResult := make([]string, len(result))
	copy(finalResult, result)

	// Return slices to pool
	stringSlicePool.Put(fields)
	stringSlicePool.Put(result)

	return finalResult, nextCursor
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
	collectKeys(c.sets_)
	collectKeys(c.hsets)
	collectKeys(c.lists)

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

func (c *MemoryCache) HDel(hash string, field string) (bool, error) {
	hashMapI, exists := c.hsets.Load(hash)
	if !exists {
		return false, nil
	}

	hashMap := hashMapI.(*sync.Map)
	if _, exists := hashMap.LoadAndDelete(field); !exists {
		return false, nil
	}

	// Check if hash is empty after deletion
	empty := true
	hashMap.Range(func(_, _ interface{}) bool {
		empty = false
		return false // Stop iteration at first key
	})

	// If hash is empty, remove it completely
	if empty {
		c.hsets.Delete(hash)
		// Return the empty sync.Map to a pool if you maintain one
		syncMapPool.Put(hashMap)
	}

	c.incrementKeyVersion(hash)
	return true, nil
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

// HIncrBy increments the integer value of a hash field by the given increment
func (c *MemoryCache) HIncrBy(key, field string, increment int64) (int64, error) {
	var hashMap sync.Map
	actual, _ := c.hsets.LoadOrStore(key, &hashMap)
	actualMap := actual.(*sync.Map)

	for {
		// Get current value
		currentI, _ := actualMap.LoadOrStore(field, "0")
		current := currentI.(string)

		// Convert current value to int64
		currentVal, err := strconv.ParseInt(current, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not an integer")
		}

		// Calculate new value
		newVal := currentVal + increment

		// Try to store new value
		if actualMap.CompareAndSwap(field, current, strconv.FormatInt(newVal, 10)) {
			c.incrementKeyVersion(key)
			return newVal, nil
		}
	}
}

// HIncrByFloat increments the float value of a hash field by the given increment
func (c *MemoryCache) HIncrByFloat(key, field string, increment float64) (float64, error) {
	var hashMap sync.Map
	actual, _ := c.hsets.LoadOrStore(key, &hashMap)
	actualMap := actual.(*sync.Map)

	for {
		// Get current value
		currentI, _ := actualMap.LoadOrStore(field, "0")
		current := currentI.(string)

		// Convert current value to float64
		currentVal, err := strconv.ParseFloat(current, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not a float")
		}

		// Calculate new value
		newVal := currentVal + increment

		// Format new value with maximum precision
		newValStr := strconv.FormatFloat(newVal, 'f', -1, 64)

		// Try to store new value
		if actualMap.CompareAndSwap(field, current, newValStr) {
			c.incrementKeyVersion(key)
			return newVal, nil
		}
	}
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

// LIndex returns an element from a list by its index with retry logic
func (rd *RetryDecorator) LIndex(key string, index int) (string, bool) {
	var value string
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		value, exists = rd.cache.LIndex(key, index)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("index out of range")
	})

	if err != nil {
		return "", false
	}
	return value, finalExists
}

// LInsert inserts an element before or after a pivot in a list with retry logic
func (rd *RetryDecorator) LInsert(key string, before bool, pivot string, value string) (int, error) {
	var length int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		length, err = rd.cache.LInsert(key, before, pivot, value)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return length, finalErr
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
	// Ensure streams map exists
	var stream sync.Map
	streamI, _ := c.streams.LoadOrStore(key, &stream)
	streamMap := streamI.(*sync.Map)

	// Store entry
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

	for i, key := range keys {
		streamI, exists := c.streams.Load(key)
		if !exists {
			continue
		}

		stream := streamI.(*sync.Map)
		var entries []models.StreamEntry
		startID := ids[i]

		stream.Range(func(k, v interface{}) bool {
			id := k.(string)
			if id > startID {
				entries = append(entries, *v.(*models.StreamEntry))
				if count > 0 && len(entries) >= count {
					return false
				}
			}
			return true
		})

		if len(entries) > 0 {
			result[key] = entries
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

func (c *MemoryCache) GetBit(key string, offset int64) (int, error) {
	val, exists := c.bitmaps.Load(key)
	if !exists {
		return 0, nil
	}

	valBytes := val.([]byte)
	byteIndex := offset / 8
	if int64(len(valBytes)) <= byteIndex {
		return 0, nil
	}

	bitIndex := offset % 8
	return int((valBytes[byteIndex] >> (7 - bitIndex)) & 1), nil
}

func (c *MemoryCache) SetBit(key string, offset int64, value int) (int, error) {
	if value != 0 && value != 1 {
		return 0, fmt.Errorf("ERR bit value must be 0 or 1")
	}

	valI, _ := c.bitmaps.LoadOrStore(key, make([]byte, 0))
	valBytes := valI.([]byte)

	byteIndex := offset / 8
	bitIndex := offset % 8

	if int64(len(valBytes)) <= byteIndex {
		newBytes := make([]byte, byteIndex+1)
		copy(newBytes, valBytes)
		valBytes = newBytes
	}

	oldBit := (valBytes[byteIndex] >> (7 - bitIndex)) & 1
	if value == 1 {
		valBytes[byteIndex] |= 1 << (7 - bitIndex)
	} else {
		valBytes[byteIndex] &= ^(1 << (7 - bitIndex))
	}

	c.bitmaps.Store(key, valBytes)
	c.incrementKeyVersion(key)

	return int(oldBit), nil
}

func (c *MemoryCache) BitCount(key string, start, end int64) (int64, error) {
	val, exists := c.bitmaps.Load(key)
	if !exists {
		return 0, nil
	}

	bytes := val.([]byte)
	if start < 0 {
		start = int64(len(bytes)) + start
	}
	if end < 0 {
		end = int64(len(bytes)) + end
	}

	if start < 0 {
		start = 0
	}
	if end >= int64(len(bytes)) {
		end = int64(len(bytes)) - 1
	}

	var count int64
	for i := start; i <= end; i++ {
		count += int64(bits.OnesCount8(bytes[i]))
	}
	return count, nil
}

func (c *MemoryCache) BitField(key string, commands []models.BitFieldCommand) ([]int64, error) {
	results := make([]int64, len(commands))
	for i, cmd := range commands {
		switch cmd.Op {
		case "GET":
			val, err := c.bitfieldGet(key, cmd.Type, cmd.Offset)
			if err != nil {
				return nil, err
			}
			results[i] = val
		case "SET":
			oldVal, err := c.bitfieldSet(key, cmd.Type, cmd.Offset, cmd.Value)
			if err != nil {
				return nil, err
			}
			results[i] = oldVal
		case "INCRBY":
			val, err := c.bitfieldIncrBy(key, cmd.Type, cmd.Offset, cmd.Increment)
			if err != nil {
				return nil, err
			}
			results[i] = val
		}
	}
	return results, nil
}

func (c *MemoryCache) BitFieldRO(key string, commands []models.BitFieldCommand) ([]int64, error) {
	results := make([]int64, len(commands))
	for i, cmd := range commands {
		if cmd.Op != "GET" {
			return nil, fmt.Errorf("ERR BITFIELD_RO only supports GET operation")
		}
		val, err := c.bitfieldGet(key, cmd.Type, cmd.Offset)
		if err != nil {
			return nil, err
		}
		results[i] = val
	}
	return results, nil
}

func (c *MemoryCache) BitOp(operation string, destkey string, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'bitop' command")
	}

	var result []byte
	switch strings.ToUpper(operation) {
	case "AND":
		result = c.bitopAND(keys...)
	case "OR":
		result = c.bitopOR(keys...)
	case "XOR":
		result = c.bitopXOR(keys...)
	case "NOT":
		if len(keys) != 1 {
			return 0, fmt.Errorf("ERR BITOP NOT must be called with a single source key")
		}
		result = c.bitopNOT(keys[0])
	default:
		return 0, fmt.Errorf("ERR unknown operation '%s'", operation)
	}

	c.bitmaps.Store(destkey, result)
	c.incrementKeyVersion(destkey)
	return int64(len(result)), nil
}

func (c *MemoryCache) BitPos(key string, bit int, start, end int64, reverse bool) (int64, error) {
	val, exists := c.bitmaps.Load(key)
	if !exists {
		if bit == 0 {
			return 0, nil
		}
		return -1, nil
	}

	bytes := val.([]byte)
	if start < 0 {
		start = int64(len(bytes)) + start
	}
	if end < 0 {
		end = int64(len(bytes)) + end
	}

	if start < 0 {
		start = 0
	}
	if end >= int64(len(bytes)) {
		end = int64(len(bytes)) - 1
	}

	if reverse {
		for i := end; i >= start; i-- {
			pos := findBitInByte(bytes[i], bit, true)
			if pos >= 0 {
				return i*8 + int64(pos), nil
			}
		}
	} else {
		for i := start; i <= end; i++ {
			pos := findBitInByte(bytes[i], bit, false)
			if pos >= 0 {
				return i*8 + int64(pos), nil
			}
		}
	}
	return -1, nil
}

func findBitInByte(b byte, bit int, reverse bool) int {
	if reverse {
		for i := 7; i >= 0; i-- {
			if ((b >> i) & 1) == byte(bit) {
				return 7 - i
			}
		}
	} else {
		for i := 0; i < 8; i++ {
			if ((b >> (7 - i)) & 1) == byte(bit) {
				return i
			}
		}
	}
	return -1
}

func (c *MemoryCache) bitfieldGet(key string, typ string, offset int64) (int64, error) {
	val, exists := c.bitmaps.Load(key)
	if !exists {
		return 0, nil
	}
	bytes := val.([]byte)

	bits, signed, err := parseBitfieldType(typ)
	if err != nil {
		return 0, err
	}

	startByte := offset / 8
	endByte := (offset + int64(bits) + 7) / 8
	if startByte >= int64(len(bytes)) {
		return 0, nil
	}

	var result int64
	for i := startByte; i < endByte && i < int64(len(bytes)); i++ {
		bitOffset := 8 - uint((offset+(i-startByte)*8)%8)
		if bitOffset > 8 {
			bitOffset = 8
		}
		result = (result << bitOffset) | int64(bytes[i]>>(8-bitOffset))
	}

	mask := int64((1 << bits) - 1)
	result &= mask

	if signed && (result&(1<<(bits-1))) != 0 {
		result |= ^mask
	}

	return result, nil
}

func (c *MemoryCache) bitfieldSet(key string, typ string, offset int64, value int64) (int64, error) {
	bits, _, err := parseBitfieldType(typ)
	if err != nil {
		return 0, err
	}

	valI, _ := c.bitmaps.LoadOrStore(key, make([]byte, 0))
	bytes := valI.([]byte)

	startByte := offset / 8
	endByte := (offset + int64(bits) + 7) / 8

	if int64(len(bytes)) < endByte {
		newBytes := make([]byte, endByte)
		copy(newBytes, bytes)
		bytes = newBytes
	}

	mask := int64((1 << bits) - 1)
	oldValue := value
	value &= mask

	startBit := offset % 8
	for i := startByte; i < endByte; i++ {
		remainingBits := bits - int((i-startByte)*8)
		if remainingBits > 8 {
			remainingBits = 8
		}

		shift := remainingBits - int(8-startBit)
		if shift < 0 {
			shift = 0
		}

		byteMask := byte(mask >> ((i - startByte) * 8))
		bytes[i] &= ^(byteMask << startBit)
		bytes[i] |= byte(value>>(shift)) << startBit

		startBit = 0
	}

	c.bitmaps.Store(key, bytes)
	return oldValue, nil
}

func (c *MemoryCache) bitfieldIncrBy(key string, typ string, offset int64, increment int64) (int64, error) {
	current, err := c.bitfieldGet(key, typ, offset)
	if err != nil {
		return 0, err
	}

	bits, signed, err := parseBitfieldType(typ)
	if err != nil {
		return 0, err
	}

	result := current + increment
	if signed {
		max := int64(1<<(bits-1) - 1)
		min := -max - 1
		if result > max {
			result = max
		} else if result < min {
			result = min
		}
	} else {
		max := int64(1<<bits - 1)
		if result > max {
			result = max
		} else if result < 0 {
			result = 0
		}
	}

	_, err = c.bitfieldSet(key, typ, offset, result)
	if err != nil {
		return 0, err
	}

	return result, nil
}

func (c *MemoryCache) bitopAND(keys ...string) []byte {
	var maxLen int
	values := make([][]byte, 0, len(keys))

	for _, key := range keys {
		if val, exists := c.bitmaps.Load(key); exists {
			bytes := val.([]byte)
			if len(bytes) > maxLen {
				maxLen = len(bytes)
			}
			values = append(values, bytes)
		}
	}

	if len(values) == 0 {
		return nil
	}

	result := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		result[i] = 0xFF
		for _, val := range values {
			if i < len(val) {
				result[i] &= val[i]
			} else {
				result[i] = 0
				break
			}
		}
	}
	return result
}

func (c *MemoryCache) bitopOR(keys ...string) []byte {
	var maxLen int
	values := make([][]byte, 0, len(keys))

	for _, key := range keys {
		if val, exists := c.bitmaps.Load(key); exists {
			bytes := val.([]byte)
			if len(bytes) > maxLen {
				maxLen = len(bytes)
			}
			values = append(values, bytes)
		}
	}

	if len(values) == 0 {
		return nil
	}

	result := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		for _, val := range values {
			if i < len(val) {
				result[i] |= val[i]
			}
		}
	}
	return result
}

func (c *MemoryCache) bitopXOR(keys ...string) []byte {
	var maxLen int
	values := make([][]byte, 0, len(keys))

	for _, key := range keys {
		if val, exists := c.bitmaps.Load(key); exists {
			bytes := val.([]byte)
			if len(bytes) > maxLen {
				maxLen = len(bytes)
			}
			values = append(values, bytes)
		}
	}

	if len(values) == 0 {
		return nil
	}

	result := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		for j, val := range values {
			if i < len(val) {
				if j == 0 {
					result[i] = val[i]
				} else {
					result[i] ^= val[i]
				}
			}
		}
	}
	return result
}

func (c *MemoryCache) bitopNOT(key string) []byte {
	val, exists := c.bitmaps.Load(key)
	if !exists {
		return nil
	}

	bytes := val.([]byte)
	result := make([]byte, len(bytes))
	for i := 0; i < len(bytes); i++ {
		result[i] = ^bytes[i]
	}
	return result
}

func parseBitfieldType(typ string) (bits int, signed bool, err error) {
	if len(typ) < 2 {
		return 0, false, fmt.Errorf("invalid bitfield type")
	}

	switch typ[0] {
	case 'i':
		signed = true
	case 'u':
		signed = false
	default:
		return 0, false, fmt.Errorf("invalid bitfield type")
	}

	bits, err = strconv.Atoi(typ[1:])
	if err != nil || bits <= 0 || bits > 64 {
		return 0, false, fmt.Errorf("invalid bitfield size")
	}

	return bits, signed, nil
}

func (c *MemoryCache) WithRetry(strategy models.RetryStrategy) ports.Cache {
	return NewRetryDecorator(c, strategy)
}
