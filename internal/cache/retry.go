package cache

import (
	"context"
	"errors"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type RetryDecorator struct {
	cache    *MemoryCache
	strategy models.RetryStrategy
}

func (rd *RetryDecorator) executeWithRetry(operation func() error) error {
	ctx, cancel := context.WithTimeout(context.Background(), rd.strategy.Timeout)
	defer cancel()

	attempts := 0
	interval := rd.strategy.InitialInterval

	for {
		select {
		case <-ctx.Done():
			return models.ErrOperationTimeout
		default:
			attempts++

			err := operation()
			if err == nil {
				return nil
			}

			if attempts >= rd.strategy.MaxAttempts {
				return models.ErrMaxRetriesExceeded
			}

			if interval < rd.strategy.MaxInterval {
				interval = time.Duration(float64(interval) * rd.strategy.Multiplier)
				if interval > rd.strategy.MaxInterval {
					interval = rd.strategy.MaxInterval
				}
			}

			select {
			case <-ctx.Done():
				return models.ErrOperationTimeout
			case <-time.After(interval):
				// Bir sonraki deneme için bekle
			}
		}
	}
}

func NewRetryDecorator(cache *MemoryCache, strategy models.RetryStrategy) *RetryDecorator {
	return &RetryDecorator{
		cache:    cache,
		strategy: strategy,
	}
}

func (rd *RetryDecorator) IncrCommandCount() {
	rd.cache.IncrCommandCount()
}

func (rd *RetryDecorator) Pipeline() *models.Pipeline {
	return rd.cache.Pipeline()
}

func (rd *RetryDecorator) ExecPipeline(pipeline *models.Pipeline) []models.Value {
	var results []models.Value
	err := rd.executeWithRetry(func() error {
		results = rd.cache.ExecPipeline(pipeline)
		return nil
	})
	if err != nil {
		return []models.Value{{Type: "error", Str: err.Error()}}
	}
	return results
}

func (rd *RetryDecorator) Multi() error {
	return rd.executeWithRetry(func() error {
		return rd.cache.Multi()
	})
}

func (rd *RetryDecorator) Exec() ([]models.Value, error) {
	var results []models.Value
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.Exec()
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

func (rd *RetryDecorator) Discard() error {
	return rd.executeWithRetry(func() error {
		return rd.cache.Discard()
	})
}

func (rd *RetryDecorator) Watch(keys ...string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.Watch(keys...)
	})
}

func (rd *RetryDecorator) Unwatch() error {
	return rd.executeWithRetry(func() error {
		return rd.cache.Unwatch()
	})
}

func (rd *RetryDecorator) IsInTransaction() bool {
	return rd.cache.IsInTransaction()
}

func (rd *RetryDecorator) AddToTransaction(cmd models.Command) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.AddToTransaction(cmd)
	})
}

func (rd *RetryDecorator) Keys(pattern string) []string {
	var results []string
	rd.executeWithRetry(func() error {
		results = rd.cache.Keys(pattern)
		return nil
	})
	return results
}

func (rd *RetryDecorator) TTL(key string) int {
	var ttl int
	rd.executeWithRetry(func() error {
		ttl = rd.cache.TTL(key)
		return nil
	})
	return ttl
}

func (rd *RetryDecorator) Type(key string) string {
	var typ string
	rd.executeWithRetry(func() error {
		typ = rd.cache.Type(key)
		return nil
	})
	return typ
}

func (rd *RetryDecorator) Exists(key string) bool {
	var exists bool
	rd.executeWithRetry(func() error {
		exists = rd.cache.Exists(key)
		return nil
	})
	return exists
}

func (rd *RetryDecorator) FlushAll() {
	rd.executeWithRetry(func() error {
		rd.cache.FlushAll()
		return nil
	})
}

func (rd *RetryDecorator) DBSize() int {
	var size int
	rd.executeWithRetry(func() error {
		size = rd.cache.DBSize()
		return nil
	})
	return size
}

func (rd *RetryDecorator) Info() map[string]string {
	var info map[string]string
	rd.executeWithRetry(func() error {
		info = rd.cache.Info()
		return nil
	})
	return info
}

func (rd *RetryDecorator) LLen(key string) int {
	var length int
	rd.executeWithRetry(func() error {
		length = rd.cache.LLen(key)
		return nil
	})
	return length
}

func (rd *RetryDecorator) LPop(key string) (string, bool) {
	var value string
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		value, exists = rd.cache.LPop(key)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("key not found")
	})

	if err != nil {
		return "", false
	}
	return value, finalExists
}

func (rd *RetryDecorator) RPop(key string) (string, bool) {
	var value string
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		value, exists = rd.cache.RPop(key)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("key not found")
	})

	if err != nil {
		return "", false
	}
	return value, finalExists
}

func (rd *RetryDecorator) Del(key string) (bool, error) {
	var deleted bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		deleted, err = rd.cache.Del(key)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return deleted, finalErr
}

func (rd *RetryDecorator) SCard(key string) int {
	var count int
	rd.executeWithRetry(func() error {
		count = rd.cache.SCard(key)
		return nil
	})
	return count
}

func (rd *RetryDecorator) SRem(key string, member string) (bool, error) {
	var removed bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		removed, err = rd.cache.SRem(key, member)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return removed, finalErr
}

func (rd *RetryDecorator) SIsMember(key string, member string) bool {
	var isMember bool
	rd.executeWithRetry(func() error {
		isMember = rd.cache.SIsMember(key, member)
		return nil
	})
	return isMember
}

func (rd *RetryDecorator) Expire(key string, seconds int) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.Expire(key, seconds)
	})
}

func (rd *RetryDecorator) GetKeyVersion(key string) int64 {
	var version int64
	rd.executeWithRetry(func() error {
		version = rd.cache.GetKeyVersion(key)
		return nil
	})
	return version
}

func (rd *RetryDecorator) LRange(key string, start, stop int) ([]string, error) {
	var result []string
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		result, err = rd.cache.LRange(key, start, stop)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return result, finalErr
}

func (rd *RetryDecorator) LSet(key string, index int, value string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.LSet(key, index, value)
	})
}

func (rd *RetryDecorator) LRem(key string, count int, value string) (int, error) {
	var removed int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		removed, err = rd.cache.LRem(key, count, value)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return removed, finalErr
}

func (rd *RetryDecorator) SAdd(key string, member string) (bool, error) {
	var added bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		added, err = rd.cache.SAdd(key, member)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return added, finalErr
}

func (rd *RetryDecorator) SMembers(key string) ([]string, error) {
	var members []string
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		members, err = rd.cache.SMembers(key)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return members, finalErr
}

func (rd *RetryDecorator) SDiff(keys ...string) []string {
	var diff []string
	rd.executeWithRetry(func() error {
		diff = rd.cache.SDiff(keys...)
		return nil
	})
	return diff
}

func (rd *RetryDecorator) SInter(keys ...string) []string {
	var inter []string
	rd.executeWithRetry(func() error {
		inter = rd.cache.SInter(keys...)
		return nil
	})
	return inter
}

func (rd *RetryDecorator) SUnion(keys ...string) []string {
	var union []string
	rd.executeWithRetry(func() error {
		union = rd.cache.SUnion(keys...)
		return nil
	})
	return union
}

func (rd *RetryDecorator) Rename(oldKey, newKey string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.Rename(oldKey, newKey)
	})
}

func (rd *RetryDecorator) Get(key string) (string, bool) {
	var value string
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		value, exists = rd.cache.Get(key)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("key not found")
	})

	if err != nil {
		return "", false
	}
	return value, finalExists
}

func (rd *RetryDecorator) Set(key string, value string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.Set(key, value)
	})
}

func (rd *RetryDecorator) HSet(hash string, key string, value string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.HSet(hash, key, value)
	})
}

func (rd *RetryDecorator) HGet(hash string, key string) (string, bool) {
	var value string
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		value, exists = rd.cache.HGet(hash, key)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("hash key not found")
	})

	if err != nil {
		return "", false
	}
	return value, finalExists
}

func (rd *RetryDecorator) HGetAll(hash string) map[string]string {
	var result map[string]string
	rd.executeWithRetry(func() error {
		result = rd.cache.HGetAll(hash)
		return nil
	})
	return result
}

func (rd *RetryDecorator) Incr(key string) (int, error) {
	var value int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		value, err = rd.cache.Incr(key)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return value, finalErr
}

func (rd *RetryDecorator) LPush(key string, value string) (int, error) {
	var length int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		length, err = rd.cache.LPush(key, value)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return length, finalErr
}

func (rd *RetryDecorator) RPush(key string, value string) (int, error) {
	var length int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		length, err = rd.cache.RPush(key, value)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return length, finalErr
}

func (rd *RetryDecorator) ZAdd(key string, score float64, member string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.ZAdd(key, score, member)
	})
}

func (rd *RetryDecorator) ZCard(key string) int {
	var count int
	rd.executeWithRetry(func() error {
		count = rd.cache.ZCard(key)
		return nil
	})
	return count
}

func (rd *RetryDecorator) ZCount(key string, min, max float64) int {
	var count int
	rd.executeWithRetry(func() error {
		count = rd.cache.ZCount(key, min, max)
		return nil
	})
	return count
}

func (rd *RetryDecorator) ZRange(key string, start, stop int) []string {
	var result []string
	rd.executeWithRetry(func() error {
		result = rd.cache.ZRange(key, start, stop)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZRangeWithScores(key string, start, stop int) []models.ZSetMember {
	var result []models.ZSetMember
	rd.executeWithRetry(func() error {
		result = rd.cache.ZRangeWithScores(key, start, stop)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZRangeByScore(key string, min, max float64) []string {
	var result []string
	rd.executeWithRetry(func() error {
		result = rd.cache.ZRangeByScore(key, min, max)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZRank(key string, member string) (int, bool) {
	var rank int
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		rank, exists = rd.cache.ZRank(key, member)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("member not found")
	})

	if err != nil {
		return 0, false
	}
	return rank, finalExists
}

func (rd *RetryDecorator) ZRem(key string, member string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.ZRem(key, member)
	})
}

func (rd *RetryDecorator) ZScore(key string, member string) (float64, bool) {
	var score float64
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		score, exists = rd.cache.ZScore(key, member)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("member not found")
	})

	if err != nil {
		return 0, false
	}
	return score, finalExists
}

func (rd *RetryDecorator) ZRevRange(key string, start, stop int) []string {
	var result []string
	rd.executeWithRetry(func() error {
		result = rd.cache.ZRevRange(key, start, stop)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZRevRangeWithScores(key string, start, stop int) []models.ZSetMember {
	var result []models.ZSetMember
	rd.executeWithRetry(func() error {
		result = rd.cache.ZRevRangeWithScores(key, start, stop)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZIncrBy(key string, increment float64, member string) (float64, error) {
	var score float64
	err := rd.executeWithRetry(func() error {
		var err error
		score, err = rd.cache.ZIncrBy(key, increment, member)
		return err
	})
	return score, err
}

func (rd *RetryDecorator) ZRangeByScoreWithScores(key string, min, max float64) []models.ZSetMember {
	var result []models.ZSetMember
	rd.executeWithRetry(func() error {
		result = rd.cache.ZRangeByScoreWithScores(key, min, max)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZInterStore(destination string, keys []string, weights []float64) (int, error) {
	var count int
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.ZInterStore(destination, keys, weights)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) ZUnionStore(destination string, keys []string, weights []float64) (int, error) {
	var count int
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.ZUnionStore(destination, keys, weights)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) PFAdd(key string, elements ...string) (bool, error) {
	var modified bool
	err := rd.executeWithRetry(func() error {
		var err error
		modified, err = rd.cache.PFAdd(key, elements...)
		return err
	})
	return modified, err
}

func (rd *RetryDecorator) PFCount(keys ...string) (int64, error) {
	var count int64
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.PFCount(keys...)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) PFMerge(destKey string, sourceKeys ...string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.PFMerge(destKey, sourceKeys...)
	})
}

func (rd *RetryDecorator) GetMemoryStats() models.MemoryStats {
	var stats models.MemoryStats
	rd.executeWithRetry(func() error {
		stats = rd.cache.GetMemoryStats()
		return nil
	})
	return stats
}

func (rd *RetryDecorator) StartDefragmentation(interval time.Duration, threshold float64) {
	rd.executeWithRetry(func() error {
		rd.cache.StartDefragmentation(interval, threshold)
		return nil
	})
}

func (rd *RetryDecorator) Defragment() {
	rd.executeWithRetry(func() error {
		rd.cache.Defragment()
		return nil
	})
}

func (rd *RetryDecorator) Scan(cursor int, pattern string, count int) ([]string, int) {
	var keys []string
	var nextCursor int

	rd.executeWithRetry(func() error {
		keys, nextCursor = rd.cache.Scan(cursor, pattern, count)
		return nil
	})

	return keys, nextCursor
}

func (rd *RetryDecorator) HDel(hash string, field string) (bool, error) {
	var deleted bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		deleted, err = rd.cache.HDel(hash, field)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return deleted, finalErr
}

func (rd *RetryDecorator) HScan(hash string, cursor int, pattern string, count int) ([]string, int) {
	var results []string
	var nextCursor int

	rd.executeWithRetry(func() error {
		results, nextCursor = rd.cache.HScan(hash, cursor, pattern, count)
		return nil
	})

	return results, nextCursor
}

// Add GetJSON with retry logic
func (rd *RetryDecorator) GetJSON(key string) (interface{}, bool) {
	var value interface{}
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		value, exists = rd.cache.GetJSON(key)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("json key not found")
	})

	if err != nil {
		return nil, false
	}
	return value, finalExists
}

// Add SetJSON with retry logic
func (rd *RetryDecorator) SetJSON(key string, value interface{}) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.SetJSON(key, value)
	})
}

// Add DeleteJSON with retry logic
func (rd *RetryDecorator) DeleteJSON(key string) bool {
	var deleted bool
	rd.executeWithRetry(func() error {
		deleted = rd.cache.DeleteJSON(key)
		if deleted {
			return nil
		}
		return errors.New("json key not found")
	})
	return deleted
}

func (rd *RetryDecorator) ZDiff(keys ...string) []string {
	var result []string
	rd.executeWithRetry(func() error {
		result = rd.cache.ZDiff(keys...)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZDiffStore(destination string, keys ...string) (int, error) {
	var count int
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.ZDiffStore(destination, keys...) // Fixed: spread the keys slice
		return err
	})
	return count, err
}

func (rd *RetryDecorator) ZInter(keys ...string) []string {
	var result []string
	rd.executeWithRetry(func() error {
		result = rd.cache.ZInter(keys...)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZInterCard(keys ...string) (int, error) {
	var count int
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.ZInterCard(keys...)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) ZLexCount(key, min, max string) (int, error) {
	var count int
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.ZLexCount(key, min, max)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) ZRangeByLex(key string, min, max string) []string {
	var result []string
	rd.executeWithRetry(func() error {
		result = rd.cache.ZRangeByLex(key, min, max)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZRangeStore(destination string, source string, start, stop int, withScores bool) (int, error) {
	var count int
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.ZRangeStore(destination, source, start, stop, withScores)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) ZRemRangeByLex(key string, min, max string) (int, error) {
	var count int
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.ZRemRangeByLex(key, min, max)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) ZRemRangeByRank(key string, start, stop int) (int, error) {
	var count int
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.ZRemRangeByRank(key, start, stop)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) ZRemRangeByScore(key string, min, max float64) (int, error) {
	var count int
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.ZRemRangeByScore(key, min, max)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) ZRevRangeByLex(key string, max, min string) []string {
	var result []string
	rd.executeWithRetry(func() error {
		result = rd.cache.ZRevRangeByLex(key, max, min)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZRevRangeByScore(key string, max, min float64) []string {
	var result []string
	rd.executeWithRetry(func() error {
		result = rd.cache.ZRevRangeByScore(key, max, min)
		return nil
	})
	return result
}

func (rd *RetryDecorator) ZRevRank(key string, member string) (int, bool) {
	var rank int
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		rank, exists = rd.cache.ZRevRank(key, member)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("member not found")
	})

	if err != nil {
		return 0, false
	}
	return rank, finalExists
}

func (rd *RetryDecorator) ZScan(key string, cursor int, match string, count int) ([]models.ZSetMember, int) {
	var members []models.ZSetMember
	var nextCursor int
	rd.executeWithRetry(func() error {
		members, nextCursor = rd.cache.ZScan(key, cursor, match, count)
		return nil
	})
	return members, nextCursor
}

func (rd *RetryDecorator) ZUnion(keys ...string) []models.ZSetMember {
	var result []models.ZSetMember
	rd.executeWithRetry(func() error {
		result = rd.cache.ZUnion(keys...)
		return nil
	})
	return result
}

// ExpireAt sets an absolute Unix timestamp when the key should expire
func (rd *RetryDecorator) ExpireAt(key string, timestamp int64) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.ExpireAt(key, timestamp)
	})
}

// ExpireTime returns the absolute Unix timestamp when the key will expire
func (rd *RetryDecorator) ExpireTime(key string) (int64, error) {
	var expireTime int64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		expireTime, err = rd.cache.ExpireTime(key)
		finalErr = err
		return err
	})

	if err != nil {
		return -2, err
	}
	return expireTime, finalErr
}

// HIncrBy increments the integer value of a hash field by the given increment
func (rd *RetryDecorator) HIncrBy(key, field string, increment int64) (int64, error) {
	var result int64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		result, err = rd.cache.HIncrBy(key, field, increment)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return result, finalErr
}

// HIncrByFloat increments the float value of a hash field by the given increment
func (rd *RetryDecorator) HIncrByFloat(key, field string, increment float64) (float64, error) {
	var result float64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		result, err = rd.cache.HIncrByFloat(key, field, increment)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return result, finalErr
}

func (rd *RetryDecorator) WithRetry(strategy models.RetryStrategy) ports.Cache {
	return NewRetryDecorator(rd.cache, strategy)
}
