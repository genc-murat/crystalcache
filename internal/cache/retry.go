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

func (rd *RetryDecorator) ZUnion(keys ...string) ([]models.ZSetMember, error) {
	var result []models.ZSetMember
	var err error
	rd.executeWithRetry(func() error {
		result, err = rd.cache.ZUnion(keys...)
		return err
	})
	return result, err
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

// LPOS with retry logic
func (rd *RetryDecorator) LPos(key string, element string) (int, bool) {
	var index int
	var exists bool
	var finalExists bool

	err := rd.executeWithRetry(func() error {
		index, exists = rd.cache.LPos(key, element)
		if exists {
			finalExists = true
			return nil
		}
		return errors.New("element not found")
	})

	if err != nil {
		return 0, false
	}
	return index, finalExists
}

// LPUSHX with retry logic
func (rd *RetryDecorator) LPushX(key string, value string) (int, error) {
	var length int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		length, err = rd.cache.LPushX(key, value)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return length, finalErr
}

// RPUSHX with retry logic
func (rd *RetryDecorator) RPushX(key string, value string) (int, error) {
	var length int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		length, err = rd.cache.RPushX(key, value)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return length, finalErr
}

// LTRIM with retry logic
func (rd *RetryDecorator) LTrim(key string, start int, stop int) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.LTrim(key, start, stop)
	})
}

func (rd *RetryDecorator) XAdd(key string, id string, fields map[string]string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.XAdd(key, id, fields)
	})
}

func (rd *RetryDecorator) XACK(key, group string, ids ...string) (int64, error) {
	var count int64
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.XACK(key, group, ids...)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) XDEL(key string, ids ...string) (int64, error) {
	var count int64
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.XDEL(key, ids...)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) XAutoClaim(key, group, consumer string, minIdleTime int64, start string, count int) ([]string, []models.StreamEntry, string, error) {
	var ids []string
	var entries []models.StreamEntry
	var cursor string
	err := rd.executeWithRetry(func() error {
		var err error
		ids, entries, cursor, err = rd.cache.XAutoClaim(key, group, consumer, minIdleTime, start, count)
		return err
	})
	return ids, entries, cursor, err
}

func (rd *RetryDecorator) XClaim(key, group, consumer string, minIdleTime int64, ids ...string) ([]models.StreamEntry, error) {
	var entries []models.StreamEntry
	err := rd.executeWithRetry(func() error {
		var err error
		entries, err = rd.cache.XClaim(key, group, consumer, minIdleTime, ids...)
		return err
	})
	return entries, err
}

func (rd *RetryDecorator) XLEN(key string) int64 {
	var count int64
	rd.executeWithRetry(func() error {
		count = rd.cache.XLEN(key)
		return nil
	})
	return count
}

func (rd *RetryDecorator) XPENDING(key, group string) (int64, error) {
	var count int64
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.XPENDING(key, group)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) XRANGE(key, start, end string, count int) ([]models.StreamEntry, error) {
	var entries []models.StreamEntry
	err := rd.executeWithRetry(func() error {
		var err error
		entries, err = rd.cache.XRANGE(key, start, end, count)
		return err
	})
	return entries, err
}

func (rd *RetryDecorator) XREAD(keys []string, ids []string, count int) (map[string][]models.StreamEntry, error) {
	var result map[string][]models.StreamEntry
	err := rd.executeWithRetry(func() error {
		var err error
		result, err = rd.cache.XREAD(keys, ids, count)
		return err
	})
	return result, err
}

func (rd *RetryDecorator) XREVRANGE(key, start, end string, count int) ([]models.StreamEntry, error) {
	var entries []models.StreamEntry
	err := rd.executeWithRetry(func() error {
		var err error
		entries, err = rd.cache.XREVRANGE(key, start, end, count)
		return err
	})
	return entries, err
}

func (rd *RetryDecorator) XSETID(key string, id string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.XSETID(key, id)
	})
}

func (rd *RetryDecorator) XTRIM(key string, strategy string, threshold int64) (int64, error) {
	var count int64
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.XTRIM(key, strategy, threshold)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) XInfoGroups(key string) ([]models.StreamGroup, error) {
	var groups []models.StreamGroup
	err := rd.executeWithRetry(func() error {
		var err error
		groups, err = rd.cache.XInfoGroups(key)
		return err
	})
	return groups, err
}

func (rd *RetryDecorator) XInfoConsumers(key, group string) ([]models.StreamConsumer, error) {
	var consumers []models.StreamConsumer
	err := rd.executeWithRetry(func() error {
		var err error
		consumers, err = rd.cache.XInfoConsumers(key, group)
		return err
	})
	return consumers, err
}

func (rd *RetryDecorator) XInfoStream(key string) (*models.StreamInfo, error) {
	var info *models.StreamInfo
	err := rd.executeWithRetry(func() error {
		var err error
		info, err = rd.cache.XInfoStream(key)
		return err
	})
	return info, err
}

func (rd *RetryDecorator) XGroupCreate(key, group, id string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.XGroupCreate(key, group, id)
	})
}

func (rd *RetryDecorator) XGroupCreateConsumer(key, group, consumer string) (int64, error) {
	var count int64
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.XGroupCreateConsumer(key, group, consumer)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) XGroupDelConsumer(key, group, consumer string) (int64, error) {
	var count int64
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.XGroupDelConsumer(key, group, consumer)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) XGroupDestroy(key, group string) (int64, error) {
	var count int64
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.XGroupDestroy(key, group)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) XGroupSetID(key, group, id string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.XGroupSetID(key, group, id)
	})
}

func (rd *RetryDecorator) GetBit(key string, offset int64) (int, error) {
	var bit int
	err := rd.executeWithRetry(func() error {
		var err error
		bit, err = rd.cache.GetBit(key, offset)
		return err
	})
	return bit, err
}

func (rd *RetryDecorator) SetBit(key string, offset int64, value int) (int, error) {
	var oldBit int
	err := rd.executeWithRetry(func() error {
		var err error
		oldBit, err = rd.cache.SetBit(key, offset, value)
		return err
	})
	return oldBit, err
}

func (rd *RetryDecorator) BitCount(key string, start, end int64) (int64, error) {
	var count int64
	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.BitCount(key, start, end)
		return err
	})
	return count, err
}

func (rd *RetryDecorator) BitField(key string, commands []models.BitFieldCommand) ([]int64, error) {
	var results []int64
	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.BitField(key, commands)
		return err
	})
	return results, err
}

func (rd *RetryDecorator) BitFieldRO(key string, commands []models.BitFieldCommand) ([]int64, error) {
	var results []int64
	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.BitFieldRO(key, commands)
		return err
	})
	return results, err
}

func (rd *RetryDecorator) BitOp(operation string, destkey string, keys ...string) (int64, error) {
	var length int64
	err := rd.executeWithRetry(func() error {
		var err error
		length, err = rd.cache.BitOp(operation, destkey, keys...)
		return err
	})
	return length, err
}

func (rd *RetryDecorator) BitPos(key string, bit int, start, end int64, reverse bool) (int64, error) {
	var pos int64
	err := rd.executeWithRetry(func() error {
		var err error
		pos, err = rd.cache.BitPos(key, bit, start, end, reverse)
		return err
	})
	return pos, err
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

func (rd *RetryDecorator) GeoAdd(key string, items ...models.GeoPoint) (int, error) {
	var added int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		added, err = rd.cache.GeoAdd(key, items...)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return added, finalErr
}

func (rd *RetryDecorator) GeoDist(key, member1, member2, unit string) (float64, error) {
	var distance float64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		distance, err = rd.cache.GeoDist(key, member1, member2, unit)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return distance, finalErr
}

func (rd *RetryDecorator) GeoPos(key string, members ...string) ([]*models.GeoPoint, error) {
	var positions []*models.GeoPoint
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		positions, err = rd.cache.GeoPos(key, members...)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return positions, finalErr
}

func (rd *RetryDecorator) GeoRadius(key string, longitude, latitude, radius float64, unit string, withDist, withCoord, withHash bool, count int, sort string) ([]models.GeoPoint, error) {
	var results []models.GeoPoint
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.GeoRadius(key, longitude, latitude, radius, unit, withDist, withCoord, withHash, count, sort)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

func (rd *RetryDecorator) GeoSearch(key string, options *models.GeoSearchOptions) ([]models.GeoPoint, error) {
	var results []models.GeoPoint
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.GeoSearch(key, options)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

func (rd *RetryDecorator) GeoSearchStore(destKey string, srcKey string, options *models.GeoSearchOptions) (int, error) {
	var stored int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		stored, err = rd.cache.GeoSearchStore(destKey, srcKey, options)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return stored, finalErr
}

// Add suggestion methods to RetryDecorator
func (rd *RetryDecorator) FTSugAdd(key, str string, score float64, opts ...string) (bool, error) {
	var added bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		added, err = rd.cache.FTSugAdd(key, str, score, opts...)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return added, finalErr
}

func (rd *RetryDecorator) FTSugDel(key, str string) (bool, error) {
	var deleted bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		deleted, err = rd.cache.FTSugDel(key, str)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return deleted, finalErr
}

func (rd *RetryDecorator) FTSugGet(key, prefix string, fuzzy bool, max int) ([]models.Suggestion, error) {
	var suggestions []models.Suggestion
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		suggestions, err = rd.cache.FTSugGet(key, prefix, fuzzy, max)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return suggestions, finalErr
}

func (rd *RetryDecorator) FTSugLen(key string) (int64, error) {
	var length int64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		length, err = rd.cache.FTSugLen(key)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return length, finalErr
}

func (rd *RetryDecorator) CMSInitByDim(key string, width, depth uint) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.CMSInitByDim(key, width, depth)
	})
}

func (rd *RetryDecorator) CMSInitByProb(key string, epsilon, delta float64) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.CMSInitByProb(key, epsilon, delta)
	})
}

func (rd *RetryDecorator) CMSIncrBy(key string, items []string, increments []uint64) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.CMSIncrBy(key, items, increments)
	})
}

func (rd *RetryDecorator) CMSQuery(key string, items []string) ([]uint64, error) {
	var counts []uint64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		counts, err = rd.cache.CMSQuery(key, items)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return counts, finalErr
}

func (rd *RetryDecorator) CMSMerge(destination string, sources []string, weights []float64) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.CMSMerge(destination, sources, weights)
	})
}

func (rd *RetryDecorator) CMSInfo(key string) (map[string]interface{}, error) {
	var info map[string]interface{}
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		info, err = rd.cache.CMSInfo(key)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return info, finalErr
}

// Cuckoo Filter operations for RetryDecorator
func (rd *RetryDecorator) CFReserve(key string, capacity uint64) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.CFReserve(key, capacity)
	})
}

func (rd *RetryDecorator) CFAdd(key string, item string) (bool, error) {
	var added bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		added, err = rd.cache.CFAdd(key, item)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return added, finalErr
}

func (rd *RetryDecorator) CFAddNX(key string, item string) (bool, error) {
	var added bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		added, err = rd.cache.CFAddNX(key, item)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return added, finalErr
}

func (rd *RetryDecorator) CFInsert(key string, items []string) ([]bool, error) {
	var results []bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.CFInsert(key, items)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

func (rd *RetryDecorator) CFInsertNX(key string, items []string) ([]bool, error) {
	var results []bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.CFInsertNX(key, items)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

func (rd *RetryDecorator) CFDel(key string, item string) (bool, error) {
	var deleted bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		deleted, err = rd.cache.CFDel(key, item)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return deleted, finalErr
}

func (rd *RetryDecorator) CFCount(key string, item string) (int, error) {
	var count int
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.CFCount(key, item)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return count, finalErr
}

func (rd *RetryDecorator) CFExists(key string, item string) (bool, error) {
	var exists bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		exists, err = rd.cache.CFExists(key, item)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return exists, finalErr
}

func (rd *RetryDecorator) CFMExists(key string, items []string) ([]bool, error) {
	var results []bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.CFMExists(key, items)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

func (rd *RetryDecorator) CFInfo(key string) (*models.CuckooInfo, error) {
	var info *models.CuckooInfo
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		info, err = rd.cache.CFInfo(key)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return info, finalErr
}

func (rd *RetryDecorator) CFScanDump(key string, iter uint64) (uint64, []byte, error) {
	var nextIter uint64
	var data []byte
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		nextIter, data, err = rd.cache.CFScanDump(key, iter)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, nil, err
	}
	return nextIter, data, finalErr
}

func (rd *RetryDecorator) CFLoadChunk(key string, iter uint64, data []byte) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.CFLoadChunk(key, iter, data)
	})
}

func (rd *RetryDecorator) PFAdd(key string, elements ...string) (bool, error) {
	var modified bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		modified, err = rd.cache.PFAdd(key, elements...)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return modified, finalErr
}

func (rd *RetryDecorator) PFCount(keys ...string) (int64, error) {
	var count int64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		count, err = rd.cache.PFCount(keys...)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return count, finalErr
}

func (rd *RetryDecorator) PFMerge(destKey string, sourceKeys ...string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.PFMerge(destKey, sourceKeys...)
	})
}

func (rd *RetryDecorator) PFDebug(key string) (map[string]interface{}, error) {
	var info map[string]interface{}
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		info, err = rd.cache.PFDebug(key)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return info, finalErr
}

func (rd *RetryDecorator) PFSelfTest() error {
	return rd.executeWithRetry(func() error {
		return rd.cache.PFSelfTest()
	})
}

func (rd *RetryDecorator) TDigestCreate(key string, compression float64) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.TDigestCreate(key, compression)
	})
}

func (rd *RetryDecorator) TDigestAdd(key string, values ...float64) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.TDigestAdd(key, values...)
	})
}

func (rd *RetryDecorator) TDigestMerge(destKey string, sourceKeys []string, weights []float64) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.TDigestMerge(destKey, sourceKeys, weights)
	})
}

func (rd *RetryDecorator) TDigestReset(key string) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.TDigestReset(key)
	})
}

func (rd *RetryDecorator) TDigestQuantile(key string, quantiles ...float64) ([]float64, error) {
	var results []float64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.TDigestQuantile(key, quantiles...)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

func (rd *RetryDecorator) TDigestMin(key string) (float64, error) {
	var min float64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		min, err = rd.cache.TDigestMin(key)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return min, finalErr
}

func (rd *RetryDecorator) TDigestMax(key string) (float64, error) {
	var max float64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		max, err = rd.cache.TDigestMax(key)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return max, finalErr
}

func (rd *RetryDecorator) TDigestInfo(key string) (map[string]interface{}, error) {
	var info map[string]interface{}
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		info, err = rd.cache.TDigestInfo(key)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return info, finalErr
}

func (rd *RetryDecorator) TDigestCDF(key string, values ...float64) ([]float64, error) {
	var results []float64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.TDigestCDF(key, values...)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

func (rd *RetryDecorator) TDigestTrimmedMean(key string, lowQuantile, highQuantile float64) (float64, error) {
	var mean float64
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		mean, err = rd.cache.TDigestTrimmedMean(key, lowQuantile, highQuantile)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return mean, finalErr
}

// BFAdd with retry logic
func (rd *RetryDecorator) BFAdd(key string, item string) (bool, error) {
	var added bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		added, err = rd.cache.BFAdd(key, item)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return added, finalErr
}

// BFExists with retry logic
func (rd *RetryDecorator) BFExists(key string, item string) (bool, error) {
	var exists bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		exists, err = rd.cache.BFExists(key, item)
		finalErr = err
		return err
	})

	if err != nil {
		return false, err
	}
	return exists, finalErr
}

// BFReserve with retry logic
func (rd *RetryDecorator) BFReserve(key string, errorRate float64, capacity uint) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.BFReserve(key, errorRate, capacity)
	})
}

// BFMAdd with retry logic
func (rd *RetryDecorator) BFMAdd(key string, items []string) ([]bool, error) {
	var results []bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.BFMAdd(key, items)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

// BFMExists with retry logic
func (rd *RetryDecorator) BFMExists(key string, items []string) ([]bool, error) {
	var results []bool
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		results, err = rd.cache.BFMExists(key, items)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return results, finalErr
}

// BFInfo with retry logic
func (rd *RetryDecorator) BFInfo(key string) (map[string]interface{}, error) {
	var info map[string]interface{}
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		info, err = rd.cache.BFInfo(key)
		finalErr = err
		return err
	})

	if err != nil {
		return nil, err
	}
	return info, finalErr
}

// BFCard with retry logic
func (rd *RetryDecorator) BFCard(key string) (uint, error) {
	var card uint
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		card, err = rd.cache.BFCard(key)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, err
	}
	return card, finalErr
}

// BFScanDump with retry logic
func (rd *RetryDecorator) BFScanDump(key string, iterator int) (int, []byte, error) {
	var nextIterator int
	var data []byte
	var finalErr error

	err := rd.executeWithRetry(func() error {
		var err error
		nextIterator, data, err = rd.cache.BFScanDump(key, iterator)
		finalErr = err
		return err
	})

	if err != nil {
		return 0, nil, err
	}
	return nextIterator, data, finalErr
}

// BFLoadChunk with retry logic
func (rd *RetryDecorator) BFLoadChunk(key string, iterator int, data []byte) error {
	return rd.executeWithRetry(func() error {
		return rd.cache.BFLoadChunk(key, iterator, data)
	})
}

func (rd *RetryDecorator) WithRetry(strategy models.RetryStrategy) ports.Cache {
	return NewRetryDecorator(rd.cache, strategy)
}
