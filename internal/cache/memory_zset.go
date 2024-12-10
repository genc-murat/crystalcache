package cache

import "github.com/genc-murat/crystalcache/internal/core/models"

func (c *MemoryCache) ZInterCard(keys ...string) (int, error) {
	return c.zsetManager.ZInterCard(keys...)
}

func (c *MemoryCache) ZDiffStore(destination string, keys ...string) (int, error) {
	return c.zsetManager.ZDiffStore(destination, keys...)
}

// Basic operations
func (c *MemoryCache) ZAdd(key string, score float64, member string) error {
	return c.zsetManager.ZAdd(key, score, member)
}

func (c *MemoryCache) ZScore(key string, member string) (float64, bool) {
	return c.zsetManager.ZScore(key, member)
}

func (c *MemoryCache) ZCard(key string) int {
	return c.zsetManager.ZCard(key)
}

func (c *MemoryCache) ZRem(key string, member string) error {
	return c.zsetManager.ZRem(key, member)
}

// Range operations
func (c *MemoryCache) ZRange(key string, start, stop int) []string {
	return c.zsetManager.ZRange(key, start, stop)
}

func (c *MemoryCache) ZRangeWithScores(key string, start, stop int) []models.ZSetMember {
	return c.zsetManager.ZRangeWithScores(key, start, stop)
}

func (c *MemoryCache) ZRevRange(key string, start, stop int) []string {
	return c.zsetManager.ZRevRange(key, start, stop)
}

func (c *MemoryCache) ZRevRangeWithScores(key string, start, stop int) []models.ZSetMember {
	return c.zsetManager.ZRevRangeWithScores(key, start, stop)
}

func (c *MemoryCache) ZRangeByScore(key string, min, max float64) []string {
	return c.zsetManager.ZRangeByScore(key, min, max)
}

func (c *MemoryCache) ZRangeByScoreWithScores(key string, min, max float64) []models.ZSetMember {
	return c.zsetManager.ZRangeByScoreWithScores(key, min, max)
}

func (c *MemoryCache) ZRevRangeByScore(key string, max, min float64) []string {
	return c.zsetManager.ZRevRangeByScore(key, max, min)
}

func (c *MemoryCache) ZRangeStore(destination string, source string, start, stop int, withScores bool) (int, error) {
	return c.zsetManager.ZRangeStore(destination, source, start, stop, withScores)
}

// Score operations
func (c *MemoryCache) ZIncrBy(key string, increment float64, member string) (float64, error) {
	return c.zsetManager.ZIncrBy(key, increment, member)
}

func (c *MemoryCache) ZCount(key string, min, max float64) int {
	return c.zsetManager.ZCount(key, min, max)
}

// Lex operations
func (c *MemoryCache) ZRangeByLex(key string, min, max string) []string {
	return c.zsetManager.ZRangeByLex(key, min, max)
}

func (c *MemoryCache) ZRemRangeByLex(key string, min, max string) (int, error) {
	return c.zsetManager.ZRemRangeByLex(key, min, max)
}

func (c *MemoryCache) ZLexCount(key string, min, max string) (int, error) {
	return c.zsetManager.ZLexCount(key, min, max)
}

func (c *MemoryCache) ZRevRangeByLex(key string, max, min string) []string {
	return c.zsetManager.ZRevRangeByLex(key, max, min)
}

// Rank operations
func (c *MemoryCache) ZRank(key string, member string) (int, bool) {
	return c.zsetManager.ZRank(key, member)
}

func (c *MemoryCache) ZRevRank(key string, member string) (int, bool) {
	return c.zsetManager.ZRevRank(key, member)
}

// Set operations
func (c *MemoryCache) ZUnion(keys ...string) []models.ZSetMember {
	return c.zsetManager.ZUnion(keys...)
}

func (c *MemoryCache) ZUnionStore(destination string, keys []string, weights []float64) (int, error) {
	return c.zsetManager.ZUnionStore(destination, keys, weights)
}

func (c *MemoryCache) ZInter(keys ...string) []string {
	return c.zsetManager.ZInter(keys...)
}

func (c *MemoryCache) ZInterStore(destination string, keys []string, weights []float64) (int, error) {
	return c.zsetManager.ZInterStore(destination, keys, weights)
}

func (c *MemoryCache) ZDiff(keys ...string) []string {
	return c.zsetManager.ZDiff(keys...)
}

// Scan operations
func (c *MemoryCache) ZScan(key string, cursor int, match string, count int) ([]models.ZSetMember, int) {
	return c.zsetManager.ZScan(key, cursor, match, count)
}

// Pop operations
func (c *MemoryCache) ZPopMax(key string) (models.ZSetMember, bool) {
	return c.zsetManager.ZPopMax(key)
}

func (c *MemoryCache) ZPopMaxN(key string, count int) []models.ZSetMember {
	return c.zsetManager.ZPopMaxN(key, count)
}

func (c *MemoryCache) ZPopMin(key string) (models.ZSetMember, bool) {
	return c.zsetManager.ZPopMin(key)
}

func (c *MemoryCache) ZPopMinN(key string, count int) []models.ZSetMember {
	return c.zsetManager.ZPopMinN(key, count)
}

// Random member operations
func (c *MemoryCache) ZRandMember(key string, count int, withScores bool) []models.ZSetMember {
	return c.zsetManager.ZRandMember(key, count, withScores)
}

func (c *MemoryCache) ZRandMemberWithoutScores(key string, count int) []string {
	return c.zsetManager.ZRandMemberWithoutScores(key, count)
}

func (c *MemoryCache) ZRemRangeByScore(key string, min, max float64) (int, error) {
	return c.zsetManager.ZRemRangeByScore(key, min, max)
}

func (c *MemoryCache) ZRemRangeByRank(key string, start, stop int) (int, error) {
	return c.zsetManager.ZRemRangeByRank(key, start, stop)
}
