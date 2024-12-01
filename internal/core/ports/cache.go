package ports

import "github.com/genc-murat/crystalcache/internal/core/models"

type Cache interface {
	Set(key string, value string) error
	Get(key string) (string, bool)
	HSet(hash string, key string, value string) error
	HGet(hash string, key string) (string, bool)
	HGetAll(hash string) map[string]string
	Incr(key string) (int, error)
	Expire(key string, seconds int) error
	Del(key string) (bool, error)
	Keys(pattern string) []string
	TTL(key string) int // TTL in seconds, -2 if not exists, -1 if no expire
	LPush(key string, value string) (int, error)
	RPush(key string, value string) (int, error)
	LRange(key string, start, stop int) ([]string, error)
	SAdd(key string, member string) (bool, error)
	SMembers(key string) ([]string, error)
	LLen(key string) int
	LPop(key string) (string, bool)
	RPop(key string) (string, bool)
	SCard(key string) int
	SRem(key string, member string) (bool, error)
	SIsMember(key string, member string) bool
	LSet(key string, index int, value string) error
	SInter(keys ...string) []string
	SUnion(keys ...string) []string
	Type(key string) string
	Exists(key string) bool
	FlushAll()
	DBSize() int
	SDiff(keys ...string) []string
	LRem(key string, count int, value string) (int, error)
	Rename(oldKey, newKey string) error
	Info() map[string]string
	Multi() error
	Exec() ([]models.Value, error)
	Discard() error
	AddToTransaction(cmd models.Command) error
	IsInTransaction() bool
	Watch(keys ...string) error
	Unwatch() error
	GetKeyVersion(key string) int64
	Pipeline() *models.Pipeline
	ExecPipeline(pipeline *models.Pipeline) []models.Value
	IncrCommandCount()
	WithRetry(strategy models.RetryStrategy) Cache
	ZAdd(key string, score float64, member string) error
	ZCard(key string) int
	ZCount(key string, min, max float64) int
	ZRange(key string, start, stop int) []string
	ZRangeWithScores(key string, start, stop int) []models.ZSetMember
	ZRangeByScore(key string, min, max float64) []string
	ZRank(key string, member string) (int, bool)
	ZRem(key string, member string) error
	ZScore(key string, member string) (float64, bool)
	ZRevRange(key string, start, stop int) []string
	ZRevRangeWithScores(key string, start, stop int) []models.ZSetMember
	ZIncrBy(key string, increment float64, member string) (float64, error)
	ZRangeByScoreWithScores(key string, min, max float64) []models.ZSetMember
	ZInterStore(destination string, keys []string, weights []float64) (int, error)
	ZUnionStore(destination string, keys []string, weights []float64) (int, error)
}
