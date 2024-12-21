package ports

import (
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

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
	PFAdd(key string, elements ...string) (bool, error)
	PFCount(keys ...string) (int64, error)
	PFMerge(destKey string, sourceKeys ...string) error
	GetMemoryStats() models.MemoryStats
	StartDefragmentation(interval time.Duration, threshold float64)
	Defragment()
	Scan(cursor int, pattern string, count int) ([]string, int)
	HDel(hash string, key string) (bool, error)
	HScan(hash string, cursor int, pattern string, count int) ([]string, int)
	SetJSON(key string, value interface{}) error
	GetJSON(key string) (interface{}, bool)
	DeleteJSON(key string) bool
	ZDiff(keys ...string) []string
	ZDiffStore(destination string, keys ...string) (int, error)
	ZInter(keys ...string) []string
	ZInterCard(keys ...string) (int, error)
	ZLexCount(key, min, max string) (int, error)
	ZRangeByLex(key string, min, max string) []string
	ZRangeStore(destination string, source string, start, stop int, withScores bool) (int, error)
	ZRemRangeByLex(key string, min, max string) (int, error)
	ZRemRangeByRank(key string, start, stop int) (int, error)
	ZRemRangeByScore(key string, min, max float64) (int, error)
	ZRevRangeByLex(key string, max, min string) []string
	ZRevRangeByScore(key string, max, min float64) []string
	ZRevRank(key string, member string) (int, bool)
	ZScan(key string, cursor int, match string, count int) ([]models.ZSetMember, int)
	ZUnion(keys ...string) ([]models.ZSetMember, error)
	ExpireAt(key string, timestamp int64) error
	ExpireTime(key string) (int64, error)
	HIncrBy(key, field string, increment int64) (int64, error)
	HIncrByFloat(key, field string, increment float64) (float64, error)
	LIndex(key string, index int) (string, bool)
	LInsert(key string, before bool, pivot string, value string) (int, error)
	LTrim(key string, start, stop int) error
	LPos(key string, element string) (int, bool)
	LPushX(key string, value string) (int, error)
	RPushX(key string, value string) (int, error)
	XAdd(key string, id string, fields map[string]string) error
	XACK(key, group string, ids ...string) (int64, error)
	XDEL(key string, ids ...string) (int64, error)
	XAutoClaim(key, group, consumer string, minIdleTime int64, start string, count int) ([]string, []models.StreamEntry, string, error)
	XClaim(key, group, consumer string, minIdleTime int64, ids ...string) ([]models.StreamEntry, error)
	XLEN(key string) int64
	XPENDING(key, group string) (int64, error)
	XRANGE(key, start, end string, count int) ([]models.StreamEntry, error)
	XREAD(keys []string, ids []string, count int) (map[string][]models.StreamEntry, error)
	XREVRANGE(key, start, end string, count int) ([]models.StreamEntry, error)
	XSETID(key string, id string) error
	XTRIM(key string, strategy string, threshold int64) (int64, error)
	XInfoGroups(key string) ([]models.StreamGroup, error)
	XInfoConsumers(key, group string) ([]models.StreamConsumer, error)
	XInfoStream(key string) (*models.StreamInfo, error)
	XGroupCreate(key, group, id string) error
	XGroupCreateConsumer(key, group, consumer string) (int64, error)
	XGroupDelConsumer(key, group, consumer string) (int64, error)
	XGroupDestroy(key, group string) (int64, error)
	XGroupSetID(key, group, id string) error
	GetBit(key string, offset int64) (int, error)
	SetBit(key string, offset int64, value int) (int, error)
	BitCount(key string, start, end int64) (int64, error)
	BitField(key string, commands []models.BitFieldCommand) ([]int64, error)
	BitFieldRO(key string, commands []models.BitFieldCommand) ([]int64, error)
	BitOp(operation string, destkey string, keys ...string) (int64, error)
	BitPos(key string, bit int, start, end int64, reverse bool) (int64, error)
	GeoAdd(key string, items ...models.GeoPoint) (int, error)
	GeoDist(key, member1, member2, unit string) (float64, error)
	GeoPos(key string, members ...string) ([]*models.GeoPoint, error)
	GeoRadius(key string, longitude, latitude, radius float64, unit string, withDist, withCoord, withHash bool, count int, sort string) ([]models.GeoPoint, error)
	GeoSearch(key string, options *models.GeoSearchOptions) ([]models.GeoPoint, error)
	GeoSearchStore(destKey, srcKey string, options *models.GeoSearchOptions) (int, error)
	// FTSugAdd adds a suggestion string to an auto-complete suggestion dictionary
	FTSugAdd(key, str string, score float64, opts ...string) (bool, error)

	// FTSugDel deletes a suggestion string from a suggestion dictionary
	FTSugDel(key, str string) (bool, error)

	// FTSugGet gets completion suggestions for a prefix
	FTSugGet(key, prefix string, fuzzy bool, max int) ([]models.Suggestion, error)

	// FTSugLen gets the size of an auto-complete suggestion dictionary
	FTSugLen(key string) (int64, error)
}
