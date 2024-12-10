package interfaces

import "github.com/genc-murat/crystalcache/internal/core/models"

type ZSetBasicOps interface {
	ZAdd(key string, score float64, member string) error
	ZScore(key string, member string) (float64, bool)
	ZCard(key string) int
	ZRem(key string, member string) error
}

type ZSetRangeOps interface {
	ZRange(key string, start, stop int) []string
	ZRangeWithScores(key string, start, stop int) []models.ZSetMember
	ZRevRange(key string, start, stop int) []string
	ZRevRangeWithScores(key string, start, stop int) []models.ZSetMember
}

type ZSetScoreOps interface {
	ZRangeByScore(key string, min, max float64) []string
	ZRangeByScoreWithScores(key string, min, max float64) []models.ZSetMember
	ZRevRangeByScore(key string, max, min float64) []string
	ZCount(key string, min, max float64) int
}

type ZSetLexOps interface {
	ZLexCount(key string, min, max string) (int, error)
	ZRangeByLex(key string, min, max string) []string
	ZRevRangeByLex(key string, max, min string) []string
	ZRemRangeByLex(key string, min, max string) (int, error)
}

type ZSetRankOps interface {
	ZRank(key string, member string) (int, bool)
	ZRevRank(key string, member string) (int, bool)
}

type ZSetSetOps interface {
	ZInter(keys ...string) []string
	ZInterStore(destination string, keys []string, weights []float64) (int, error)
	ZUnion(keys ...string) []models.ZSetMember
	ZUnionStore(destination string, keys []string, weights []float64) (int, error)
	ZDiff(keys ...string) []string
	ZDiffStore(destination string, keys ...string) (int, error)
}

type ZSetModifyOps interface {
	ZIncrBy(key string, increment float64, member string) (float64, error)
	ZRemRangeByRank(key string, start, stop int) (int, error)
	ZRemRangeByScore(key string, min, max float64) (int, error)
	ZRangeStore(destination string, source string, start, stop int, withScores bool) (int, error)
}

type ZSetScanOps interface {
	ZScan(key string, cursor int, match string, count int) ([]models.ZSetMember, int)
}

type ZSetManager interface {
	ZSetBasicOps
	ZSetRangeOps
	ZSetScoreOps
	ZSetLexOps
	ZSetRankOps
	ZSetSetOps
	ZSetModifyOps
	ZSetScanOps
}
