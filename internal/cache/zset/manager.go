package zset

import (
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// Manager coordinates all ZSet operations
type Manager struct {
	basicOps  *BasicOps
	rangeOps  *RangeOps
	scoreOps  *ScoreOps
	lexOps    *LexOps
	rankOps   *RankOps
	setOps    *SetOps
	modifyOps *ModifyOps
	scanOps   *ScanOps
}

// NewManager creates a new ZSet manager with all operations
func NewManager(cache *sync.Map, version *sync.Map) *Manager {
	// Initialize BasicOps first as other ops depend on it
	basicOps := NewBasicOps(cache, version)

	return &Manager{
		basicOps:  basicOps,
		rangeOps:  NewRangeOps(basicOps),
		scoreOps:  NewScoreOps(basicOps),
		lexOps:    NewLexOps(basicOps),
		rankOps:   NewRankOps(basicOps),
		setOps:    NewSetOps(basicOps),
		modifyOps: NewModifyOps(basicOps),
		scanOps:   NewScanOps(basicOps),
	}
}

// Basic Operations
func (m *Manager) ZAdd(key string, score float64, member string) error {
	return m.basicOps.ZAdd(key, score, member)
}

func (m *Manager) ZScore(key string, member string) (float64, bool) {
	return m.basicOps.ZScore(key, member)
}

func (m *Manager) ZCard(key string) int {
	return m.basicOps.ZCard(key)
}

func (m *Manager) ZRem(key string, member string) error {
	return m.basicOps.ZRem(key, member)
}

// Range Operations
func (m *Manager) ZRange(key string, start, stop int) []string {
	return m.rangeOps.ZRange(key, start, stop)
}

func (m *Manager) ZRangeWithScores(key string, start, stop int) []models.ZSetMember {
	return m.rangeOps.ZRangeWithScores(key, start, stop)
}

func (m *Manager) ZRevRange(key string, start, stop int) []string {
	return m.rangeOps.ZRevRange(key, start, stop)
}

// Score Operations
func (m *Manager) ZIncrBy(key string, increment float64, member string) (float64, error) {
	return m.scoreOps.ZIncrBy(key, increment, member)
}

func (m *Manager) ZCount(key string, min, max float64) int {
	return m.scoreOps.ZCount(key, min, max)
}

// Lexicographical Operations
func (m *Manager) ZRangeByLex(key string, min, max string) []string {
	return m.lexOps.ZRangeByLex(key, min, max)
}

func (m *Manager) ZRemRangeByLex(key string, min, max string) (int, error) {
	return m.lexOps.ZRemRangeByLex(key, min, max)
}

// Rank Operations
func (m *Manager) ZRank(key string, member string) (int, bool) {
	return m.rankOps.ZRank(key, member)
}

func (m *Manager) ZRevRank(key string, member string) (int, bool) {
	return m.rankOps.ZRevRank(key, member)
}

// Set Operations
func (m *Manager) ZUnion(keys ...string) ([]models.ZSetMember, error) {
	return m.setOps.ZUnion(keys...)
}

func (m *Manager) ZInter(keys ...string) []string {
	return m.setOps.ZInter(keys...)
}

// Modify Operations
func (m *Manager) ZRemRangeByRank(key string, start, stop int) (int, error) {
	return m.modifyOps.ZRemRangeByRank(key, start, stop)
}

func (m *Manager) ZRemRangeByScore(key string, min, max float64) (int, error) {
	return m.modifyOps.ZRemRangeByScore(key, min, max)
}

// ZRemRangeByRankCount removes a specified number of elements from the sorted set at given ranks
func (m *Manager) ZRemRangeByRankCount(key string, start, stop, count int) (int, error) {
	return m.modifyOps.ZRemRangeByRankCount(key, start, stop, count)
}

// Scan Operations
func (m *Manager) ZScan(key string, cursor int, match string, count int) ([]models.ZSetMember, int) {
	return m.scanOps.ZScan(key, cursor, match, count)
}

func (m *Manager) ZInterCard(keys ...string) (int, error) {
	return m.setOps.ZInterCard(keys...)
}

func (m *Manager) ZDiffStore(destination string, keys ...string) (int, error) {
	return m.setOps.ZDiffStore(destination, keys...)
}

func (m *Manager) ZRangeStore(destination string, source string, start, stop int, withScores bool) (int, error) {
	return m.rangeOps.ZRangeStore(destination, source, start, stop, withScores)
}

func (m *Manager) ZRangeByScore(key string, min, max float64) []string {
	return m.rangeOps.ZRangeByScore(key, min, max)
}

func (m *Manager) ZRangeByScoreWithScores(key string, min, max float64) []models.ZSetMember {
	return m.rangeOps.ZRangeByScoreWithScores(key, min, max)
}

func (m *Manager) ZRevRangeByScore(key string, max, min float64) []string {
	return m.rangeOps.ZRevRangeByScore(key, max, min)
}

func (m *Manager) ZRevRangeWithScores(key string, start, stop int) []models.ZSetMember {
	return m.rangeOps.ZRevRangeWithScores(key, start, stop)
}

// Set operasyonlarının tamamlanması
func (m *Manager) ZUnionStore(destination string, keys []string, weights []float64) (int, error) {
	return m.setOps.ZUnionStore(destination, keys, weights)
}

func (m *Manager) ZInterStore(destination string, keys []string, weights []float64) (int, error) {
	return m.setOps.ZInterStore(destination, keys, weights)
}

func (m *Manager) ZDiff(keys ...string) []string {
	return m.setOps.ZDiff(keys...)
}

// Lex operasyonlarının tamamlanması
func (m *Manager) ZRevRangeByLex(key string, max, min string) []string {
	return m.lexOps.ZRevRangeByLex(key, max, min)
}

func (m *Manager) ZLexCount(key string, min, max string) (int, error) {
	return m.lexOps.ZLexCount(key, min, max)
}

// Score operasyonlarının tamamlanması
func (m *Manager) ZMScore(key string, members ...string) []float64 {
	return m.scoreOps.ZMScore(key, members...)
}

func (m *Manager) ZPopMax(key string) (models.ZSetMember, bool) {
	return m.rangeOps.ZPopMaxOne(key)
}

func (m *Manager) ZPopMaxN(key string, count int) []models.ZSetMember {
	return m.rangeOps.ZPopMax(key, count)
}

func (m *Manager) ZPopMin(key string) (models.ZSetMember, bool) {
	return m.rangeOps.ZPopMinOne(key)
}

func (m *Manager) ZPopMinN(key string, count int) []models.ZSetMember {
	return m.rangeOps.ZPopMin(key, count)
}

func (m *Manager) ZRandMember(key string, count int, withScores bool) []models.ZSetMember {
	return m.basicOps.ZRandMember(key, count, withScores)
}

func (m *Manager) ZRandMemberWithoutScores(key string, count int) []string {
	return m.basicOps.ZRandMemberWithoutScores(key, count)
}
