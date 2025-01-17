package handlers

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type ZSetHandlers struct {
	cache ports.Cache
}

func NewZSetHandlers(cache ports.Cache) *ZSetHandlers {
	return &ZSetHandlers{cache: cache}
}

func (h *ZSetHandlers) HandleZAdd(args []models.Value) models.Value {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk

	for i := 1; i < len(args); i += 2 {
		score, err := util.ParseFloat(args[i])
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not a valid float"}
		}

		err = h.cache.ZAdd(key, score, args[i+1].Bulk)
		if err != nil {
			return util.ToValue(err)
		}
	}

	return models.Value{Type: "integer", Num: int((len(args) - 1) / 2)}
}

func (h *ZSetHandlers) HandleZCard(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	count := h.cache.ZCard(args[0].Bulk)
	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZCount(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	min, err := util.ParseFloat(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR min value is not a valid float"}
	}

	max, err := util.ParseFloat(args[2])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR max value is not a valid float"}
	}

	count := h.cache.ZCount(args[0].Bulk, min, max)
	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZRange(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	start, err := util.ParseInt(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	stop, err := util.ParseInt(args[2])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	withScores := false
	if len(args) == 4 && args[3].Bulk == "WITHSCORES" {
		withScores = true
	}

	if withScores {
		members := h.cache.ZRangeWithScores(args[0].Bulk, start, stop)
		result := make([]models.Value, len(members)*2)
		for i, member := range members {
			result[i*2] = models.Value{Type: "bulk", Bulk: member.Member}
			result[i*2+1] = models.Value{Type: "bulk", Bulk: util.FormatFloat(member.Score)}
		}
		return models.Value{Type: "array", Array: result}
	}

	members := h.cache.ZRange(args[0].Bulk, start, stop)
	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}
	return models.Value{Type: "array", Array: result}
}

func (h *ZSetHandlers) HandleZIncrBy(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	increment, err := util.ParseFloat(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR increment is not a valid float"}
	}

	score, err := h.cache.ZIncrBy(args[0].Bulk, increment, args[2].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "bulk", Bulk: util.FormatFloat(score)}
}

func (h *ZSetHandlers) HandleZRem(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	err := h.cache.ZRem(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}

func (h *ZSetHandlers) HandleZInterStore(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	numKeys, err := util.ParseInt(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR numkeys is not an integer"}
	}

	if len(args) < numKeys+2 {
		return models.Value{Type: "error", Str: "ERR not enough keys specified"}
	}

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = args[i+2].Bulk
	}

	var weights []float64
	weightStartIdx := numKeys + 2
	if len(args) > weightStartIdx && args[weightStartIdx].Bulk == "WEIGHTS" {
		if len(args) < weightStartIdx+numKeys+1 {
			return models.Value{Type: "error", Str: "ERR wrong number of weights"}
		}
		weights = make([]float64, numKeys)
		for i := 0; i < numKeys; i++ {
			weight, err := util.ParseFloat(args[weightStartIdx+i+1])
			if err != nil {
				return models.Value{Type: "error", Str: "ERR weight value is not a float"}
			}
			weights[i] = weight
		}
	}

	count, err := h.cache.ZInterStore(args[0].Bulk, keys, weights)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZDiff(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zdiff' command"}
	}

	// Convert all args to string slice for keys
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	members := h.cache.ZDiff(keys...)

	// Convert result to array of Values
	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *ZSetHandlers) HandleZDiffStore(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zdiffstore' command"}
	}

	destination := args[0].Bulk
	// Convert remaining args to string slice for source keys
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = args[i].Bulk
	}

	count, err := h.cache.ZDiffStore(destination, keys...)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZInter(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zinter' command"}
	}

	// Convert all args to string slice for keys
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	members := h.cache.ZInter(keys...)

	// Convert result to array of Values
	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *ZSetHandlers) HandleZInterCard(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zintercard' command"}
	}

	// Convert all args to string slice for keys
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	count, err := h.cache.ZInterCard(keys...)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZLexCount(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	key := args[0].Bulk
	min := args[1].Bulk
	max := args[2].Bulk

	count, err := h.cache.ZLexCount(key, min, max)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: count}
}

// HandleZMScore returns the scores of the specified members in a sorted set
func (h *ZSetHandlers) HandleZMScore(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zmscore' command"}
	}

	key := args[0].Bulk
	members := args[1:]
	result := make([]models.Value, len(members))

	for i, member := range members {
		score, exists := h.cache.ZScore(key, member.Bulk)
		if !exists {
			result[i] = models.Value{Type: "null"}
		} else {
			result[i] = models.Value{Type: "bulk", Bulk: util.FormatFloat(score)}
		}
	}

	return models.Value{Type: "array", Array: result}
}

// HandleZMPop removes and returns multiple elements from sorted sets
func (h *ZSetHandlers) HandleZMPop(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zmpop' command"}
	}

	// Parse number of keys
	numKeys, err := util.ParseInt(args[0])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
	}

	if len(args) < numKeys+2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zmpop' command"}
	}

	// Get keys
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = args[i+1].Bulk
	}

	// Parse direction (MIN/MAX)
	direction := args[numKeys+1].Bulk
	if direction != "MIN" && direction != "MAX" {
		return models.Value{Type: "error", Str: "ERR syntax error"}
	}

	// Parse count if provided
	count := 1
	if len(args) > numKeys+2 {
		if args[numKeys+2].Bulk != "COUNT" {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
		if len(args) <= numKeys+3 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zmpop' command"}
		}
		parsedCount, err := util.ParseInt(args[numKeys+3])
		if err != nil || parsedCount <= 0 {
			return models.Value{Type: "error", Str: "ERR value is not a valid integer or out of range"}
		}
		count = parsedCount
	}

	// Try to pop from each key until successful
	for _, key := range keys {
		// Check if the key exists and has elements
		if h.cache.ZCard(key) > 0 {
			var members []models.ZSetMember
			if direction == "MIN" {
				members = h.cache.ZRangeWithScores(key, 0, count-1)
			} else {
				members = h.cache.ZRevRangeWithScores(key, 0, count-1)
			}

			if len(members) == 0 {
				continue
			}

			// Remove the popped members
			for _, member := range members {
				err := h.cache.ZRem(key, member.Member)
				if err != nil {
					return util.ToValue(err)
				}
			}

			// Format result
			result := make([]models.Value, 2)
			result[0] = models.Value{Type: "bulk", Bulk: key}

			membersArray := make([]models.Value, len(members)*2)
			for i, member := range members {
				membersArray[i*2] = models.Value{Type: "bulk", Bulk: member.Member}
				membersArray[i*2+1] = models.Value{Type: "bulk", Bulk: util.FormatFloat(member.Score)}
			}
			result[1] = models.Value{Type: "array", Array: membersArray}

			return models.Value{Type: "array", Array: result}
		}
	}

	// No elements found in any key
	return models.Value{Type: "null"}
}

// HandleZPopMax removes and returns the highest scoring members
func (h *ZSetHandlers) HandleZPopMax(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zpopmax' command"}
	}

	// Parse optional count parameter
	count := 1
	if len(args) > 1 {
		parsedCount, err := util.ParseInt(args[1])
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
		if parsedCount <= 0 {
			return models.Value{Type: "error", Str: "ERR value is negative or zero"}
		}
		count = parsedCount
	}

	// Get the highest scoring members
	key := args[0].Bulk
	members := h.cache.ZRevRangeWithScores(key, 0, count-1)
	if len(members) == 0 {
		return models.Value{Type: "array", Array: []models.Value{}}
	}

	// Remove the members and prepare result
	result := make([]models.Value, len(members)*2)
	for i, member := range members {
		err := h.cache.ZRem(key, member.Member)
		if err != nil {
			return util.ToValue(err)
		}
		result[i*2] = models.Value{Type: "bulk", Bulk: member.Member}
		result[i*2+1] = models.Value{Type: "bulk", Bulk: util.FormatFloat(member.Score)}
	}

	return models.Value{Type: "array", Array: result}
}

// HandleZPopMin removes and returns the lowest scoring members
func (h *ZSetHandlers) HandleZPopMin(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zpopmin' command"}
	}

	// Parse optional count parameter
	count := 1
	if len(args) > 1 {
		parsedCount, err := util.ParseInt(args[1])
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
		if parsedCount <= 0 {
			return models.Value{Type: "error", Str: "ERR value is negative or zero"}
		}
		count = parsedCount
	}

	// Get the lowest scoring members
	key := args[0].Bulk
	members := h.cache.ZRangeWithScores(key, 0, count-1)
	if len(members) == 0 {
		return models.Value{Type: "array", Array: []models.Value{}}
	}

	// Remove the members and prepare result
	result := make([]models.Value, len(members)*2)
	for i, member := range members {
		err := h.cache.ZRem(key, member.Member)
		if err != nil {
			return util.ToValue(err)
		}
		result[i*2] = models.Value{Type: "bulk", Bulk: member.Member}
		result[i*2+1] = models.Value{Type: "bulk", Bulk: util.FormatFloat(member.Score)}
	}

	return models.Value{Type: "array", Array: result}
}

// HandleZRandMember returns random members from a sorted set
func (h *ZSetHandlers) HandleZRandMember(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zrandmember' command"}
	}

	key := args[0].Bulk
	count := 1
	withScores := false

	// Parse optional count and WITHSCORES
	if len(args) > 1 {
		parsedCount, err := util.ParseInt(args[1])
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer"}
		}
		count = parsedCount
	}

	if len(args) > 2 && args[2].Bulk == "WITHSCORES" {
		withScores = true
	}

	// Get all members with scores
	members := h.cache.ZRangeWithScores(key, 0, -1)
	if len(members) == 0 {
		return models.Value{Type: "null"}
	}

	// Handle negative count (sampling with replacement)
	if count < 0 {
		count = -count
		var capacity int
		if withScores {
			capacity = count * 2
		} else {
			capacity = count
		}
		result := make([]models.Value, 0, capacity)
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		for i := 0; i < count; i++ {
			idx := r.Intn(len(members))
			result = append(result, models.Value{Type: "bulk", Bulk: members[idx].Member})
			if withScores {
				result = append(result, models.Value{Type: "bulk", Bulk: util.FormatFloat(members[idx].Score)})
			}
		}
		return models.Value{Type: "array", Array: result}
	}

	// Handle positive count (sampling without replacement)
	if count > len(members) {
		count = len(members)
	}

	// Shuffle using Fisher-Yates algorithm
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := len(members) - 1; i > 0; i-- {
		j := r.Intn(i + 1)
		members[i], members[j] = members[j], members[i]
	}

	var capacity int
	if withScores {
		capacity = count * 2
	} else {
		capacity = count
	}
	result := make([]models.Value, 0, capacity)

	for i := 0; i < count; i++ {
		result = append(result, models.Value{Type: "bulk", Bulk: members[i].Member})
		if withScores {
			result = append(result, models.Value{Type: "bulk", Bulk: util.FormatFloat(members[i].Score)})
		}
	}

	return models.Value{Type: "array", Array: result}
}

// HandleZRangeByLex returns members between two lexicographical values
func (h *ZSetHandlers) HandleZRangeByLex(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zrangebylex' command"}
	}

	key := args[0].Bulk
	min := args[1].Bulk
	max := args[2].Bulk

	// Optional LIMIT offset count
	var offset, count int
	hasLimit := false

	if len(args) > 3 {
		if args[3].Bulk != "LIMIT" {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
		if len(args) != 6 {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
		var err error
		offset, err = util.ParseInt(args[4])
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
		count, err = util.ParseInt(args[5])
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
		hasLimit = true
	}

	members := h.cache.ZRangeByLex(key, min, max)

	// Apply LIMIT if specified
	if hasLimit && len(members) > 0 {
		if offset >= len(members) {
			members = []string{}
		} else {
			end := offset + count
			if end > len(members) {
				end = len(members)
			}
			members = members[offset:end]
		}
	}

	// Format result
	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

// HandleZRangeByScore returns members with scores between min and max
func (h *ZSetHandlers) HandleZRangeByScore(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zrangebyscore' command"}
	}

	key := args[0].Bulk
	min, err := util.ParseFloat(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR min value is not a valid float"}
	}

	max, err := util.ParseFloat(args[2])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR max value is not a valid float"}
	}

	withScores := false
	var offset, count int
	hasLimit := false

	// Parse optional WITHSCORES and LIMIT
	idx := 3
	for idx < len(args) {
		if args[idx].Bulk == "WITHSCORES" {
			withScores = true
			idx++
			continue
		}
		if args[idx].Bulk == "LIMIT" {
			if idx+2 >= len(args) {
				return models.Value{Type: "error", Str: "ERR syntax error"}
			}
			var err error
			offset, err = util.ParseInt(args[idx+1])
			if err != nil {
				return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
			}
			count, err = util.ParseInt(args[idx+2])
			if err != nil {
				return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
			}
			hasLimit = true
			idx += 3
			continue
		}
		return models.Value{Type: "error", Str: "ERR syntax error"}
	}

	var members []models.ZSetMember
	if withScores {
		members = h.cache.ZRangeByScoreWithScores(key, min, max)
	} else {
		stringMembers := h.cache.ZRangeByScore(key, min, max)
		members = make([]models.ZSetMember, len(stringMembers))
		for i, member := range stringMembers {
			score, _ := h.cache.ZScore(key, member)
			members[i] = models.ZSetMember{Member: member, Score: score}
		}
	}

	// Apply LIMIT if specified
	if hasLimit && len(members) > 0 {
		if offset >= len(members) {
			members = []models.ZSetMember{}
		} else {
			end := offset + count
			if end > len(members) {
				end = len(members)
			}
			members = members[offset:end]
		}
	}

	// Format result
	var result []models.Value
	if withScores {
		result = make([]models.Value, len(members)*2)
		for i, member := range members {
			result[i*2] = models.Value{Type: "bulk", Bulk: member.Member}
			result[i*2+1] = models.Value{Type: "bulk", Bulk: util.FormatFloat(member.Score)}
		}
	} else {
		result = make([]models.Value, len(members))
		for i, member := range members {
			result[i] = models.Value{Type: "bulk", Bulk: member.Member}
		}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *ZSetHandlers) HandleZRangeStore(args []models.Value) models.Value {
	if len(args) < 4 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zrangestore' command"}
	}

	destination := args[0].Bulk
	source := args[1].Bulk

	start, err := util.ParseInt(args[2])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	stop, err := util.ParseInt(args[3])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	withScores := false
	if len(args) > 4 && args[4].Bulk == "WITHSCORES" {
		withScores = true
	}

	count, err := h.cache.ZRangeStore(destination, source, start, stop, withScores)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZRemRangeByLex(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zremrangebylex' command"}
	}

	key := args[0].Bulk
	min := args[1].Bulk
	max := args[2].Bulk

	count, err := h.cache.ZRemRangeByLex(key, min, max)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZRemRangeByRank(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zremrangebyrank' command"}
	}

	start, err := util.ParseInt(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	stop, err := util.ParseInt(args[2])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	count, err := h.cache.ZRemRangeByRank(args[0].Bulk, start, stop)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZRemRangeByScore(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zremrangebyscore' command"}
	}

	min, err := util.ParseFloat(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR min value is not a valid float"}
	}

	max, err := util.ParseFloat(args[2])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR max value is not a valid float"}
	}

	count, err := h.cache.ZRemRangeByScore(args[0].Bulk, min, max)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZRank(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zrank' command"}
	}

	rank, exists := h.cache.ZRank(args[0].Bulk, args[1].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "integer", Num: rank}
}

func (h *ZSetHandlers) HandleZRevRange(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zrevrange' command"}
	}

	start, err := util.ParseInt(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	stop, err := util.ParseInt(args[2])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	withScores := false
	if len(args) == 4 && args[3].Bulk == "WITHSCORES" {
		withScores = true
	}

	if withScores {
		members := h.cache.ZRevRangeWithScores(args[0].Bulk, start, stop)
		result := make([]models.Value, len(members)*2)
		for i, member := range members {
			result[i*2] = models.Value{Type: "bulk", Bulk: member.Member}
			result[i*2+1] = models.Value{Type: "bulk", Bulk: util.FormatFloat(member.Score)}
		}
		return models.Value{Type: "array", Array: result}
	}

	members := h.cache.ZRevRange(args[0].Bulk, start, stop)
	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}
	return models.Value{Type: "array", Array: result}
}

func (h *ZSetHandlers) HandleZRevRangeByLex(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zrevrangebylex' command"}
	}

	key := args[0].Bulk
	max := args[1].Bulk
	min := args[2].Bulk

	// Optional LIMIT offset count
	var offset, count int
	hasLimit := false

	if len(args) > 3 {
		if args[3].Bulk != "LIMIT" {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
		if len(args) != 6 {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
		var err error
		offset, err = util.ParseInt(args[4])
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
		count, err = util.ParseInt(args[5])
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
		hasLimit = true
	}

	members := h.cache.ZRevRangeByLex(key, max, min)

	// Apply LIMIT if specified
	if hasLimit && len(members) > 0 {
		if offset >= len(members) {
			members = []string{}
		} else {
			end := offset + count
			if end > len(members) {
				end = len(members)
			}
			members = members[offset:end]
		}
	}

	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}
	return models.Value{Type: "array", Array: result}
}

func (h *ZSetHandlers) HandleZRevRangeByScore(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zrevrangebyscore' command"}
	}

	key := args[0].Bulk
	max, err := util.ParseFloat(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR max value is not a valid float"}
	}

	min, err := util.ParseFloat(args[2])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR min value is not a valid float"}
	}

	withScores := false
	var offset, count int
	hasLimit := false

	// Parse optional WITHSCORES and LIMIT
	idx := 3
	for idx < len(args) {
		if args[idx].Bulk == "WITHSCORES" {
			withScores = true
			idx++
			continue
		}
		if args[idx].Bulk == "LIMIT" {
			if idx+2 >= len(args) {
				return models.Value{Type: "error", Str: "ERR syntax error"}
			}
			var err error
			offset, err = util.ParseInt(args[idx+1])
			if err != nil {
				return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
			}
			count, err = util.ParseInt(args[idx+2])
			if err != nil {
				return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
			}
			hasLimit = true
			idx += 3
			continue
		}
		return models.Value{Type: "error", Str: "ERR syntax error"}
	}

	members := h.cache.ZRevRangeByScore(key, max, min)

	// Apply LIMIT if specified
	if hasLimit && len(members) > 0 {
		if offset >= len(members) {
			members = []string{}
		} else {
			end := offset + count
			if end > len(members) {
				end = len(members)
			}
			members = members[offset:end]
		}
	}

	var result []models.Value
	if withScores {
		result = make([]models.Value, len(members)*2)
		for i, member := range members {
			result[i*2] = models.Value{Type: "bulk", Bulk: member}
			score, _ := h.cache.ZScore(key, member)
			result[i*2+1] = models.Value{Type: "bulk", Bulk: util.FormatFloat(score)}
		}
	} else {
		result = make([]models.Value, len(members))
		for i, member := range members {
			result[i] = models.Value{Type: "bulk", Bulk: member}
		}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *ZSetHandlers) HandleZRevRank(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zrevrank' command"}
	}

	rank, exists := h.cache.ZRevRank(args[0].Bulk, args[1].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "integer", Num: rank}
}

func (h *ZSetHandlers) HandleZScan(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zscan' command"}
	}

	cursor, err := util.ParseInt(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid cursor"}
	}

	match := "*"
	count := 10

	// Parse optional MATCH and COUNT
	for i := 2; i < len(args); i++ {
		if args[i].Bulk == "MATCH" && i+1 < len(args) {
			match = args[i+1].Bulk
			i++
		} else if args[i].Bulk == "COUNT" && i+1 < len(args) {
			count, err = util.ParseInt(args[i+1])
			if err != nil {
				return models.Value{Type: "error", Str: "ERR value is not an integer"}
			}
			i++
		}
	}

	members, nextCursor := h.cache.ZScan(args[0].Bulk, cursor, match, count)

	// Format response
	response := make([]models.Value, 2)
	response[0] = models.Value{Type: "bulk", Bulk: strconv.Itoa(nextCursor)}

	// Add members and scores
	result := make([]models.Value, len(members)*2)
	for i, member := range members {
		result[i*2] = models.Value{Type: "bulk", Bulk: member.Member}
		result[i*2+1] = models.Value{Type: "bulk", Bulk: util.FormatFloat(member.Score)}
	}
	response[1] = models.Value{Type: "array", Array: result}

	return models.Value{Type: "array", Array: response}
}

func (h *ZSetHandlers) HandleZScore(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zscore' command"}
	}

	score, exists := h.cache.ZScore(args[0].Bulk, args[1].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: util.FormatFloat(score)}
}

func (h *ZSetHandlers) HandleZUnion(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zunion' command"}
	}

	numKeys, err := util.ParseInt(args[0])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	if len(args) < numKeys+1 {
		return models.Value{Type: "error", Str: "ERR not enough keys specified"}
	}

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = args[i+1].Bulk
	}

	withScores := false
	if len(args) > numKeys+1 && args[numKeys+1].Bulk == "WITHSCORES" {
		withScores = true
	}

	members, err := h.cache.ZUnion(keys...)
	if err != nil {
		// Handle the error appropriately. For now, return an error Value.
		return models.Value{Type: "error", Str: err.Error()}
	}

	if withScores {
		result := make([]models.Value, len(members)*2)
		for i, member := range members {
			result[i*2] = models.Value{Type: "bulk", Bulk: member.Member}
			result[i*2+1] = models.Value{Type: "bulk", Bulk: util.FormatFloat(member.Score)}
		}
		return models.Value{Type: "array", Array: result}
	}

	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member.Member}
	}
	return models.Value{Type: "array", Array: result}
}

func (h *ZSetHandlers) HandleZUnionStore(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'zunionstore' command"}
	}

	numKeys, err := util.ParseInt(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	if len(args) < numKeys+2 {
		return models.Value{Type: "error", Str: "ERR not enough keys specified"}
	}

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = args[i+2].Bulk
	}

	// Parse optional WEIGHTS
	var weights []float64
	currentArg := numKeys + 2
	if currentArg < len(args) && args[currentArg].Bulk == "WEIGHTS" {
		if len(args) < currentArg+numKeys+1 {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
		weights = make([]float64, numKeys)
		for i := 0; i < numKeys; i++ {
			weight, err := util.ParseFloat(args[currentArg+1+i])
			if err != nil {
				return models.Value{Type: "error", Str: "ERR weight value is not a float"}
			}
			weights[i] = weight
		}
	}

	count, err := h.cache.ZUnionStore(args[0].Bulk, keys, weights)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: count}
}

func (h *ZSetHandlers) HandleZRemRangeByRankCount(args []models.Value) models.Value {
	if len(args) != 4 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for ZREMRANGEBYRANKCOUNT command"}
	}

	key := args[0].Bulk

	start, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR start argument must be an integer"}
	}

	stop, err := strconv.Atoi(args[2].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR stop argument must be an integer"}
	}

	count, err := strconv.Atoi(args[3].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR count argument must be an integer"}
	}

	if count < 0 {
		return models.Value{Type: "error", Str: "ERR count cannot be negative"}
	}

	removed, err := h.cache.ZRemRangeByRankCount(key, start, stop, count)
	if err != nil {
		return models.Value{Type: "error", Str: fmt.Sprintf("ERR %v", err)}
	}

	return models.Value{Type: "integer", Num: removed}
}

func (h *ZSetHandlers) HandleZPopMinMaxBy(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'zpopminmaxby' command",
		}
	}

	key := args[0].String()
	by := args[1].String()
	isMax := args[2].String() == "max"

	// Default count is 1 if not specified
	count := 1
	if len(args) >= 5 && args[3].String() == "COUNT" {
		var err error
		count, err = strconv.Atoi(args[4].String())
		if err != nil || count <= 0 {
			return models.Value{
				Type: "error",
				Str:  "ERR value is not an integer or out of range",
			}
		}
	}

	// Validate 'by' parameter
	if by != "score" && by != "lex" {
		return models.Value{
			Type: "error",
			Str:  "ERR syntax error",
		}
	}

	result := h.cache.ZPopMinMaxBy(key, by, isMax, count)
	if len(result) == 0 {
		return models.Value{
			Type:  "array",
			Array: []models.Value{},
		}
	}

	// Convert result to array response
	response := make([]models.Value, len(result)*2)
	for i, member := range result {
		response[i*2] = models.Value{
			Type: "bulk",
			Bulk: member.Member,
		}
		response[i*2+1] = models.Value{
			Type: "bulk",
			Bulk: util.FormatFloat(member.Score),
		}
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}

func (h *ZSetHandlers) HandleZScanByScore(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'zscanbyscore' command",
		}
	}

	key := args[0].Bulk

	// Parse min score
	min, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR min value is not a valid float",
		}
	}

	// Parse max score
	max, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR max value is not a valid float",
		}
	}

	// Default values
	withScores := false
	count := 10

	// Parse optional arguments
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "WITHSCORES":
			withScores = true
		case "COUNT":
			if i+1 >= len(args) {
				return models.Value{
					Type: "error",
					Str:  "ERR COUNT option requires argument",
				}
			}
			count, err = strconv.Atoi(args[i+1].Bulk)
			if err != nil || count < 0 {
				return models.Value{
					Type: "error",
					Str:  "ERR value is not an integer or out of range",
				}
			}
			i++
		default:
			return models.Value{
				Type: "error",
				Str:  "ERR syntax error",
			}
		}
	}

	// Get matching members
	members := h.cache.ZScanByScore(key, min, max, count, withScores)

	// Format response based on WITHSCORES option
	var response []models.Value
	if withScores {
		response = make([]models.Value, len(members)*2)
		for i, member := range members {
			response[i*2] = models.Value{
				Type: "bulk",
				Bulk: member.Member,
			}
			response[i*2+1] = models.Value{
				Type: "bulk",
				Bulk: strconv.FormatFloat(member.Score, 'f', -1, 64),
			}
		}
	} else {
		response = make([]models.Value, len(members))
		for i, member := range members {
			response[i] = models.Value{
				Type: "bulk",
				Bulk: member.Member,
			}
		}
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}
