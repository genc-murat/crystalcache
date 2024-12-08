package handlers

import (
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
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	score, err := util.ParseFloat(args[1])
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not a valid float"}
	}

	err = h.cache.ZAdd(args[0].Bulk, score, args[2].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
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
