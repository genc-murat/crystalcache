package handlers

import (
	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type ListHandlers struct {
	cache ports.Cache
}

func NewListHandlers(cache ports.Cache) *ListHandlers {
	return &ListHandlers{cache: cache}
}

func (h *ListHandlers) HandleLPush(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'lpush' command"}
	}

	key := args[0].Bulk
	totalLen := 0
	var err error

	// Handle multiple values
	for i := 1; i < len(args); i++ {
		totalLen, err = h.cache.LPush(key, args[i].Bulk)
		if err != nil {
			return util.ToValue(err)
		}
	}

	return models.Value{Type: "integer", Num: totalLen}
}

func (h *ListHandlers) HandleRPush(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := args[0].Bulk
	totalLen := 0
	var err error

	// Handle multiple values
	for i := 1; i < len(args); i++ {
		totalLen, err = h.cache.RPush(key, args[i].Bulk)
		if err != nil {
			return util.ToValue(err)
		}
	}

	return models.Value{Type: "integer", Num: totalLen}
}

func (h *ListHandlers) HandleLRange(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	start, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	stop, err := util.ParseInt(args[2])
	if err != nil {
		return util.ToValue(err)
	}

	values, err := h.cache.LRange(args[0].Bulk, start, stop)
	if err != nil {
		return util.ToValue(err)
	}

	result := make([]models.Value, len(values))
	for i, value := range values {
		result[i] = models.Value{Type: "bulk", Bulk: value}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *ListHandlers) HandleLPop(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.LPop(args[0].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

func (h *ListHandlers) HandleRPop(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.RPop(args[0].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

func (h *ListHandlers) HandleLLen(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	length := h.cache.LLen(args[0].Bulk)
	return models.Value{Type: "integer", Num: length}
}

func (h *ListHandlers) HandleLSet(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	index, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	err = h.cache.LSet(args[0].Bulk, index, args[2].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *ListHandlers) HandleLRem(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	count, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	removed, err := h.cache.LRem(args[0].Bulk, count, args[2].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: removed}
}
