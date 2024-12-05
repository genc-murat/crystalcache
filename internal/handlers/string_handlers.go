package handlers

import (
	"log"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type StringHandlers struct {
	cache ports.Cache
}

func NewStringHandlers(cache ports.Cache) *StringHandlers {
	return &StringHandlers{cache: cache}
}

func (h *StringHandlers) HandleSet(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk
	nx := false
	xx := false

	// Parse optional arguments
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		}
	}

	// Check NX/XX conditions
	exists := h.cache.Exists(key)
	if (nx && exists) || (xx && !exists) {
		return models.Value{Type: "null"}
	}

	err := h.cache.Set(key, value)
	if err != nil {
		return util.ToValue(err)
	}

	log.Printf("[DEBUG] SET key=%s value=%s nx=%v xx=%v", key, value, nx, xx)
	return models.Value{Type: "string", Str: "OK"}
}

func (h *StringHandlers) HandleGet(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.Get(args[0].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

func (h *StringHandlers) HandleIncr(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	result, err := h.cache.Incr(args[0].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: result}
}

func (h *StringHandlers) HandleDel(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	deleted, err := h.cache.Del(args[0].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	if deleted {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *StringHandlers) HandleExists(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	exists := h.cache.Exists(args[0].Bulk)
	if exists {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *StringHandlers) HandleExpire(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	seconds, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	err = h.cache.Expire(args[0].Bulk, seconds)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}

func (h *StringHandlers) HandleStrlen(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.Get(args[0].Bulk)
	if !exists {
		return models.Value{Type: "integer", Num: 0}
	}

	return models.Value{Type: "integer", Num: len(value)}
}

func (h *StringHandlers) HandleGetRange(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	start, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	end, err := util.ParseInt(args[2])
	if err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.Get(args[0].Bulk)
	if !exists {
		return models.Value{Type: "bulk", Bulk: ""}
	}

	length := len(value)
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return models.Value{Type: "bulk", Bulk: ""}
	}

	return models.Value{Type: "bulk", Bulk: value[start : end+1]}
}

func (h *StringHandlers) HandleEcho(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'echo' command"}
	}
	return models.Value{Type: "bulk", Bulk: args[0].Bulk}
}
