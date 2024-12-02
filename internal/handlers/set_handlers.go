package handlers

import (
	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type SetHandlers struct {
	cache ports.Cache
}

func NewSetHandlers(cache ports.Cache) *SetHandlers {
	return &SetHandlers{cache: cache}
}

func (h *SetHandlers) HandleSAdd(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	added, err := h.cache.SAdd(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	if added {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *SetHandlers) HandleSMembers(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	members, err := h.cache.SMembers(args[0].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSCard(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	count := h.cache.SCard(args[0].Bulk)
	return models.Value{Type: "integer", Num: count}
}

func (h *SetHandlers) HandleSRem(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	removed, err := h.cache.SRem(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	if removed {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *SetHandlers) HandleSIsMember(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	isMember := h.cache.SIsMember(args[0].Bulk, args[1].Bulk)
	if isMember {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *SetHandlers) HandleSInter(args []models.Value) models.Value {
	if err := util.ValidateMinArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	intersection := h.cache.SInter(keys...)
	result := make([]models.Value, len(intersection))
	for i, member := range intersection {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSUnion(args []models.Value) models.Value {
	if err := util.ValidateMinArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	union := h.cache.SUnion(keys...)
	result := make([]models.Value, len(union))
	for i, member := range union {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSDiff(args []models.Value) models.Value {
	if err := util.ValidateMinArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	diff := h.cache.SDiff(keys...)
	result := make([]models.Value, len(diff))
	for i, member := range diff {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}
