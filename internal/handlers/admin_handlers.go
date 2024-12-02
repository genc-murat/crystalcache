package handlers

import (
	"sort"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type AdminHandlers struct {
	cache ports.Cache
}

func NewAdminHandlers(cache ports.Cache) *AdminHandlers {
	return &AdminHandlers{cache: cache}
}

func (h *AdminHandlers) HandleFlushAll(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	h.cache.FlushAll()
	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) HandleInfo(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	info := h.cache.Info()
	var builder strings.Builder

	keys := make([]string, 0, len(info))
	for k := range info {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		builder.WriteString(k)
		builder.WriteString(":")
		builder.WriteString(info[k])
		builder.WriteString("\r\n")
	}

	return models.Value{Type: "bulk", Bulk: builder.String()}
}

func (h *AdminHandlers) HandleDBSize(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	size := h.cache.DBSize()
	return models.Value{Type: "integer", Num: size}
}

func (h *AdminHandlers) HandleMulti(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	err := h.cache.Multi()
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) HandleExec(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	results, err := h.cache.Exec()
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "array", Array: results}
}

func (h *AdminHandlers) HandleDiscard(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	err := h.cache.Discard()
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) HandleWatch(args []models.Value) models.Value {
	if err := util.ValidateMinArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	err := h.cache.Watch(keys...)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) HandleUnwatch(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	err := h.cache.Unwatch()
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}
