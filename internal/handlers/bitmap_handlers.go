package handlers

import (
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type BitMapHandlers struct {
	cache ports.Cache
}

func NewBitMapHandlers(cache ports.Cache) *BitMapHandlers {
	return &BitMapHandlers{cache: cache}
}

func (h *BitMapHandlers) HandleGetBit(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'getbit' command"}
	}

	offset, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
	}

	if offset < 0 {
		return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
	}

	bit, err := h.cache.GetBit(args[0].Bulk, offset)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: bit}
}

func (h *BitMapHandlers) HandleSetBit(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'setbit' command"}
	}

	offset, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
	}

	if offset < 0 {
		return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
	}

	value, err := strconv.Atoi(args[2].Bulk)
	if err != nil || (value != 0 && value != 1) {
		return models.Value{Type: "error", Str: "ERR bit is not an integer or out of range"}
	}

	oldBit, err := h.cache.SetBit(args[0].Bulk, offset, value)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: oldBit}
}
