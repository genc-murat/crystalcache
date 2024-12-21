package handlers

import (
	"fmt"
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type CuckooHandlers struct {
	cache ports.Cache
}

func NewCuckooHandlers(cache ports.Cache) *CuckooHandlers {
	return &CuckooHandlers{
		cache: cache,
	}
}

// CF.RESERVE command
func (h *CuckooHandlers) HandleCFReserve(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.RESERVE' command"}
	}

	key := args[0].Bulk
	capacity, err := strconv.ParseUint(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid capacity"}
	}

	err = h.cache.CFReserve(key, capacity)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// CF.ADD command
func (h *CuckooHandlers) HandleCFAdd(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.ADD' command"}
	}

	key := args[0].Bulk
	item := args[1].Bulk

	added, err := h.cache.CFAdd(key, item)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: boolToInt(added)}
}

// CF.ADDNX command
func (h *CuckooHandlers) HandleCFAddNX(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.ADDNX' command"}
	}

	key := args[0].Bulk
	item := args[1].Bulk

	added, err := h.cache.CFAddNX(key, item)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: boolToInt(added)}
}

// CF.INSERT command
func (h *CuckooHandlers) HandleCFInsert(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.INSERT' command"}
	}

	key := args[0].Bulk
	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	results, err := h.cache.CFInsert(key, items)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, result := range results {
		response[i] = models.Value{Type: "integer", Num: boolToInt(result)}
	}

	return models.Value{Type: "array", Array: response}
}

// CF.INSERTNX command
func (h *CuckooHandlers) HandleCFInsertNX(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.INSERTNX' command"}
	}

	key := args[0].Bulk
	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	results, err := h.cache.CFInsertNX(key, items)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, result := range results {
		response[i] = models.Value{Type: "integer", Num: boolToInt(result)}
	}

	return models.Value{Type: "array", Array: response}
}

// CF.DEL command
func (h *CuckooHandlers) HandleCFDel(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.DEL' command"}
	}

	key := args[0].Bulk
	item := args[1].Bulk

	deleted, err := h.cache.CFDel(key, item)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: boolToInt(deleted)}
}

// CF.COUNT command
func (h *CuckooHandlers) HandleCFCount(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.COUNT' command"}
	}

	key := args[0].Bulk
	item := args[1].Bulk

	count, err := h.cache.CFCount(key, item)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: count}
}

// CF.EXISTS command
func (h *CuckooHandlers) HandleCFExists(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.EXISTS' command"}
	}

	key := args[0].Bulk
	item := args[1].Bulk

	exists, err := h.cache.CFExists(key, item)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: boolToInt(exists)}
}

// CF.MEXISTS command
func (h *CuckooHandlers) HandleCFMExists(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.MEXISTS' command"}
	}

	key := args[0].Bulk
	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	results, err := h.cache.CFMExists(key, items)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, result := range results {
		response[i] = models.Value{Type: "integer", Num: boolToInt(result)}
	}

	return models.Value{Type: "array", Array: response}
}

// CF.INFO command
func (h *CuckooHandlers) HandleCFInfo(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.INFO' command"}
	}

	key := args[0].Bulk
	info, err := h.cache.CFInfo(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := []models.Value{
		{Type: "bulk", Bulk: "Size"},
		{Type: "integer", Num: int(info.Size)},
		{Type: "bulk", Bulk: "Number of buckets"},
		{Type: "integer", Num: int(info.Size)},
		{Type: "bulk", Bulk: "Number of items"},
		{Type: "integer", Num: int(info.ItemCount)},
		{Type: "bulk", Bulk: "Bucket size"},
		{Type: "integer", Num: info.BucketSize},
		{Type: "bulk", Bulk: "Expansion rate"},
		{Type: "integer", Num: info.Expansion},
		{Type: "bulk", Bulk: "Filter filled ratio"},
		{Type: "bulk", Bulk: fmt.Sprintf("%.2f", info.FilterFilled)},
	}

	return models.Value{Type: "array", Array: response}
}

// CF.SCANDUMP command
func (h *CuckooHandlers) HandleCFScanDump(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.SCANDUMP' command"}
	}

	key := args[0].Bulk
	iter, err := strconv.ParseUint(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid iterator"}
	}

	nextIter, data, err := h.cache.CFScanDump(key, iter)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := []models.Value{
		{Type: "integer", Num: int(nextIter)},
		{Type: "bulk", Bulk: string(data)},
	}

	return models.Value{Type: "array", Array: response}
}

// CF.LOADCHUNK command
func (h *CuckooHandlers) HandleCFLoadChunk(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CF.LOADCHUNK' command"}
	}

	key := args[0].Bulk
	iter, err := strconv.ParseUint(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid iterator"}
	}

	data := []byte(args[2].Bulk)
	err = h.cache.CFLoadChunk(key, iter, data)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}
