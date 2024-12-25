package handlers

import (
	"fmt"
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type BloomFilterHandlers struct {
	cache ports.Cache
}

func NewBloomFilterHandlers(cache ports.Cache) *BloomFilterHandlers {
	return &BloomFilterHandlers{
		cache: cache,
	}
}

// HandleBFAdd handles BF.ADD command
func (h *BloomFilterHandlers) HandleBFAdd(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.ADD' command",
		}
	}

	added, err := h.cache.BFAdd(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "integer",
		Num:  boolToInt(added),
	}
}

// HandleBFInsert handles BF.INSERT command
func (h *BloomFilterHandlers) HandleBFInsert(args []models.Value) models.Value {
	if len(args) < 4 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.INSERT' command",
		}
	}

	errorRate, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid error rate. Must be between 0 and 1",
		}
	}

	capacity, err := strconv.ParseUint(args[2].Bulk, 10, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid capacity. Must be a positive integer",
		}
	}

	items := make([]string, len(args)-3)
	for i := 3; i < len(args); i++ {
		items[i-3] = args[i].Bulk
	}

	results, err := h.cache.BFInsert(args[0].Bulk, errorRate, uint(capacity), items)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	response := make([]models.Value, len(results))
	for i, added := range results {
		response[i] = models.Value{
			Type: "integer",
			Num:  boolToInt(added),
		}
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}

// HandleBFExists handles BF.EXISTS command
func (h *BloomFilterHandlers) HandleBFExists(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.EXISTS' command",
		}
	}

	exists, err := h.cache.BFExists(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "integer",
		Num:  boolToInt(exists),
	}
}

// HandleBFReserve handles BF.RESERVE command
func (h *BloomFilterHandlers) HandleBFReserve(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.RESERVE' command",
		}
	}

	errorRate, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid error rate. Must be between 0 and 1",
		}
	}

	capacity, err := strconv.ParseUint(args[2].Bulk, 10, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid capacity. Must be a positive integer",
		}
	}

	err = h.cache.BFReserve(args[0].Bulk, errorRate, uint(capacity))
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "string",
		Str:  "OK",
	}
}

// HandleBFMAdd handles BF.MADD command
func (h *BloomFilterHandlers) HandleBFMAdd(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.MADD' command",
		}
	}

	key := args[0].Bulk
	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	results, err := h.cache.BFMAdd(key, items)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	response := make([]models.Value, len(results))
	for i, added := range results {
		response[i] = models.Value{
			Type: "integer",
			Num:  boolToInt(added),
		}
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}

// HandleBFMExists handles BF.MEXISTS command
func (h *BloomFilterHandlers) HandleBFMExists(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.MEXISTS' command",
		}
	}

	key := args[0].Bulk
	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	results, err := h.cache.BFMExists(key, items)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	response := make([]models.Value, len(results))
	for i, exists := range results {
		response[i] = models.Value{
			Type: "integer",
			Num:  boolToInt(exists),
		}
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}

// HandleBFInfo handles BF.INFO command
func (h *BloomFilterHandlers) HandleBFInfo(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.INFO' command",
		}
	}

	info, err := h.cache.BFInfo(args[0].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	// Convert map to array of key-value pairs
	response := make([]models.Value, 0, len(info)*2)
	for k, v := range info {
		response = append(response,
			models.Value{Type: "bulk", Bulk: k},
			models.Value{Type: "bulk", Bulk: fmt.Sprintf("%v", v)},
		)
	}

	return models.Value{
		Type:  "array",
		Array: response,
	}
}

// HandleBFCard handles BF.CARD command
func (h *BloomFilterHandlers) HandleBFCard(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.CARD' command",
		}
	}

	card, err := h.cache.BFCard(args[0].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "integer",
		Num:  int(card),
	}
}

// HandleBFScanDump handles BF.SCANDUMP command
func (h *BloomFilterHandlers) HandleBFScanDump(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.SCANDUMP' command",
		}
	}

	iterator, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid iterator value",
		}
	}

	nextIterator, data, err := h.cache.BFScanDump(args[0].Bulk, iterator)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "array",
		Array: []models.Value{
			{Type: "integer", Num: nextIterator},
			{Type: "bulk", Bulk: string(data)},
		},
	}
}

// HandleBFLoadChunk handles BF.LOADCHUNK command
func (h *BloomFilterHandlers) HandleBFLoadChunk(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'BF.LOADCHUNK' command",
		}
	}

	iterator, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR invalid iterator value",
		}
	}

	err = h.cache.BFLoadChunk(args[0].Bulk, iterator, []byte(args[2].Bulk))
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  fmt.Sprintf("ERR %v", err),
		}
	}

	return models.Value{
		Type: "string",
		Str:  "OK",
	}
}
