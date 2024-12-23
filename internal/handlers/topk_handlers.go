package handlers

import (
	"fmt"
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type TopKHandlers struct {
	cache ports.Cache
}

func NewTopKHandlers(cache ports.Cache) *TopKHandlers {
	return &TopKHandlers{
		cache: cache,
	}
}

// HandleTOPKReserve handles TOPK.RESERVE command
func (h *TopKHandlers) HandleTOPKReserve(args []models.Value) models.Value {
	if len(args) != 4 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'TOPK.RESERVE' command",
		}
	}

	topk, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid topk"}
	}

	capacity, err := strconv.Atoi(args[2].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid capacity"}
	}

	decay, err := strconv.ParseFloat(args[3].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid decay"}
	}

	err = h.cache.TOPKReserve(args[0].Bulk, topk, capacity, decay)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleTOPKAdd handles TOPK.ADD command
func (h *TopKHandlers) HandleTOPKAdd(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'TOPK.ADD' command",
		}
	}

	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	results, err := h.cache.TOPKAdd(args[0].Bulk, items...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, added := range results {
		if added {
			response[i] = models.Value{Type: "integer", Num: 1}
		} else {
			response[i] = models.Value{Type: "integer", Num: 0}
		}
	}

	return models.Value{Type: "array", Array: response}
}

// HandleTOPKIncrBy handles TOPK.INCRBY command
func (h *TopKHandlers) HandleTOPKIncrBy(args []models.Value) models.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'TOPK.INCRBY' command",
		}
	}

	itemsWithCount := make(map[string]int64)
	for i := 1; i < len(args); i += 2 {
		count, err := strconv.ParseInt(args[i+1].Bulk, 10, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid increment"}
		}
		itemsWithCount[args[i].Bulk] = count
	}

	results, err := h.cache.TOPKIncrBy(args[0].Bulk, itemsWithCount)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, added := range results {
		if added {
			response[i] = models.Value{Type: "integer", Num: 1}
		} else {
			response[i] = models.Value{Type: "integer", Num: 0}
		}
	}

	return models.Value{Type: "array", Array: response}
}

// HandleTOPKQuery handles TOPK.QUERY command
func (h *TopKHandlers) HandleTOPKQuery(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'TOPK.QUERY' command",
		}
	}

	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	results, err := h.cache.TOPKQuery(args[0].Bulk, items...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, exists := range results {
		if exists {
			response[i] = models.Value{Type: "integer", Num: 1}
		} else {
			response[i] = models.Value{Type: "integer", Num: 0}
		}
	}

	return models.Value{Type: "array", Array: response}
}

// HandleTOPKCount handles TOPK.COUNT command
func (h *TopKHandlers) HandleTOPKCount(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'TOPK.COUNT' command",
		}
	}

	items := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		items[i-1] = args[i].Bulk
	}

	counts, err := h.cache.TOPKCount(args[0].Bulk, items...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(counts))
	for i, count := range counts {
		response[i] = models.Value{Type: "integer", Num: int(count)}
	}

	return models.Value{Type: "array", Array: response}
}

// HandleTOPKList handles TOPK.LIST command
func (h *TopKHandlers) HandleTOPKList(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'TOPK.LIST' command",
		}
	}

	items, err := h.cache.TOPKList(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	// Return array of arrays, where each inner array contains [item, count]
	response := make([]models.Value, len(items))
	for i, item := range items {
		response[i] = models.Value{
			Type: "array",
			Array: []models.Value{
				{Type: "bulk", Bulk: item.Item},
				{Type: "integer", Num: int(item.Count)},
			},
		}
	}

	return models.Value{Type: "array", Array: response}
}

// HandleTOPKInfo handles TOPK.INFO command
func (h *TopKHandlers) HandleTOPKInfo(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'TOPK.INFO' command",
		}
	}

	info, err := h.cache.TOPKInfo(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	// Convert info map to array of key-value pairs
	response := make([]models.Value, 0, len(info)*2)
	for key, value := range info {
		response = append(response,
			models.Value{Type: "bulk", Bulk: key},
			models.Value{Type: "bulk", Bulk: fmt.Sprintf("%v", value)},
		)
	}

	return models.Value{Type: "array", Array: response}
}
