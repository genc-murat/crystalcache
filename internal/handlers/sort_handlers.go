package handlers

import (
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type SortHandlers struct {
	cache ports.Cache
}

func NewSortHandlers(cache ports.Cache) *SortHandlers {
	return &SortHandlers{
		cache: cache,
	}
}

func (h *SortHandlers) HandleSort(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'sort' command"}
	}

	key := args[0].Bulk
	var desc bool
	var alpha bool
	var limit bool
	var start, count int
	var store string

	// Parse options
	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "DESC":
			desc = true
		case "ALPHA":
			alpha = true
		case "LIMIT":
			if i+2 >= len(args) {
				return models.Value{Type: "error", Str: "ERR syntax error"}
			}
			start, _ = strconv.Atoi(args[i+1].Bulk)
			count, _ = strconv.Atoi(args[i+2].Bulk)
			limit = true
			i += 2
		case "STORE":
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR syntax error"}
			}
			store = args[i+1].Bulk
			i++
		}
	}

	// Call the cache's Sort method
	result, err := h.cache.Sort(key, desc, alpha, limit, start, count, store)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if store != "" {
		return models.Value{Type: "integer", Num: len(result)}
	}

	// Convert result to Value array
	values := make([]models.Value, len(result))
	for i, v := range result {
		values[i] = models.Value{Type: "bulk", Bulk: v}
	}
	return models.Value{Type: "array", Array: values}
}

// HandleSortRO handles the SORT_RO command (read-only variant)
func (h *SortHandlers) HandleSortRO(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'sort_ro' command"}
	}

	// SORT_RO doesn't support STORE option
	for _, arg := range args {
		if strings.ToUpper(arg.Bulk) == "STORE" {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
	}

	// Reuse HandleSort logic but ensure no STORE operation
	return h.HandleSort(args)
}
