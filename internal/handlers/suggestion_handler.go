package handlers

import (
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type SuggestionHandlers struct {
	cache ports.Cache
}

func NewSuggestionHandlers(cache ports.Cache) *SuggestionHandlers {
	return &SuggestionHandlers{
		cache: cache,
	}
}

func (h *SuggestionHandlers) HandleFTSugAdd(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk
	str := args[1].Bulk

	score, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR score must be a valid float"}
	}

	// Handle optional arguments
	var opts []string
	for i := 3; i < len(args); i++ {
		opts = append(opts, args[i].Bulk)
	}

	added, err := h.cache.FTSugAdd(key, str, score, opts...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if added {
		return models.Value{Type: "string", Str: "OK"}
	}
	return models.Value{Type: "null"}
}

func (h *SuggestionHandlers) HandleFTSugDel(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk
	str := args[1].Bulk

	deleted, err := h.cache.FTSugDel(key, str)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if deleted {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *SuggestionHandlers) HandleFTSugGet(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk
	prefix := args[1].Bulk
	fuzzy := false
	max := 0

	// Parse options
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "FUZZY":
			fuzzy = true
		case "MAX":
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR MAX requires argument"}
			}
			var err error
			max, err = strconv.Atoi(args[i+1].Bulk)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR MAX must be numeric"}
			}
			i++
		}
	}

	suggestions, err := h.cache.FTSugGet(key, prefix, fuzzy, max)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	// Format results
	results := make([]models.Value, len(suggestions))
	for i, sug := range suggestions {
		result := []models.Value{
			{Type: "bulk", Bulk: sug.String},
			{Type: "bulk", Bulk: strconv.FormatFloat(sug.Score, 'f', -1, 64)},
		}
		if sug.Payload != "" {
			result = append(result, models.Value{Type: "bulk", Bulk: sug.Payload})
		}
		results[i] = models.Value{Type: "array", Array: result}
	}

	return models.Value{Type: "array", Array: results}
}

func (h *SuggestionHandlers) HandleFTSugLen(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk

	length, err := h.cache.FTSugLen(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: int(length)}
}
