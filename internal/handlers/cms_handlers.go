package handlers

import (
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type CMSHandlers struct {
	cache ports.Cache
}

func NewCMSHandlers(cache ports.Cache) *CMSHandlers {
	return &CMSHandlers{
		cache: cache,
	}
}

// HandleCMSInitByDim handles CMS.INITBYDIM command
func (h *CMSHandlers) HandleCMSInitByDim(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CMS.INITBYDIM' command"}
	}

	key := args[0].Bulk
	width, err := strconv.ParseUint(args[1].Bulk, 10, 32)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid width"}
	}

	depth, err := strconv.ParseUint(args[2].Bulk, 10, 32)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid depth"}
	}

	err = h.cache.CMSInitByDim(key, uint(width), uint(depth))
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleCMSInitByProb handles CMS.INITBYPROB command
func (h *CMSHandlers) HandleCMSInitByProb(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CMS.INITBYPROB' command"}
	}

	key := args[0].Bulk
	epsilon, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid epsilon"}
	}

	delta, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid delta"}
	}

	err = h.cache.CMSInitByProb(key, epsilon, delta)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleCMSIncrBy handles CMS.INCRBY command
func (h *CMSHandlers) HandleCMSIncrBy(args []models.Value) models.Value {
	if len(args) < 3 || len(args)%2 != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CMS.INCRBY' command"}
	}

	key := args[0].Bulk
	var items []string
	var increments []uint64

	for i := 1; i < len(args); i += 2 {
		item := args[i].Bulk
		increment, err := strconv.ParseUint(args[i+1].Bulk, 10, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid increment"}
		}
		items = append(items, item)
		increments = append(increments, increment)
	}

	err := h.cache.CMSIncrBy(key, items, increments)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleCMSQuery handles CMS.QUERY command
func (h *CMSHandlers) HandleCMSQuery(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CMS.QUERY' command"}
	}

	key := args[0].Bulk
	var items []string
	for i := 1; i < len(args); i++ {
		items = append(items, args[i].Bulk)
	}

	counts, err := h.cache.CMSQuery(key, items)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	results := make([]models.Value, len(counts))
	for i, count := range counts {
		results[i] = models.Value{Type: "integer", Num: int(count)}
	}

	return models.Value{Type: "array", Array: results}
}

// HandleCMSMerge handles CMS.MERGE command
func (h *CMSHandlers) HandleCMSMerge(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CMS.MERGE' command"}
	}

	destination := args[0].Bulk
	weight := 1.0
	var sourceKeys []string
	var weights []float64

	i := 1
	if strings.ToUpper(args[i].Bulk) == "WEIGHTS" {
		i++
		for ; i < len(args); i += 2 {
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR wrong number of weights"}
			}
			w, err := strconv.ParseFloat(args[i].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR invalid weight"}
			}
			weights = append(weights, w)
			sourceKeys = append(sourceKeys, args[i+1].Bulk)
		}
	} else {
		for ; i < len(args); i++ {
			sourceKeys = append(sourceKeys, args[i].Bulk)
			weights = append(weights, weight)
		}
	}

	err := h.cache.CMSMerge(destination, sourceKeys, weights)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleCMSInfo handles CMS.INFO command
func (h *CMSHandlers) HandleCMSInfo(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'CMS.INFO' command"}
	}

	key := args[0].Bulk
	info, err := h.cache.CMSInfo(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := []models.Value{
		{Type: "bulk", Bulk: "width"},
		{Type: "integer", Num: int(info["width"].(uint))},
		{Type: "bulk", Bulk: "depth"},
		{Type: "integer", Num: int(info["depth"].(uint))},
		{Type: "bulk", Bulk: "count"},
		{Type: "integer", Num: int(info["count"].(uint64))},
	}

	return models.Value{Type: "array", Array: result}
}
