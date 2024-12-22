package handlers

import (
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type TDigestHandlers struct {
	cache ports.Cache
}

func NewTDigestHandlers(cache ports.Cache) *TDigestHandlers {
	return &TDigestHandlers{
		cache: cache,
	}
}

// HandleTDigestCreate handles TDIGEST.CREATE command
func (h *TDigestHandlers) HandleTDigestCreate(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.CREATE'"}
	}

	key := args[0].Bulk
	compression := 100.0 // default compression

	if len(args) > 1 {
		var err error
		compression, err = strconv.ParseFloat(args[1].Bulk, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid compression value"}
		}
	}

	err := h.cache.TDigestCreate(key, compression)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleTDigestAdd handles TDIGEST.ADD command
func (h *TDigestHandlers) HandleTDigestAdd(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.ADD'"}
	}

	key := args[0].Bulk
	var values []float64

	for i := 1; i < len(args); i++ {
		val, err := strconv.ParseFloat(args[i].Bulk, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid value"}
		}
		values = append(values, val)
	}

	err := h.cache.TDigestAdd(key, values...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleTDigestMerge handles TDIGEST.MERGE command
func (h *TDigestHandlers) HandleTDigestMerge(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.MERGE'"}
	}

	destKey := args[0].Bulk
	weights := make([]float64, 0)
	sourceKeys := make([]string, 0)

	i := 1
	if args[i].Bulk == "WEIGHTS" {
		i++
		for ; i < len(args); i += 2 {
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR weight not specified"}
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
			weights = append(weights, 1.0)
			sourceKeys = append(sourceKeys, args[i].Bulk)
		}
	}

	err := h.cache.TDigestMerge(destKey, sourceKeys, weights)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleTDigestReset handles TDIGEST.RESET command
func (h *TDigestHandlers) HandleTDigestReset(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.RESET'"}
	}

	key := args[0].Bulk
	err := h.cache.TDigestReset(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleTDigestQuantile handles TDIGEST.QUANTILE command
func (h *TDigestHandlers) HandleTDigestQuantile(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.QUANTILE'"}
	}

	key := args[0].Bulk
	var quantiles []float64

	for i := 1; i < len(args); i++ {
		q, err := strconv.ParseFloat(args[i].Bulk, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid quantile"}
		}
		if q < 0 || q > 1 {
			return models.Value{Type: "error", Str: "ERR quantile must be between 0 and 1"}
		}
		quantiles = append(quantiles, q)
	}

	results, err := h.cache.TDigestQuantile(key, quantiles...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, val := range results {
		response[i] = models.Value{Type: "bulk", Bulk: strconv.FormatFloat(val, 'f', -1, 64)}
	}

	return models.Value{Type: "array", Array: response}
}

// HandleTDigestMin handles TDIGEST.MIN command
func (h *TDigestHandlers) HandleTDigestMin(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.MIN'"}
	}

	key := args[0].Bulk
	min, err := h.cache.TDigestMin(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "bulk", Bulk: strconv.FormatFloat(min, 'f', -1, 64)}
}

// HandleTDigestMax handles TDIGEST.MAX command
func (h *TDigestHandlers) HandleTDigestMax(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.MAX'"}
	}

	key := args[0].Bulk
	max, err := h.cache.TDigestMax(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "bulk", Bulk: strconv.FormatFloat(max, 'f', -1, 64)}
}

// HandleTDigestInfo handles TDIGEST.INFO command
func (h *TDigestHandlers) HandleTDigestInfo(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.INFO'"}
	}

	key := args[0].Bulk
	info, err := h.cache.TDigestInfo(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := []models.Value{
		{Type: "bulk", Bulk: "Compression"},
		{Type: "bulk", Bulk: strconv.FormatFloat(info["compression"].(float64), 'f', -1, 64)},
		{Type: "bulk", Bulk: "Count"},
		{Type: "bulk", Bulk: strconv.FormatFloat(info["count"].(float64), 'f', -1, 64)},
		{Type: "bulk", Bulk: "Min"},
		{Type: "bulk", Bulk: strconv.FormatFloat(info["min"].(float64), 'f', -1, 64)},
		{Type: "bulk", Bulk: "Max"},
		{Type: "bulk", Bulk: strconv.FormatFloat(info["max"].(float64), 'f', -1, 64)},
		{Type: "bulk", Bulk: "Centroids"},
		{Type: "integer", Num: info["num_centroids"].(int)},
		{Type: "bulk", Bulk: "Memory"},
		{Type: "integer", Num: int(info["memory_usage"].(int64))},
	}

	return models.Value{Type: "array", Array: response}
}

// HandleTDigestCDF handles TDIGEST.CDF command
func (h *TDigestHandlers) HandleTDigestCDF(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.CDF'"}
	}

	key := args[0].Bulk
	var values []float64

	for i := 1; i < len(args); i++ {
		val, err := strconv.ParseFloat(args[i].Bulk, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid value"}
		}
		values = append(values, val)
	}

	results, err := h.cache.TDigestCDF(key, values...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, val := range results {
		response[i] = models.Value{Type: "bulk", Bulk: strconv.FormatFloat(val, 'f', -1, 64)}
	}

	return models.Value{Type: "array", Array: response}
}

// HandleTDigestTrimmedMean handles TDIGEST.TRIMMED_MEAN command
func (h *TDigestHandlers) HandleTDigestTrimmedMean(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'TDIGEST.TRIMMED_MEAN'"}
	}

	key := args[0].Bulk
	lowQ, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid lower quantile"}
	}

	highQ, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid upper quantile"}
	}

	mean, err := h.cache.TDigestTrimmedMean(key, lowQ, highQ)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "bulk", Bulk: strconv.FormatFloat(mean, 'f', -1, 64)}
}
