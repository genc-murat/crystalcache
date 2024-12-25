package handlers

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type TimeSeriesHandlers struct {
	cache ports.Cache
}

func NewTimeSeriesHandlers(cache ports.Cache) *TimeSeriesHandlers {
	return &TimeSeriesHandlers{cache: cache}
}

func parseLabels(args []models.Value) map[string]string {
	labels := make(map[string]string)

	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			key := args[i].Str
			value := args[i+1].Str
			labels[key] = value
		}
	}

	return labels
}

// TS.CREATE Handler
func (h *TimeSeriesHandlers) HandleTSCreate(args []models.Value) models.Value {
	key := args[0].Str
	labels := parseLabels(args[1:])
	if err := h.cache.TSCreate(key, labels); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}
	return models.Value{Type: "string", Str: "OK"}
}

// TS.ADD Handler
func (h *TimeSeriesHandlers) HandleTSAdd(args []models.Value) models.Value {
	key := args[0].Str
	timestamp, _ := strconv.ParseInt(args[1].Str, 10, 64)
	value, _ := strconv.ParseFloat(args[2].Str, 64)

	if err := h.cache.TSAdd(key, timestamp, value); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}
	return models.Value{Type: "string", Str: "OK"}
}

// TS.MADD Handler
func (h *TimeSeriesHandlers) HandleTSMAdd(args []models.Value) models.Value {
	entries := make(map[string][]models.TimeSeriesSample)
	for i := 0; i < len(args); i += 3 {
		key := args[i].Str
		timestamp, _ := strconv.ParseInt(args[i+1].Str, 10, 64)
		value, _ := strconv.ParseFloat(args[i+2].Str, 64)
		entries[key] = append(entries[key], models.TimeSeriesSample{Timestamp: timestamp, Value: value})
	}

	if err := h.cache.TSMAdd(entries); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}
	return models.Value{Type: "string", Str: "OK"}
}

// TS.RANGE Handler
func (h *TimeSeriesHandlers) HandleTSRange(args []models.Value) models.Value {
	key := args[0].Str
	from, _ := strconv.ParseInt(args[1].Str, 10, 64)
	to, _ := strconv.ParseInt(args[2].Str, 10, 64)

	samples, err := h.cache.TSRange(key, from, to)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, len(samples))
	for i, sample := range samples {
		result[i] = models.Value{Type: "array", Array: []models.Value{
			{Type: "integer", Num: int(sample.Timestamp)},
			{Type: "float", Str: fmt.Sprintf("%f", sample.Value)},
		}}
	}
	return models.Value{Type: "array", Array: result}
}

// TS.INFO Handler
func (h *TimeSeriesHandlers) HandleTSInfo(args []models.Value) models.Value {
	key := args[0].Str

	stats, err := h.cache.TSInfo(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "array", Array: []models.Value{
		{Type: "string", Str: "total_samples"},
		{Type: "integer", Num: stats.TotalSamples},
		{Type: "string", Str: "max_value"},
		{Type: "float", Str: fmt.Sprintf("%f", stats.MaxValue)},
		{Type: "string", Str: "min_value"},
		{Type: "float", Str: fmt.Sprintf("%f", stats.MinValue)},
		{Type: "string", Str: "avg_value"},
		{Type: "float", Str: fmt.Sprintf("%f", stats.AvgValue)},
	}}
}

// TS.INCRBY Handler
func (h *TimeSeriesHandlers) HandleTSIncrBy(args []models.Value) models.Value {
	key := args[0].Str
	increment, _ := strconv.ParseFloat(args[1].Str, 64)

	if err := h.cache.TSIncrBy(key, increment); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}
	return models.Value{Type: "string", Str: "OK"}
}

// TS.DECRBY Handler
func (h *TimeSeriesHandlers) HandleTSDecrBy(args []models.Value) models.Value {
	key := args[0].Str
	decrement, _ := strconv.ParseFloat(args[1].Str, 64)

	if err := h.cache.TSDecrBy(key, decrement); err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}
	return models.Value{Type: "string", Str: "OK"}
}

// TS.DEL Handler
func (h *TimeSeriesHandlers) HandleTSDel(args []models.Value) models.Value {
	key := args[0].Str
	from, _ := strconv.ParseInt(args[1].Str, 10, 64)
	to, _ := strconv.ParseInt(args[2].Str, 10, 64)

	count, err := h.cache.TSDel(key, from, to)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}
	return models.Value{Type: "integer", Num: int(count)}
}

func (h *TimeSeriesHandlers) HandleTSAlter(args []models.Value) models.Value {
	key := args[0].Str
	labels := parseLabels(args[1:])

	err := h.cache.TSAlter(key, labels)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}
	return models.Value{Type: "string", Str: "OK"}
}

func (h *TimeSeriesHandlers) HandleTSCreateRule(args []models.Value) models.Value {
	sourceKey := args[0].Str
	destKey := args[1].Str
	aggregationType := args[2].Str
	bucketSize, err := strconv.ParseInt(args[3].Str, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid bucket size"}
	}

	err = h.cache.TSCreateRule(sourceKey, destKey, aggregationType, bucketSize)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}
	return models.Value{Type: "string", Str: "OK"}
}

func (h *TimeSeriesHandlers) HandleTSGet(args []models.Value) models.Value {
	key := args[0].Str

	sample, err := h.cache.TSGet(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "array", Array: []models.Value{
		{Type: "integer", Num: int(sample.Timestamp)},
		{Type: "float", Float: sample.Value},
	}}
}

func (h *TimeSeriesHandlers) HandleTSMGet(args []models.Value) models.Value {
	filters := make(map[string]string)

	for _, arg := range args {
		keyValue := strings.SplitN(arg.Str, "=", 2)
		if len(keyValue) == 2 {
			filters[keyValue[0]] = keyValue[1]
		}
	}

	results, err := h.cache.TSMGet(filters)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	resultArray := []models.Value{}
	for key, sample := range results {
		resultArray = append(resultArray, models.Value{Type: "array", Array: []models.Value{
			{Type: "string", Str: key},
			{Type: "integer", Num: int(sample.Timestamp)},
			{Type: "float", Float: sample.Value},
		}})
	}

	return models.Value{Type: "array", Array: resultArray}
}

func (h *TimeSeriesHandlers) HandleTSMRange(args []models.Value) models.Value {
	from, _ := strconv.ParseInt(args[0].Str, 10, 64)
	to, _ := strconv.ParseInt(args[1].Str, 10, 64)
	filters := make(map[string]string)

	// Filtreleri ayıkla
	for _, arg := range args[2:] {
		keyValue := strings.SplitN(arg.Str, "=", 2)
		if len(keyValue) == 2 {
			filters[keyValue[0]] = keyValue[1]
		}
	}

	results, err := h.cache.TSMRange(filters, from, to)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	// Sonuçları dönüştür
	resultArray := []models.Value{}
	for key, samples := range results {
		sampleArray := []models.Value{}
		for _, sample := range samples {
			sampleArray = append(sampleArray, models.Value{Type: "array", Array: []models.Value{
				{Type: "integer", Num: int(sample.Timestamp)},
				{Type: "float", Float: sample.Value},
			}})
		}
		resultArray = append(resultArray, models.Value{Type: "array", Array: []models.Value{
			{Type: "string", Str: key},
			{Type: "array", Array: sampleArray},
		}})
	}

	return models.Value{Type: "array", Array: resultArray}
}

func (h *TimeSeriesHandlers) HandleTSMRevRange(args []models.Value) models.Value {
	from, _ := strconv.ParseInt(args[0].Str, 10, 64)
	to, _ := strconv.ParseInt(args[1].Str, 10, 64)
	filters := make(map[string]string)

	// Filtreleri ayıkla
	for _, arg := range args[2:] {
		keyValue := strings.SplitN(arg.Str, "=", 2)
		if len(keyValue) == 2 {
			filters[keyValue[0]] = keyValue[1]
		}
	}

	results, err := h.cache.TSMRevRange(filters, from, to)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	// Sonuçları dönüştür
	resultArray := []models.Value{}
	for key, samples := range results {
		sampleArray := []models.Value{}
		for _, sample := range samples {
			sampleArray = append(sampleArray, models.Value{Type: "array", Array: []models.Value{
				{Type: "integer", Num: int(sample.Timestamp)},
				{Type: "float", Float: sample.Value},
			}})
		}
		resultArray = append(resultArray, models.Value{Type: "array", Array: []models.Value{
			{Type: "string", Str: key},
			{Type: "array", Array: sampleArray},
		}})
	}

	return models.Value{Type: "array", Array: resultArray}
}

func (h *TimeSeriesHandlers) HandleTSRevRange(args []models.Value) models.Value {
	key := args[0].Str
	from, _ := strconv.ParseInt(args[1].Str, 10, 64)
	to, _ := strconv.ParseInt(args[2].Str, 10, 64)

	samples, err := h.cache.TSRevRange(key, from, to)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	resultArray := make([]models.Value, len(samples))
	for i, sample := range samples {
		resultArray[i] = models.Value{Type: "array", Array: []models.Value{
			{Type: "integer", Num: int(sample.Timestamp)},
			{Type: "float", Float: sample.Value},
		}}
	}

	return models.Value{Type: "array", Array: resultArray}
}

func (h *TimeSeriesHandlers) HandleTSQueryIndex(args []models.Value) models.Value {
	filters := make(map[string]string)

	// Filtreleri ayıkla
	for _, arg := range args {
		keyValue := strings.SplitN(arg.Str, "=", 2)
		if len(keyValue) == 2 {
			filters[keyValue[0]] = keyValue[1]
		}
	}

	keys, err := h.cache.TSQueryIndex(filters)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	resultArray := []models.Value{}
	for _, key := range keys {
		resultArray = append(resultArray, models.Value{Type: "string", Str: key})
	}

	return models.Value{Type: "array", Array: resultArray}
}

func (h *TimeSeriesHandlers) HandleTSDeleteRule(args []models.Value) models.Value {
	sourceKey := args[0].Str
	destinationKey := args[1].Str

	err := h.cache.TSDeleteRule(sourceKey, destinationKey)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}
