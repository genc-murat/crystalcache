package handlers

import (
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type ScanHandlers struct {
	cache ports.Cache
}

func NewScanHandlers(cache ports.Cache) *ScanHandlers {
	return &ScanHandlers{cache: cache}
}

func (h *ScanHandlers) HandleScan(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for SCAN"}
	}

	cursor := 0
	if len(args) > 0 {
		var err error
		cursor, err = strconv.Atoi(args[0].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid cursor"}
		}
	}

	pattern := "*"
	if len(args) >= 3 && strings.ToUpper(args[1].Bulk) == "MATCH" {
		pattern = args[2].Bulk
	}

	count := 10
	if len(args) >= 5 && strings.ToUpper(args[3].Bulk) == "COUNT" {
		var err error
		count, err = strconv.Atoi(args[4].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid COUNT value"}
		}
	}

	keys, nextCursor := h.cache.Scan(cursor, pattern, count)

	keyValues := make([]models.Value, len(keys))
	for i, key := range keys {
		keyValues[i] = models.Value{Type: "string", Str: key}
	}

	return models.Value{
		Type: "array",
		Array: []models.Value{
			{Type: "string", Str: strconv.Itoa(nextCursor)},
			{Type: "array", Array: keyValues},
		},
	}
}
