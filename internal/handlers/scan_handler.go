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

	cursor, err := strconv.Atoi(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid cursor"}
	}

	pattern := "*"
	count := 10

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}

		switch strings.ToUpper(args[i].Bulk) {
		case "MATCH":
			pattern = args[i+1].Bulk
		case "COUNT":
			count, err = strconv.Atoi(args[i+1].Bulk)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR invalid COUNT value"}
			}
		default:
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
	}

	keys, nextCursor := h.cache.Scan(cursor, pattern, count)

	keyValues := make([]models.Value, len(keys))
	for i, key := range keys {
		keyValues[i] = models.Value{Type: "bulk", Bulk: key}
	}

	return models.Value{
		Type: "array",
		Array: []models.Value{
			{Type: "bulk", Bulk: strconv.Itoa(nextCursor)},
			{Type: "array", Array: keyValues},
		},
	}
}
