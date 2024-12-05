package handlers

import (
	"fmt"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type MemoryHandlers struct {
	cache ports.Cache
}

func NewMemoryHandlers(cache ports.Cache) *MemoryHandlers {
	return &MemoryHandlers{cache: cache}
}

func (h *MemoryHandlers) HandleMemory(args []models.Value) models.Value {
	if len(args) == 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for MEMORY command"}
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	switch subCmd {
	case "USAGE":
		if len(args) != 2 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for MEMORY USAGE"}
		}
		stats := h.cache.GetMemoryStats()
		return models.Value{Type: "integer", Num: int(stats.UsedMemory)}

	case "STATS":
		stats := h.cache.GetMemoryStats()
		return models.Value{Type: "bulk", Bulk: fmt.Sprintf(
			"total_memory:%d\nused_memory:%d\nfragmented_bytes:%d\n",
			stats.TotalMemory,
			stats.UsedMemory,
			stats.FragmentedBytes,
		)}

	case "PURGE":
		h.cache.Defragment()
		return models.Value{Type: "string", Str: "OK"}

	default:
		return models.Value{Type: "error", Str: "ERR unknown subcommand for MEMORY"}
	}
}

func (h *MemoryHandlers) HandleType(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for TYPE command"}
	}
	return models.Value{Type: "string", Str: h.cache.Type(args[0].Bulk)}
}

func (h *MemoryHandlers) HandleTTL(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for TTL command"}
	}
	return models.Value{Type: "integer", Num: h.cache.TTL(args[0].Bulk)}
}
