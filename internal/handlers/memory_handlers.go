package handlers

import (
	"fmt"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

// MemoryHandlers implements handlers for memory-related operations in the cache
type MemoryHandlers struct {
	cache ports.Cache
}

// NewMemoryHandlers creates a new instance of MemoryHandlers
// Parameters:
//   - cache: The cache implementation to be used for memory operations
//
// Returns:
//   - *MemoryHandlers: A pointer to the newly created MemoryHandlers instance
func NewMemoryHandlers(cache ports.Cache) *MemoryHandlers {
	return &MemoryHandlers{cache: cache}
}

// HandleMemory handles various memory-related commands including usage, stats, and purge operations
// Supports subcommands:
//   - USAGE: Returns memory usage for a specific key
//   - STATS: Returns general memory statistics
//   - PURGE: Triggers memory defragmentation
//
// Parameters:
//   - args: Array of Values containing the subcommand and its arguments
//
// Returns:
//   - models.Value: Response depends on subcommand:
//     USAGE: Integer value of memory used
//     STATS: String containing memory statistics
//     PURGE: "OK" on successful defragmentation
//     Returns error for unknown subcommands or invalid arguments
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

// HandleType handles the TYPE command which returns the data type of a key
// Parameters:
//   - args: Array of Values containing the key to check
//
// Returns:
//   - models.Value: String value indicating the type of the key
//     (e.g., "string", "list", "hash", etc.)
//     Returns error if wrong number of arguments
func (h *MemoryHandlers) HandleType(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for TYPE command"}
	}
	return models.Value{Type: "string", Str: h.cache.Type(args[0].Bulk)}
}

// HandleTTL handles the TTL command which returns the remaining time to live for a key
// Parameters:
//   - args: Array of Values containing the key to check
//
// Returns:
//   - models.Value: Integer value representing:
//     Positive number: Remaining TTL in seconds
//     -1: Key exists but has no TTL (persistent)
//     -2: Key does not exist
//     Returns error if wrong number of arguments
func (h *MemoryHandlers) HandleTTL(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for TTL command"}
	}
	return models.Value{Type: "integer", Num: h.cache.TTL(args[0].Bulk)}
}
