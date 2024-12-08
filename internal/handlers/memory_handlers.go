package handlers

import (
	"fmt"
	"runtime"
	"strings"
	"time"

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

// HandleMemory handles various memory-related commands including usage, stats, purge operations,
// diagnostics and memory allocator statistics.
//
// Supports subcommands:
//   - USAGE: Returns memory usage for a specific key
//   - STATS: Returns general memory statistics
//   - PURGE: Triggers memory defragmentation
//   - DOCTOR: Provides memory health analysis and recommendations
//   - MALLOC-STATS: Returns detailed memory allocator statistics
//
// Parameters:
//   - args: Array of Values containing the subcommand and its arguments
//
// Returns:
//   - models.Value: Response depends on subcommand:
//     USAGE: Integer value of memory used
//     STATS: String containing memory statistics
//     PURGE: "OK" on successful defragmentation
//     DOCTOR: String containing memory health analysis and recommendations
//     MALLOC-STATS: String containing detailed memory allocator statistics
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

	case "DOCTOR":
		stats := h.cache.GetMemoryStats()
		fragPercent := float64(stats.FragmentedBytes) / float64(stats.TotalMemory) * 100

		var diagnosis strings.Builder
		diagnosis.WriteString("Memory Doctor Analysis\n")
		diagnosis.WriteString("--------------------\n")

		// Check fragmentation
		if fragPercent > 50 {
			diagnosis.WriteString("[WARNING] High fragmentation detected: %.2f%%\n")
			diagnosis.WriteString("Recommendation: Consider running MEMORY PURGE\n")
		} else {
			diagnosis.WriteString("[OK] Memory fragmentation is normal: %.2f%%\n")
		}

		// Check memory usage
		memUsagePercent := float64(stats.UsedMemory) / float64(stats.TotalMemory) * 100
		if memUsagePercent > 90 {
			diagnosis.WriteString("[WARNING] High memory usage: %.2f%%\n")
			diagnosis.WriteString("Recommendation: Consider increasing maximum memory or removing unused keys\n")
		} else {
			diagnosis.WriteString("[OK] Memory usage is normal: %.2f%%\n")
		}

		// Check last defrag time
		if time.Since(stats.LastDefrag) > 24*time.Hour {
			diagnosis.WriteString("[WARNING] No recent defragmentation\n")
			diagnosis.WriteString("Recommendation: Consider running periodic defragmentation\n")
		} else {
			diagnosis.WriteString("[OK] Recent defragmentation performed\n")
		}

		return models.Value{Type: "bulk", Bulk: diagnosis.String()}

	case "MALLOC-STATS":
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)

		var report strings.Builder
		report.WriteString("Memory Allocator Statistics\n")
		report.WriteString("-------------------------\n")
		report.WriteString(fmt.Sprintf("Allocated Memory: %d bytes\n", stats.Alloc))
		report.WriteString(fmt.Sprintf("Total Memory Allocated: %d bytes\n", stats.TotalAlloc))
		report.WriteString(fmt.Sprintf("System Memory: %d bytes\n", stats.Sys))
		report.WriteString(fmt.Sprintf("Heap Objects: %d\n", stats.HeapObjects))
		report.WriteString(fmt.Sprintf("GC Cycles: %d\n", stats.NumGC))
		report.WriteString(fmt.Sprintf("Force GC Cycles: %d\n", stats.NumForcedGC))
		report.WriteString(fmt.Sprintf("Next GC Target: %d bytes\n", stats.NextGC))
		report.WriteString(fmt.Sprintf("Heap Memory: %d bytes\n", stats.HeapAlloc))
		report.WriteString(fmt.Sprintf("Stack Memory: %d bytes\n", stats.StackInuse))
		report.WriteString(fmt.Sprintf("MSpan Memory: %d bytes\n", stats.MSpanInuse))
		report.WriteString(fmt.Sprintf("MCentral Memory: %d bytes\n", stats.MCacheInuse))

		return models.Value{Type: "bulk", Bulk: report.String()}

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
