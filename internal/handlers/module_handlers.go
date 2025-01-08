package handlers

import (
	"log"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type ModuleHandlers struct {
	cache ports.Cache
}

func NewModuleHandlers(cache ports.Cache) *ModuleHandlers {
	return &ModuleHandlers{
		cache: cache,
	}
}

func (h *ModuleHandlers) HandleModule(args []models.Value) models.Value {
	if len(args) == 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'module' command"}
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	switch subCmd {
	case "LIST":
		// Return information about all available modules
		modules := []models.Value{
			createModuleInfo("ReJSON", 20000),         // JSON data type support
			createModuleInfo("Bloom", 20000),          // Bloom filter capabilities
			createModuleInfo("CuckooFilter", 20000),   // Cuckoo filter support
			createModuleInfo("CountMinSketch", 20000), // Count-Min Sketch structure
			createModuleInfo("TopK", 20000),           // Top-K statistics
			createModuleInfo("HyperLogLog", 20000),    // HyperLogLog structure
			createModuleInfo("TDigest", 20000),        // T-Digest structure
			createModuleInfo("TimeSeries", 20000),     // Time series data type
			createModuleInfo("Geo", 20000),            // Geospatial features
			createModuleInfo("Search", 20000),         // Search capabilities
			createModuleInfo("BitMap", 20000),         // Bitmap operations
			createModuleInfo("Stream", 20000),         // Stream data type
		}

		log.Printf("[DEBUG] MODULE response - LIST: %+v", models.Value{Type: "array", Array: modules})
		return models.Value{Type: "array", Array: modules}

	case "LOAD":
		log.Printf("[DEBUG] MODULE response - LOAD: %+v", models.Value{Type: "error", Str: "ERR modules not supported"})
		return models.Value{Type: "error", Str: "ERR modules not supported"}

	case "UNLOAD":
		log.Printf("[DEBUG] MODULE response - UNLOAD: %+v", models.Value{Type: "error", Str: "ERR modules not supported"})
		return models.Value{Type: "error", Str: "ERR modules not supported"}

	default:
		return models.Value{Type: "error", Str: "ERR unknown subcommand for MODULE"}
	}
}

// createModuleInfo creates a standardized module information array
func createModuleInfo(name string, version int) models.Value {
	return models.Value{
		Type: "array",
		Array: []models.Value{
			{Type: "string", Str: "name"},
			{Type: "string", Str: name},
			{Type: "string", Str: "ver"},
			{Type: "integer", Num: version},
			{Type: "string", Str: "path"},
			{Type: "string", Str: "built-in"},
		},
	}
}
