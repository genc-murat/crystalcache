package handlers

import (
	"log"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

// ModuleHandlers implements handlers for module-related operations in the cache
// Currently supports listing built-in modules and handles (with error) loading/unloading attempts
type ModuleHandlers struct {
	cache ports.Cache
}

// NewModuleHandlers creates a new instance of ModuleHandlers
// Parameters:
//   - cache: The cache implementation to be used for module operations
//
// Returns:
//   - *ModuleHandlers: A pointer to the newly created ModuleHandlers instance
func NewModuleHandlers(cache ports.Cache) *ModuleHandlers {
	return &ModuleHandlers{
		cache: cache,
	}
}

// HandleModule handles various module-related commands
// Supports subcommands:
//   - LIST: Returns information about built-in modules (currently only ReJSON)
//   - LOAD: Returns error as module loading is not supported
//   - UNLOAD: Returns error as module unloading is not supported
//
// Parameters:
//   - args: Array of Values containing the subcommand and its arguments
//
// Returns:
//   - models.Value: Response depends on subcommand:
//     LIST: Array containing module information in format:
//     [name, ReJSON, ver, 20000, path, built-in]
//     LOAD/UNLOAD: Returns error as these operations are not supported
//     Returns error for unknown subcommands or invalid arguments
func (h *ModuleHandlers) HandleModule(args []models.Value) models.Value {
	if len(args) == 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'module' command"}
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	switch subCmd {
	case "LIST":
		// Return JSON module info
		jsonModule := []models.Value{
			{Type: "array", Array: []models.Value{
				{Type: "string", Str: "name"},
				{Type: "string", Str: "ReJSON"},
				{Type: "string", Str: "ver"},
				{Type: "integer", Num: 20000}, // Version 2.0.0
				{Type: "string", Str: "path"},
				{Type: "string", Str: "built-in"},
			}},
		}
		log.Printf("[DEBUG] MODULE response - LIST: %+v", models.Value{Type: "array", Array: jsonModule})
		return models.Value{Type: "array", Array: jsonModule}

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
