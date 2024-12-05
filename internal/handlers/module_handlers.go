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
		log.Printf("[DEBUG] MODULE response - LIST: %+v", models.Value{Type: "array", Array: []models.Value{}})
		return models.Value{Type: "array", Array: []models.Value{}}
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
