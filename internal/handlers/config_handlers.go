package handlers

import (
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type ConfigHandlers struct {
	cache ports.Cache
}

func NewConfigHandlers(cache ports.Cache) *ConfigHandlers {
	return &ConfigHandlers{
		cache: cache,
	}
}

func (h *ConfigHandlers) HandleConfig(args []models.Value) models.Value {
	if len(args) == 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for CONFIG command"}
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	switch subCmd {
	case "GET":
		if len(args) != 2 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for CONFIG GET"}
		}
		return h.handleConfigGet(args[1].Bulk)
	case "SET":
		if len(args) != 3 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for CONFIG SET"}
		}
		return h.handleConfigSet(args[1].Bulk, args[2].Bulk)
	case "RESETSTAT":
		return models.Value{Type: "string", Str: "OK"}
	default:
		return models.Value{Type: "error", Str: "ERR unknown subcommand for CONFIG"}
	}
}

func (h *ConfigHandlers) handleConfigGet(parameter string) models.Value {
	configs := map[string]string{
		"maxmemory":  "0",
		"maxclients": "10000",
		"databases":  "16",
	}

	if parameter == "*" {
		result := make([]models.Value, 0, len(configs)*2)
		for k, v := range configs {
			result = append(result, models.Value{Type: "string", Str: k})
			result = append(result, models.Value{Type: "string", Str: v})
		}
		return models.Value{Type: "array", Array: result}
	}

	if val, ok := configs[parameter]; ok {
		return models.Value{Type: "array", Array: []models.Value{
			{Type: "string", Str: parameter},
			{Type: "string", Str: val},
		}}
	}

	return models.Value{Type: "array", Array: []models.Value{}}
}

func (h *ConfigHandlers) handleConfigSet(parameter, value string) models.Value {
	return models.Value{Type: "string", Str: "OK"}
}
