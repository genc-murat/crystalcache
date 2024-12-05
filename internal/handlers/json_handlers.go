package handlers

import (
	"encoding/json"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type JSONHandlers struct {
	cache ports.Cache
}

func NewJSONHandlers(cache ports.Cache) *JSONHandlers {
	return &JSONHandlers{cache: cache}
}

func (h *JSONHandlers) HandleJSON(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for JSON command"}
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	switch subCmd {
	case "SET":
		return h.handleJSONSet(args[1:])
	case "GET":
		return h.handleJSONGet(args[1:])
	case "DEL":
		return h.handleJSONDel(args[1:])
	case "TYPE":
		return h.handleJSONType(args[1:])
	default:
		return models.Value{Type: "error", Str: "ERR unknown JSON subcommand '" + subCmd + "'"}
	}
}

func (h *JSONHandlers) handleJSONSet(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'JSON.SET' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk

	// Validate JSON
	if !json.Valid([]byte(value)) {
		return models.Value{Type: "error", Str: "ERR invalid JSON string"}
	}

	err := h.cache.Set(key, value)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *JSONHandlers) handleJSONGet(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'JSON.GET' command"}
	}

	key := args[0].Bulk
	value, exists := h.cache.Get(key)
	if !exists {
		return models.Value{Type: "null"}
	}

	// Validate stored value is JSON
	if !json.Valid([]byte(value)) {
		return models.Value{Type: "error", Str: "ERR key contains invalid JSON"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

func (h *JSONHandlers) handleJSONDel(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'JSON.DEL' command"}
	}

	key := args[0].Bulk
	deleted, err := h.cache.Del(key)
	if err != nil {
		return util.ToValue(err)
	}

	if deleted {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *JSONHandlers) handleJSONType(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'JSON.TYPE' command"}
	}

	key := args[0].Bulk
	value, exists := h.cache.Get(key)
	if !exists {
		return models.Value{Type: "null"}
	}

	var js interface{}
	if err := json.Unmarshal([]byte(value), &js); err != nil {
		return models.Value{Type: "error", Str: "ERR key contains invalid JSON"}
	}

	switch js.(type) {
	case map[string]interface{}:
		return models.Value{Type: "string", Str: "object"}
	case []interface{}:
		return models.Value{Type: "string", Str: "array"}
	case string:
		return models.Value{Type: "string", Str: "string"}
	case float64:
		return models.Value{Type: "string", Str: "number"}
	case bool:
		return models.Value{Type: "string", Str: "boolean"}
	case nil:
		return models.Value{Type: "string", Str: "null"}
	default:
		return models.Value{Type: "string", Str: "unknown"}
	}
}
