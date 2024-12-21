package handlers

import (
	"fmt"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type HLLHandlers struct {
	cache ports.Cache
}

func NewHLLHandlers(cache ports.Cache) *HLLHandlers {
	return &HLLHandlers{
		cache: cache,
	}
}

// HandlePFAdd handles the PFADD command
func (h *HLLHandlers) HandlePFAdd(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'pfadd' command"}
	}

	key := args[0].Bulk
	elements := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		elements[i-1] = args[i].Bulk
	}

	modified, err := h.cache.PFAdd(key, elements...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: boolToInt(modified)}
}

// HandlePFCount handles the PFCOUNT command
func (h *HLLHandlers) HandlePFCount(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'pfcount' command"}
	}

	keys := make([]string, len(args))
	for i := range args {
		keys[i] = args[i].Bulk
	}

	count, err := h.cache.PFCount(keys...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: int(count)}
}

// HandlePFMerge handles the PFMERGE command
func (h *HLLHandlers) HandlePFMerge(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'pfmerge' command"}
	}

	destKey := args[0].Bulk
	sourceKeys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		sourceKeys[i-1] = args[i].Bulk
	}

	err := h.cache.PFMerge(destKey, sourceKeys...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandlePFDebug handles the PFDEBUG command
func (h *HLLHandlers) HandlePFDebug(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'pfdebug' command"}
	}

	key := args[0].Bulk
	info, err := h.cache.PFDebug(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	// Format debug info
	result := []models.Value{
		{Type: "bulk", Bulk: "Encoding"},
		{Type: "bulk", Bulk: info["encoding"].(string)},
		{Type: "bulk", Bulk: "Size"},
		{Type: "integer", Num: info["size"].(int)},
		{Type: "bulk", Bulk: "Register width"},
		{Type: "integer", Num: info["regwidth"].(int)},
		{Type: "bulk", Bulk: "Sparseness"},
		{Type: "bulk", Bulk: fmt.Sprintf("%.4f", info["sparseness"].(float64))},
		{Type: "bulk", Bulk: "Non-zero registers"},
		{Type: "integer", Num: info["nonZeroRegs"].(int)},
	}

	return models.Value{Type: "array", Array: result}
}

// HandlePFSelfTest handles the PFSELFTEST command
func (h *HLLHandlers) HandlePFSelfTest(args []models.Value) models.Value {
	// Run internal consistency checks
	err := h.cache.PFSelfTest()
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
