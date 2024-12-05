package handlers

import (
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type HashHandlers struct {
	cache ports.Cache
}

func NewHashHandlers(cache ports.Cache) *HashHandlers {
	return &HashHandlers{cache: cache}
}

func (h *HashHandlers) HandleHSet(args []models.Value) models.Value {
	if len(args) < 3 || len(args)%2 != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}

	hashKey := args[0].Bulk
	fieldsAdded := 0

	for i := 1; i < len(args); i += 2 {
		err := h.cache.HSet(hashKey, args[i].Bulk, args[i+1].Bulk)
		if err != nil {
			return util.ToValue(err)
		}
		fieldsAdded++
	}

	return models.Value{Type: "integer", Num: fieldsAdded}
}

func (h *HashHandlers) HandleHGet(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.HGet(args[0].Bulk, args[1].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

func (h *HashHandlers) HandleHGetAll(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	pairs := h.cache.HGetAll(args[0].Bulk)
	result := make([]models.Value, 0, len(pairs)*2)

	for key, value := range pairs {
		result = append(result,
			models.Value{Type: "bulk", Bulk: key},
			models.Value{Type: "bulk", Bulk: value},
		)
	}

	return models.Value{Type: "array", Array: result}
}

func (h *HashHandlers) HandleHLen(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	pairs := h.cache.HGetAll(args[0].Bulk)
	return models.Value{Type: "integer", Num: len(pairs)}
}

func (h *HashHandlers) HandleHScan(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for HSCAN"}
	}

	key := args[0].Bulk
	cursor, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid cursor"}
	}

	pattern := "*"
	count := 10

	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}

		switch strings.ToUpper(args[i].Bulk) {
		case "MATCH":
			pattern = args[i+1].Bulk
		case "COUNT":
			count, err = strconv.Atoi(args[i+1].Bulk)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR invalid COUNT"}
			}
		default:
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
	}

	hashMap := h.cache.HGetAll(key)
	if len(hashMap) == 0 {
		return models.Value{Type: "array", Array: []models.Value{
			{Type: "string", Str: "0"},
			{Type: "array", Array: []models.Value{}},
		}}
	}

	// Convert map to sorted slice for consistent iteration
	fields := make([]string, 0, len(hashMap))
	for field := range hashMap {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	if cursor >= len(fields) {
		return models.Value{Type: "array", Array: []models.Value{
			{Type: "string", Str: "0"},
			{Type: "array", Array: []models.Value{}},
		}}
	}

	resultArray := make([]models.Value, 0, count*2)
	nextCursor := cursor

	for i := cursor; i < len(fields) && len(resultArray) < count*2; i++ {
		field := fields[i]
		if matchPattern(pattern, field) {
			resultArray = append(resultArray,
				models.Value{Type: "string", Str: field},
				models.Value{Type: "string", Str: hashMap[field]},
			)
		}
		nextCursor = i + 1
	}

	if nextCursor >= len(fields) {
		nextCursor = 0
	}

	log.Printf("[DEBUG] HSCAN hash=%s cursor=%d nextCursor=%d matches=%d",
		key, cursor, nextCursor, len(resultArray)/2)

	return models.Value{Type: "array", Array: []models.Value{
		{Type: "string", Str: strconv.Itoa(nextCursor)},
		{Type: "array", Array: resultArray},
	}}
}

func (h *HashHandlers) HandleHDel(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for HDEL"}
	}

	key := args[0].Bulk
	deleted := 0

	for i := 1; i < len(args); i++ {
		if exists, err := h.cache.HDel(key, args[i].Bulk); err == nil && exists {
			deleted++
		}
	}

	return models.Value{Type: "integer", Num: deleted}
}
