package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type StreamHandlers struct {
	cache ports.Cache
}

func NewStreamHandlers(cache ports.Cache) *StreamHandlers {
	return &StreamHandlers{cache: cache}
}

func (h *StreamHandlers) HandleXAdd(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xadd' command"}
	}

	key := args[0].Bulk
	id := args[1].Bulk

	// Check if fields come in pairs
	if (len(args)-2)%2 != 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for XADD"}
	}

	// Create fields map
	fields := make(map[string]string)
	for i := 2; i < len(args); i += 2 {
		fields[args[i].Bulk] = args[i+1].Bulk
	}

	// Generate ID if "*"
	if id == "*" {
		id = generateStreamID()
	}

	// Add to stream
	err := h.cache.XAdd(key, id, fields)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: id}
}

func (h *StreamHandlers) HandleXACK(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xack' command"}
	}

	key := args[0].Bulk
	group := args[1].Bulk

	ids := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		ids[i-2] = args[i].Bulk
	}

	count, err := h.cache.XACK(key, group, ids...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: int(count)}
}

func (h *StreamHandlers) HandleXDEL(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xdel' command"}
	}

	key := args[0].Bulk
	ids := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		ids[i-1] = args[i].Bulk
	}

	count, err := h.cache.XDEL(key, ids...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: int(count)}
}

func (h *StreamHandlers) HandleXAutoClaim(args []models.Value) models.Value {
	if len(args) < 5 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xautoclaim' command"}
	}

	key := args[0].Bulk
	group := args[1].Bulk
	consumer := args[2].Bulk
	minIdleTime, err := strconv.ParseInt(args[3].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid min-idle-time"}
	}
	start := args[4].Bulk

	count := 100
	if len(args) > 5 {
		count, err = strconv.Atoi(args[5].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid count"}
		}
	}

	_, entries, cursor, err := h.cache.XAutoClaim(key, group, consumer, minIdleTime, start, count)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, len(entries))
	for i, entry := range entries {
		fields := make([]models.Value, 0, len(entry.Fields)*2)
		for k, v := range entry.Fields {
			fields = append(fields, models.Value{Type: "bulk", Bulk: k})
			fields = append(fields, models.Value{Type: "bulk", Bulk: v})
		}
		result[i] = models.Value{Type: "array", Array: []models.Value{
			{Type: "bulk", Bulk: entry.ID},
			{Type: "array", Array: fields},
		}}
	}

	return models.Value{Type: "array", Array: []models.Value{
		{Type: "bulk", Bulk: cursor},
		{Type: "array", Array: result},
	}}
}

func (h *StreamHandlers) HandleXClaim(args []models.Value) models.Value {
	if len(args) < 6 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xclaim' command"}
	}

	key := args[0].Bulk
	group := args[1].Bulk
	consumer := args[2].Bulk
	minIdleTime, err := strconv.ParseInt(args[3].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid min-idle-time"}
	}

	ids := make([]string, len(args)-4)
	for i := 4; i < len(args); i++ {
		ids[i-4] = args[i].Bulk
	}

	entries, err := h.cache.XClaim(key, group, consumer, minIdleTime, ids...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, len(entries))
	for i, entry := range entries {
		fields := make([]models.Value, 0, len(entry.Fields)*2)
		for k, v := range entry.Fields {
			fields = append(fields, models.Value{Type: "bulk", Bulk: k})
			fields = append(fields, models.Value{Type: "bulk", Bulk: v})
		}
		result[i] = models.Value{Type: "array", Array: []models.Value{
			{Type: "bulk", Bulk: entry.ID},
			{Type: "array", Array: fields},
		}}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *StreamHandlers) HandleXLEN(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xlen' command"}
	}

	count := h.cache.XLEN(args[0].Bulk)
	return models.Value{Type: "integer", Num: int(count)}
}

func (h *StreamHandlers) HandleXPENDING(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xpending' command"}
	}

	count, err := h.cache.XPENDING(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: int(count)}
}

func (h *StreamHandlers) HandleXRANGE(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xrange' command"}
	}

	count := 0
	if len(args) >= 4 {
		var err error
		count, err = strconv.Atoi(args[3].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid COUNT"}
		}
	}

	entries, err := h.cache.XRANGE(args[0].Bulk, args[1].Bulk, args[2].Bulk, count)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, len(entries))
	for i, entry := range entries {
		fields := make([]models.Value, 0, len(entry.Fields)*2)
		for k, v := range entry.Fields {
			fields = append(fields, models.Value{Type: "bulk", Bulk: k})
			fields = append(fields, models.Value{Type: "bulk", Bulk: v})
		}
		result[i] = models.Value{Type: "array", Array: []models.Value{
			{Type: "bulk", Bulk: entry.ID},
			{Type: "array", Array: fields},
		}}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *StreamHandlers) HandleXREAD(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xread' command"}
	}

	count := 0
	argIndex := 0

	if strings.ToUpper(args[0].Bulk) == "COUNT" {
		if len(args) < 5 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xread' command"}
		}
		var err error
		count, err = strconv.Atoi(args[1].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid COUNT"}
		}
		argIndex = 2
	}

	numKeys := (len(args) - argIndex) / 2
	keys := make([]string, numKeys)
	ids := make([]string, numKeys)

	for i := 0; i < numKeys; i++ {
		keys[i] = args[argIndex+i].Bulk
		ids[i] = args[argIndex+numKeys+i].Bulk
	}

	entries, err := h.cache.XREAD(keys, ids, count)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, 0, len(entries))
	for key, keyEntries := range entries {
		entryValues := make([]models.Value, len(keyEntries))
		for i, entry := range keyEntries {
			fields := make([]models.Value, 0, len(entry.Fields)*2)
			for k, v := range entry.Fields {
				fields = append(fields, models.Value{Type: "bulk", Bulk: k})
				fields = append(fields, models.Value{Type: "bulk", Bulk: v})
			}
			entryValues[i] = models.Value{Type: "array", Array: []models.Value{
				{Type: "bulk", Bulk: entry.ID},
				{Type: "array", Array: fields},
			}}
		}
		result = append(result, models.Value{Type: "array", Array: []models.Value{
			{Type: "bulk", Bulk: key},
			{Type: "array", Array: entryValues},
		}})
	}

	return models.Value{Type: "array", Array: result}
}

func (h *StreamHandlers) HandleXREVRANGE(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xrevrange' command"}
	}

	count := 0
	if len(args) >= 5 && strings.ToUpper(args[3].Bulk) == "COUNT" {
		var err error
		count, err = strconv.Atoi(args[4].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR invalid COUNT"}
		}
	}

	entries, err := h.cache.XREVRANGE(args[0].Bulk, args[1].Bulk, args[2].Bulk, count)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, len(entries))
	for i, entry := range entries {
		fields := make([]models.Value, 0, len(entry.Fields)*2)
		for k, v := range entry.Fields {
			fields = append(fields, models.Value{Type: "bulk", Bulk: k})
			fields = append(fields, models.Value{Type: "bulk", Bulk: v})
		}
		result[i] = models.Value{Type: "array", Array: []models.Value{
			{Type: "bulk", Bulk: entry.ID},
			{Type: "array", Array: fields},
		}}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *StreamHandlers) HandleXSETID(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xsetid' command"}
	}

	err := h.cache.XSETID(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *StreamHandlers) HandleXTRIM(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xtrim' command"}
	}

	threshold, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid threshold"}
	}

	count, err := h.cache.XTRIM(args[0].Bulk, args[1].Bulk, threshold)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: int(count)}
}

func (h *StreamHandlers) HandleXInfoGroups(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xinfo groups' command"}
	}

	groups, err := h.cache.XInfoGroups(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, len(groups))
	for i, group := range groups {
		result[i] = models.Value{Type: "array", Array: []models.Value{
			{Type: "bulk", Bulk: "name"}, {Type: "bulk", Bulk: group.Name},
			{Type: "bulk", Bulk: "consumers"}, {Type: "integer", Num: int(group.Consumers)},
			{Type: "bulk", Bulk: "pending"}, {Type: "integer", Num: int(group.Pending)},
			{Type: "bulk", Bulk: "last-delivered-id"}, {Type: "bulk", Bulk: group.LastDeliveredID},
		}}
	}
	return models.Value{Type: "array", Array: result}
}

func (h *StreamHandlers) HandleXInfoConsumers(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xinfo consumers' command"}
	}

	consumers, err := h.cache.XInfoConsumers(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, len(consumers))
	for i, consumer := range consumers {
		result[i] = models.Value{Type: "array", Array: []models.Value{
			{Type: "bulk", Bulk: "name"}, {Type: "bulk", Bulk: consumer.Name},
			{Type: "bulk", Bulk: "pending"}, {Type: "integer", Num: int(consumer.Pending)},
			{Type: "bulk", Bulk: "idle"}, {Type: "integer", Num: int(consumer.IdleTime)},
		}}
	}
	return models.Value{Type: "array", Array: result}
}

func (h *StreamHandlers) HandleXInfoStream(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xinfo stream' command"}
	}

	info, err := h.cache.XInfoStream(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "array", Array: []models.Value{
		{Type: "bulk", Bulk: "length"}, {Type: "integer", Num: int(info.Length)},
		{Type: "bulk", Bulk: "radix-tree-keys"}, {Type: "integer", Num: int(info.RadixTreeKeys)},
		{Type: "bulk", Bulk: "radix-tree-nodes"}, {Type: "integer", Num: int(info.RadixTreeNodes)},
		{Type: "bulk", Bulk: "groups"}, {Type: "integer", Num: int(info.Groups)},
		{Type: "bulk", Bulk: "last-generated-id"}, {Type: "bulk", Bulk: info.LastGeneratedID},
	}}
}

func (h *StreamHandlers) HandleXInfo(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xinfo' command"}
	}

	subcommand := strings.ToUpper(args[0].Bulk)
	switch subcommand {
	case "GROUPS":
		if len(args) != 2 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xinfo groups' command"}
		}
		return h.HandleXInfoGroups(args[1:])

	case "CONSUMERS":
		if len(args) != 3 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xinfo consumers' command"}
		}
		return h.HandleXInfoConsumers(args[1:])

	case "STREAM":
		if len(args) != 2 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xinfo stream' command"}
		}
		return h.HandleXInfoStream(args[1:])

	default:
		return models.Value{Type: "error", Str: "ERR unknown subcommand '" + subcommand + "'"}
	}
}

func (h *StreamHandlers) HandleXGroup(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xgroup' command"}
	}

	subcommand := strings.ToUpper(args[0].Bulk)
	switch subcommand {
	case "CREATE":
		if len(args) != 4 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xgroup create' command"}
		}
		err := h.cache.XGroupCreate(args[1].Bulk, args[2].Bulk, args[3].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
		return models.Value{Type: "string", Str: "OK"}

	case "CREATECONSUMER":
		if len(args) != 4 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xgroup createconsumer' command"}
		}
		created, err := h.cache.XGroupCreateConsumer(args[1].Bulk, args[2].Bulk, args[3].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
		return models.Value{Type: "integer", Num: int(created)}

	case "DELCONSUMER":
		if len(args) != 4 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xgroup delconsumer' command"}
		}
		deleted, err := h.cache.XGroupDelConsumer(args[1].Bulk, args[2].Bulk, args[3].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
		return models.Value{Type: "integer", Num: int(deleted)}

	case "DESTROY":
		if len(args) != 3 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xgroup destroy' command"}
		}
		destroyed, err := h.cache.XGroupDestroy(args[1].Bulk, args[2].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
		return models.Value{Type: "integer", Num: int(destroyed)}

	case "SETID":
		if len(args) != 4 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'xgroup setid' command"}
		}
		err := h.cache.XGroupSetID(args[1].Bulk, args[2].Bulk, args[3].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: err.Error()}
		}
		return models.Value{Type: "string", Str: "OK"}

	default:
		return models.Value{Type: "error", Str: "ERR unknown subcommand '" + subcommand + "'"}
	}
}

func generateStreamID() string {
	timestamp := time.Now().UnixMilli()
	sequence := 0 // You might want to implement a sequence counter
	return fmt.Sprintf("%d-%d", timestamp, sequence)
}
