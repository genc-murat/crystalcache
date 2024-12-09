package handlers

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type BitMapHandlers struct {
	cache ports.Cache
}

func NewBitMapHandlers(cache ports.Cache) *BitMapHandlers {
	return &BitMapHandlers{cache: cache}
}

func (h *BitMapHandlers) HandleGetBit(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'getbit' command"}
	}

	offset, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
	}

	if offset < 0 {
		return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
	}

	bit, err := h.cache.GetBit(args[0].Bulk, offset)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: bit}
}

func (h *BitMapHandlers) HandleSetBit(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'setbit' command"}
	}

	offset, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
	}

	if offset < 0 {
		return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
	}

	value, err := strconv.Atoi(args[2].Bulk)
	if err != nil || (value != 0 && value != 1) {
		return models.Value{Type: "error", Str: "ERR bit is not an integer or out of range"}
	}

	oldBit, err := h.cache.SetBit(args[0].Bulk, offset, value)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: oldBit}
}

func (h *BitMapHandlers) HandleBitCount(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'bitcount' command"}
	}

	start := int64(0)
	end := int64(-1)
	if len(args) >= 3 {
		var err error
		start, err = strconv.ParseInt(args[1].Bulk, 10, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
		}
		end, err = strconv.ParseInt(args[2].Bulk, 10, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
		}
	}

	count, err := h.cache.BitCount(args[0].Bulk, start, end)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: int(count)}
}

func (h *BitMapHandlers) HandleBitOp(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'bitop' command"}
	}

	operation := strings.ToUpper(args[0].Bulk)
	destkey := args[1].Bulk

	sourceKeys := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		sourceKeys[i-2] = args[i].Bulk
	}

	length, err := h.cache.BitOp(operation, destkey, sourceKeys...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: int(length)}
}

func (h *BitMapHandlers) HandleBitPos(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'bitpos' command"}
	}

	bit, err := strconv.Atoi(args[1].Bulk)
	if err != nil || (bit != 0 && bit != 1) {
		return models.Value{Type: "error", Str: "ERR bit must be 0 or 1"}
	}

	start := int64(0)
	end := int64(-1)
	if len(args) >= 4 {
		start, err = strconv.ParseInt(args[2].Bulk, 10, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
		}
		end, err = strconv.ParseInt(args[3].Bulk, 10, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR bit offset is not an integer or out of range"}
		}
	}

	reverse := len(args) >= 5 && strings.ToUpper(args[4].Bulk) == "REV"

	pos, err := h.cache.BitPos(args[0].Bulk, bit, start, end, reverse)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: int(pos)}
}

func (h *BitMapHandlers) HandleBitField(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'bitfield' command"}
	}

	commands, err := h.parseBitFieldCommands(args[1:])
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	results, err := h.cache.BitField(args[0].Bulk, commands)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, result := range results {
		response[i] = models.Value{Type: "integer", Num: int(result)}
	}

	return models.Value{Type: "array", Array: response}
}

func (h *BitMapHandlers) HandleBitFieldRO(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'bitfield_ro' command"}
	}

	commands, err := h.parseBitFieldCommands(args[1:])
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	results, err := h.cache.BitFieldRO(args[0].Bulk, commands)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	response := make([]models.Value, len(results))
	for i, result := range results {
		response[i] = models.Value{Type: "integer", Num: int(result)}
	}

	return models.Value{Type: "array", Array: response}
}

func (h *BitMapHandlers) parseBitFieldCommands(args []models.Value) ([]models.BitFieldCommand, error) {
	var commands []models.BitFieldCommand
	i := 0
	for i < len(args) {
		if i+2 >= len(args) {
			return nil, fmt.Errorf("ERR wrong number of arguments for BITFIELD")
		}

		cmd := models.BitFieldCommand{Op: strings.ToUpper(args[i].Bulk)}
		switch cmd.Op {
		case "GET":
			cmd.Type = args[i+1].Bulk
			offset, err := strconv.ParseInt(args[i+2].Bulk, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid offset for GET")
			}
			cmd.Offset = offset
			i += 3
		case "SET":
			if i+3 >= len(args) {
				return nil, fmt.Errorf("ERR wrong number of arguments for SET")
			}
			cmd.Type = args[i+1].Bulk
			offset, err := strconv.ParseInt(args[i+2].Bulk, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid offset for SET")
			}
			cmd.Offset = offset
			value, err := strconv.ParseInt(args[i+3].Bulk, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid value for SET")
			}
			cmd.Value = value
			i += 4
		case "INCRBY":
			if i+3 >= len(args) {
				return nil, fmt.Errorf("ERR wrong number of arguments for INCRBY")
			}
			cmd.Type = args[i+1].Bulk
			offset, err := strconv.ParseInt(args[i+2].Bulk, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid offset for INCRBY")
			}
			cmd.Offset = offset
			increment, err := strconv.ParseInt(args[i+3].Bulk, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid increment for INCRBY")
			}
			cmd.Increment = increment
			i += 4
		default:
			return nil, fmt.Errorf("ERR unknown bitfield command '%s'", cmd.Op)
		}
		commands = append(commands, cmd)
	}
	return commands, nil
}
