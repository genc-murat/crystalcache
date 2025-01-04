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

// NewBitMapHandlers creates a new instance of BitMapHandlers with the provided cache.
// It takes a ports.Cache interface as an argument and returns a pointer to BitMapHandlers.
func NewBitMapHandlers(cache ports.Cache) *BitMapHandlers {
	return &BitMapHandlers{cache: cache}
}

// HandleGetBit handles the 'getbit' command which retrieves the bit value at the specified offset
// in the string value stored at the given key.
//
// Arguments:
// - args: A slice of models.Value containing the key and the offset.
//
// Returns:
//   - models.Value: An integer value representing the bit value at the specified offset, or an error
//     if the arguments are invalid or if there is an issue retrieving the bit value.
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

// HandleSetBit sets or clears the bit at a specified offset in the string value stored at key.
// It expects three arguments: the key, the offset, and the value (0 or 1).
// If the number of arguments is incorrect, or if the offset or value are invalid, it returns an error.
// Otherwise, it sets the bit and returns the old bit value.
//
// Arguments:
// - args[0]: The key (string).
// - args[1]: The offset (integer).
// - args[2]: The value (0 or 1).
//
// Returns:
// - models.Value: The old bit value (integer) or an error message (string).
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

// HandleBitCount processes the 'bitcount' command, which counts the number of set bits (1s) in a string.
// It accepts a variable number of arguments:
// - The first argument is the key of the string to count bits in.
// - The second and third arguments (optional) specify the start and end positions (inclusive) for the bit count.
// If the start and end positions are not provided, the entire string is considered.
// Returns a models.Value containing the count of set bits or an error message if the arguments are invalid.
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

// HandleBitOp processes the 'bitop' command which performs bitwise operations
// between multiple keys and stores the result in the destination key.
//
// Args:
//
//	args ([]models.Value): A slice of Value objects where the first element is the
//	  bitwise operation (AND, OR, XOR, NOT), the second element is the destination key,
//	  and the remaining elements are the source keys.
//
// Returns:
//
//	models.Value: A Value object containing the result of the operation. If the operation
//	  is successful, it returns the length of the string stored in the destination key as
//	  an integer type. If there is an error, it returns an error type with the error message.
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

// HandleBitPos handles the 'bitpos' command which finds the first bit set to the specified value (0 or 1)
// in a string. The command accepts the following arguments:
// - args[0]: The key of the bitmap.
// - args[1]: The bit value to search for (0 or 1).
// - args[2] (optional): The start position to begin the search.
// - args[3] (optional): The end position to end the search.
// - args[4] (optional): The keyword "REV" to search in reverse order.
//
// It returns a models.Value containing the position of the first bit set to the specified value,
// or an error if the arguments are invalid or if there is an issue with the cache.
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

// HandleBitField processes the 'bitfield' command with the given arguments.
// It expects at least two arguments, where the first argument is the key and
// the remaining arguments are the bitfield commands to be executed.
//
// If the number of arguments is less than two, it returns an error value indicating
// the wrong number of arguments.
//
// It parses the bitfield commands and executes them on the cache. If there is an
// error during parsing or execution, it returns an error value with the error message.
//
// On successful execution, it returns an array of integer values representing the
// results of the bitfield commands.
//
// Args:
//
//	args ([]models.Value): The arguments for the 'bitfield' command.
//
// Returns:
//
//	models.Value: The result of the 'bitfield' command execution, which is either
//	an error value or an array of integer values.
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

// HandleBitFieldRO processes the 'bitfield_ro' command with the given arguments.
// It expects at least two arguments: the key and the bitfield commands.
// If the number of arguments is insufficient, it returns an error.
//
// The function parses the bitfield commands and executes them in read-only mode
// on the specified key in the cache. The results of the bitfield operations are
// returned as an array of integers.
//
// Args:
//
//	args ([]models.Value): The arguments for the 'bitfield_ro' command.
//
// Returns:
//
//	models.Value: The result of the 'bitfield_ro' command execution. It can be
//	an error message if the command fails or an array of integers representing
//	the results of the bitfield operations.
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

// parseBitFieldCommands parses a list of bitfield commands from the provided arguments.
// It supports the following commands: GET, SET, and INCRBY.
//
// Each command must be followed by the appropriate number of arguments:
// - GET: type, offset
// - SET: type, offset, value
// - INCRBY: type, offset, increment
//
// If the arguments are invalid or the command is unknown, an error is returned.
//
// Parameters:
// - args: A slice of models.Value representing the command arguments.
//
// Returns:
// - A slice of models.BitFieldCommand containing the parsed commands.
// - An error if the arguments are invalid or the command is unknown.
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
