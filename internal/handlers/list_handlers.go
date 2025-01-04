package handlers

import (
	"strconv"
	"strings"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

// ListHandlers implements handlers for list operations in the cache
type ListHandlers struct {
	cache ports.Cache
}

// NewListHandlers creates a new instance of ListHandlers
// Parameters:
//   - cache: The cache implementation to be used for list operations
//
// Returns:
//   - *ListHandlers: A pointer to the newly created ListHandlers instance
func NewListHandlers(cache ports.Cache) *ListHandlers {
	return &ListHandlers{cache: cache}
}

// HandleLPush handles the LPUSH command which inserts elements at the head of the list
// Parameters:
//   - args: Array of Values containing the key followed by one or more values to push
//
// Returns:
//   - models.Value: The length of the list after the push operation
//     Returns error if wrong number of arguments or operation fails
func (h *ListHandlers) HandleLPush(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'lpush' command"}
	}

	key := args[0].Bulk
	totalLen := 0
	var err error

	// Handle multiple values
	for i := 1; i < len(args); i++ {
		totalLen, err = h.cache.LPush(key, args[i].Bulk)
		if err != nil {
			return util.ToValue(err)
		}
	}

	return models.Value{Type: "integer", Num: totalLen}
}

// HandleRPush handles the RPUSH command which inserts elements at the tail of the list
// Parameters:
//   - args: Array of Values containing the key followed by one or more values to push
//
// Returns:
//   - models.Value: The length of the list after the push operation
//     Returns error if wrong number of arguments or operation fails
func (h *ListHandlers) HandleRPush(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := args[0].Bulk
	totalLen := 0
	var err error

	// Handle multiple values
	for i := 1; i < len(args); i++ {
		totalLen, err = h.cache.RPush(key, args[i].Bulk)
		if err != nil {
			return util.ToValue(err)
		}
	}

	return models.Value{Type: "integer", Num: totalLen}
}

// HandleLRange handles the LRANGE command which returns a range of elements from the list
// Parameters:
//   - args: Array of Values containing the key, start index, and stop index
//
// Returns:
//   - models.Value: Array of elements in the specified range
//     Returns error if wrong number of arguments or invalid indices
func (h *ListHandlers) HandleLRange(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	start, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	stop, err := util.ParseInt(args[2])
	if err != nil {
		return util.ToValue(err)
	}

	values, err := h.cache.LRange(args[0].Bulk, start, stop)
	if err != nil {
		return util.ToValue(err)
	}

	result := make([]models.Value, len(values))
	for i, value := range values {
		result[i] = models.Value{Type: "bulk", Bulk: value}
	}

	return models.Value{Type: "array", Array: result}
}

// HandleLPop handles the LPOP command which removes and returns an element from the head of the list
// Parameters:
//   - args: Array of Values containing the key
//
// Returns:
//   - models.Value: The popped element, or null if the list is empty
//     Returns error if wrong number of arguments
func (h *ListHandlers) HandleLPop(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.LPop(args[0].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

// HandleRPop handles the RPOP command which removes and returns an element from the tail of the list
// Parameters:
//   - args: Array of Values containing the key
//
// Returns:
//   - models.Value: The popped element, or null if the list is empty
//     Returns error if wrong number of arguments
func (h *ListHandlers) HandleRPop(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.RPop(args[0].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

// HandleLLen handles the LLEN command which returns the length of the list
// Parameters:
//   - args: Array of Values containing the key
//
// Returns:
//   - models.Value: The length of the list as an integer
//     Returns error if wrong number of arguments
func (h *ListHandlers) HandleLLen(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	length := h.cache.LLen(args[0].Bulk)
	return models.Value{Type: "integer", Num: length}
}

// HandleLSet handles the LSET command which sets the value of an element at a specific index
// Parameters:
//   - args: Array of Values containing the key, index, and new value
//
// Returns:
//   - models.Value: "OK" if successful
//     Returns error if wrong number of arguments, invalid index, or operation fails
func (h *ListHandlers) HandleLSet(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	index, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	err = h.cache.LSet(args[0].Bulk, index, args[2].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

// HandleLRem handles the LREM command which removes elements from the list
// Parameters:
//   - args: Array of Values containing:
//   - key: The list key
//   - count: Number of occurrences to remove (>0 from head, <0 from tail, 0 all)
//   - value: The value to remove
//
// Returns:
//   - models.Value: The number of elements removed
//     Returns error if wrong number of arguments or invalid count
func (h *ListHandlers) HandleLRem(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	count, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	removed, err := h.cache.LRem(args[0].Bulk, count, args[2].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: removed}
}

// HandleBLPop handles the BLPOP command which removes and returns an element from the head of the list
// If the list is empty, it blocks until an element is available or timeout is reached
// Parameters:
//   - args: Array of Values containing the keys and timeout in seconds
//
// Returns:
//   - models.Value: Array containing the key and popped element, or null if timeout reached
func (h *ListHandlers) HandleBLPop(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'blpop' command"}
	}

	// Last argument is the timeout
	timeout, err := util.ParseFloat(args[len(args)-1])
	if err != nil {
		return util.ToValue(err)
	}

	// Convert timeout to duration
	timeoutDuration := time.Duration(timeout * float64(time.Second))
	if timeout == 0 {
		timeoutDuration = time.Duration(0)
	}

	// Try each key until we get a value or reach timeout
	timer := time.NewTimer(timeoutDuration)
	defer timer.Stop()

	for {
		// Try all keys first without blocking
		for i := 0; i < len(args)-1; i++ {
			key := args[i].Bulk
			if value, exists := h.cache.LPop(key); exists {
				return models.Value{
					Type: "array",
					Array: []models.Value{
						{Type: "bulk", Bulk: key},
						{Type: "bulk", Bulk: value},
					},
				}
			}
		}

		// If timeout is 0, we keep trying indefinitely
		if timeout == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		select {
		case <-timer.C:
			return models.Value{Type: "null"}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// HandleBRPop handles the BRPOP command which removes and returns an element from the tail of the list
// If the list is empty, it blocks until an element is available or timeout is reached
// Parameters:
//   - args: Array of Values containing the keys and timeout in seconds
//
// Returns:
//   - models.Value: Array containing the key and popped element, or null if timeout reached
func (h *ListHandlers) HandleBRPop(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'brpop' command"}
	}

	// Last argument is the timeout
	timeout, err := util.ParseFloat(args[len(args)-1])
	if err != nil {
		return util.ToValue(err)
	}

	// Convert timeout to duration
	timeoutDuration := time.Duration(timeout * float64(time.Second))
	if timeout == 0 {
		timeoutDuration = time.Duration(0)
	}

	// Try each key until we get a value or reach timeout
	timer := time.NewTimer(timeoutDuration)
	defer timer.Stop()

	for {
		// Try all keys first without blocking
		for i := 0; i < len(args)-1; i++ {
			key := args[i].Bulk
			if value, exists := h.cache.RPop(key); exists {
				return models.Value{
					Type: "array",
					Array: []models.Value{
						{Type: "bulk", Bulk: key},
						{Type: "bulk", Bulk: value},
					},
				}
			}
		}

		// If timeout is 0, we keep trying indefinitely
		if timeout == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		select {
		case <-timer.C:
			return models.Value{Type: "null"}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// HandleBLMPOP handles the BLMPOP command which atomically removes and returns elements from multiple lists
// Parameters:
//   - args: Array of Values containing timeout, key count, keys, direction (LEFT/RIGHT), and count
//
// Returns:
//   - models.Value: Array containing the key and popped elements, or null if timeout reached
func (h *ListHandlers) HandleBLMPOP(args []models.Value) models.Value {
	if len(args) < 4 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'blmpop' command"}
	}

	timeout, err := util.ParseFloat(args[0])
	if err != nil {
		return util.ToValue(err)
	}

	keyCount, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	if len(args) < 2+keyCount+1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'blmpop' command"}
	}

	direction := args[2+keyCount].Bulk
	if direction != "LEFT" && direction != "RIGHT" {
		return models.Value{Type: "error", Str: "ERR direction must be LEFT or RIGHT"}
	}

	count := 1
	if len(args) > 3+keyCount {
		count, err = util.ParseInt(args[3+keyCount])
		if err != nil {
			return util.ToValue(err)
		}
		if count <= 0 {
			return models.Value{Type: "error", Str: "ERR count must be positive"}
		}
	}

	// Convert timeout to duration
	timeoutDuration := time.Duration(timeout * float64(time.Second))
	if timeout == 0 {
		timeoutDuration = time.Duration(0)
	}

	timer := time.NewTimer(timeoutDuration)
	defer timer.Stop()

	for {
		// Try all keys first without blocking
		for i := 0; i < keyCount; i++ {
			key := args[2+i].Bulk
			var elements []string
			var exists bool

			if direction == "LEFT" {
				for j := 0; j < count; j++ {
					if value, ok := h.cache.LPop(key); ok {
						elements = append(elements, value)
						exists = true
					} else {
						break
					}
				}
			} else {
				for j := 0; j < count; j++ {
					if value, ok := h.cache.RPop(key); ok {
						elements = append(elements, value)
						exists = true
					} else {
						break
					}
				}
			}

			if exists {
				result := []models.Value{{Type: "bulk", Bulk: key}}
				elementArray := make([]models.Value, len(elements))
				for j, element := range elements {
					elementArray[j] = models.Value{Type: "bulk", Bulk: element}
				}
				result = append(result, models.Value{Type: "array", Array: elementArray})
				return models.Value{Type: "array", Array: result}
			}
		}

		// If timeout is 0, we keep trying indefinitely
		if timeout == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		select {
		case <-timer.C:
			return models.Value{Type: "null"}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// HandleBLMOVE handles the BLMOVE command which atomically moves an element from one list to another
// Parameters:
//   - args: Array of Values containing source key, destination key, source direction (LEFT/RIGHT),
//     destination direction (LEFT/RIGHT), and timeout
//
// Returns:
//   - models.Value: The moved element, or null if timeout reached
func (h *ListHandlers) HandleBLMOVE(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 5); err != nil {
		return util.ToValue(err)
	}

	source := args[0].Bulk
	destination := args[1].Bulk
	sourceDir := args[2].Bulk
	destDir := args[3].Bulk

	if sourceDir != "LEFT" && sourceDir != "RIGHT" {
		return models.Value{Type: "error", Str: "ERR source direction must be LEFT or RIGHT"}
	}
	if destDir != "LEFT" && destDir != "RIGHT" {
		return models.Value{Type: "error", Str: "ERR destination direction must be LEFT or RIGHT"}
	}

	timeout, err := util.ParseFloat(args[4])
	if err != nil {
		return util.ToValue(err)
	}

	// Convert timeout to duration
	timeoutDuration := time.Duration(timeout * float64(time.Second))
	if timeout == 0 {
		timeoutDuration = time.Duration(0)
	}

	timer := time.NewTimer(timeoutDuration)
	defer timer.Stop()

	for {
		// Try to move element
		var value string
		var exists bool

		if sourceDir == "LEFT" {
			value, exists = h.cache.LPop(source)
		} else {
			value, exists = h.cache.RPop(source)
		}

		if exists {
			if destDir == "LEFT" {
				_, err = h.cache.LPush(destination, value)
			} else {
				_, err = h.cache.RPush(destination, value)
			}

			if err != nil {
				return util.ToValue(err)
			}

			return models.Value{Type: "bulk", Bulk: value}
		}

		// If timeout is 0, we keep trying indefinitely
		if timeout == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		select {
		case <-timer.C:
			return models.Value{Type: "null"}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// HandleLIndex handles the LINDEX command which returns an element from a list by its index
// Parameters:
//   - args: Array of Values containing the key and index
//
// Returns:
//   - models.Value: The element at the specified index, or null if out of range
func (h *ListHandlers) HandleLIndex(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	index, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.LIndex(args[0].Bulk, index)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

// HandleLInsert handles the LINSERT command which inserts an element before or after another element
// Parameters:
//   - args: Array of Values containing key, BEFORE/AFTER, pivot, and value
//
// Returns:
//   - models.Value: The length of the list after insertion, or -1 if pivot not found
func (h *ListHandlers) HandleLInsert(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 4); err != nil {
		return util.ToValue(err)
	}

	position := args[1].Bulk
	if position != "BEFORE" && position != "AFTER" {
		return models.Value{Type: "error", Str: "ERR syntax error"}
	}

	pivot := args[2].Bulk
	value := args[3].Bulk

	length, err := h.cache.LInsert(args[0].Bulk, position == "BEFORE", pivot, value)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: length}
}

// HandleLMove handles the LMOVE command which atomically moves an element from one list to another
// Parameters:
//   - args: Array of Values containing source key, destination key, source direction (LEFT/RIGHT),
//     and destination direction (LEFT/RIGHT)
//
// Returns:
//   - models.Value: The element being moved, or null if source list is empty
func (h *ListHandlers) HandleLMove(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 4); err != nil {
		return util.ToValue(err)
	}

	source := args[0].Bulk
	destination := args[1].Bulk
	sourceDir := args[2].Bulk
	destDir := args[3].Bulk

	if sourceDir != "LEFT" && sourceDir != "RIGHT" {
		return models.Value{Type: "error", Str: "ERR source direction must be LEFT or RIGHT"}
	}
	if destDir != "LEFT" && destDir != "RIGHT" {
		return models.Value{Type: "error", Str: "ERR destination direction must be LEFT or RIGHT"}
	}

	// Atomically move the element
	var value string
	var exists bool

	if sourceDir == "LEFT" {
		value, exists = h.cache.LPop(source)
	} else {
		value, exists = h.cache.RPop(source)
	}

	if !exists {
		return models.Value{Type: "null"}
	}

	var err error
	if destDir == "LEFT" {
		_, err = h.cache.LPush(destination, value)
	} else {
		_, err = h.cache.RPush(destination, value)
	}

	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "bulk", Bulk: value}
}

// HandleLMPop handles the LMPOP command which atomically removes and returns elements from multiple lists
// Parameters:
//   - args: Array of Values containing numkeys, keys..., direction (LEFT/RIGHT), [COUNT count]
//
// Returns:
//   - models.Value: Array containing the key and popped elements
func (h *ListHandlers) HandleLMPop(args []models.Value) models.Value {
	if len(args) < 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'lmpop' command"}
	}

	numKeys, err := util.ParseInt(args[0])
	if err != nil {
		return util.ToValue(err)
	}

	if len(args) < 1+numKeys+1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'lmpop' command"}
	}

	direction := args[1+numKeys].Bulk
	if direction != "LEFT" && direction != "RIGHT" {
		return models.Value{Type: "error", Str: "ERR direction must be LEFT or RIGHT"}
	}

	count := 1
	if len(args) > 2+numKeys {
		if args[2+numKeys].Bulk != "COUNT" {
			return models.Value{Type: "error", Str: "ERR syntax error"}
		}
		if len(args) <= 3+numKeys {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'lmpop' command"}
		}
		count, err = util.ParseInt(args[3+numKeys])
		if err != nil {
			return util.ToValue(err)
		}
		if count <= 0 {
			return models.Value{Type: "error", Str: "ERR count must be positive"}
		}
	}

	// Try each key until we find a non-empty list
	for i := 0; i < numKeys; i++ {
		key := args[1+i].Bulk
		var elements []string
		var exists bool

		if direction == "LEFT" {
			for j := 0; j < count; j++ {
				if value, ok := h.cache.LPop(key); ok {
					elements = append(elements, value)
					exists = true
				} else {
					break
				}
			}
		} else {
			for j := 0; j < count; j++ {
				if value, ok := h.cache.RPop(key); ok {
					elements = append(elements, value)
					exists = true
				} else {
					break
				}
			}
		}

		if exists {
			result := []models.Value{{Type: "bulk", Bulk: key}}
			elementArray := make([]models.Value, len(elements))
			for j, element := range elements {
				elementArray[j] = models.Value{Type: "bulk", Bulk: element}
			}
			result = append(result, models.Value{Type: "array", Array: elementArray})
			return models.Value{Type: "array", Array: result}
		}
	}

	return models.Value{Type: "null"}
}

// HandleLPos handles the LPOS command which returns the position of an element in the list
// Parameters:
//   - args: Array of Values containing the key and element to find
//
// Returns:
//   - models.Value: The position of the element, or null if not found
//     Returns error if wrong number of arguments
func (h *ListHandlers) HandleLPos(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	position, exists := h.cache.LPos(args[0].Bulk, args[1].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "integer", Num: position}
}

func (h *ListHandlers) HandleLPushXGet(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'lpushxget' command",
		}
	}

	key := args[0].Bulk
	value := args[1].Bulk

	length, err := h.cache.LPushXGet(key, value)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  err.Error(),
		}
	}

	return models.Value{
		Type: "integer",
		Num:  length,
	}
}

func (h *ListHandlers) HandleRPushXGet(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'rpushxget' command",
		}
	}

	key := args[0].Bulk
	value := args[1].Bulk

	length, err := h.cache.RPushXGet(key, value)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  err.Error(),
		}
	}

	return models.Value{
		Type: "integer",
		Num:  length,
	}
}

// HandleLPushX handles the LPUSHX command which inserts an element at the head of an existing list
// Parameters:
//   - args: Array of Values containing the key and value to push
//
// Returns:
//   - models.Value: The length of the list after the push operation
//     Returns error if wrong number of arguments or operation fails
func (h *ListHandlers) HandleLPushX(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	length, err := h.cache.LPushX(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: length}
}

// HandleRPushX handles the RPUSHX command which inserts an element at the tail of an existing list
// Parameters:
//   - args: Array of Values containing the key and value to push
//
// Returns:
//   - models.Value: The length of the list after the push operation
//     Returns error if wrong number of arguments or operation fails
func (h *ListHandlers) HandleRPushX(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	length, err := h.cache.RPushX(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: length}
}

// HandleLTrim handles the LTRIM command which trims a list to the specified range
// Parameters:
//   - args: Array of Values containing the key, start index, and stop index
//
// Returns:
//   - models.Value: "OK" if successful
//     Returns error if wrong number of arguments or invalid indices
func (h *ListHandlers) HandleLTrim(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	start, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	stop, err := util.ParseInt(args[2])
	if err != nil {
		return util.ToValue(err)
	}

	err = h.cache.LTrim(args[0].Bulk, start, stop)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *ListHandlers) HandleLInsertBeforeAfter(args []models.Value) models.Value {
	if len(args) < 4 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'linsertbeforeafter' command",
		}
	}

	key := args[0].Bulk
	operation := strings.ToLower(args[1].Bulk)
	if operation != "before" && operation != "after" {
		return models.Value{
			Type: "error",
			Str:  "ERR syntax error",
		}
	}

	pivot := args[2].Bulk
	values := make([]string, len(args)-3)
	for i := 3; i < len(args); i++ {
		values[i-3] = args[i].Bulk
	}

	// Default count is all values
	count := len(values)

	// Check for COUNT option
	for i := 3; i < len(args)-1; i++ {
		if strings.ToUpper(args[i].Bulk) == "COUNT" {
			var err error
			count, err = strconv.Atoi(args[i+1].Bulk)
			if err != nil {
				return models.Value{
					Type: "error",
					Str:  "ERR value is not an integer or out of range",
				}
			}
			if count <= 0 {
				return models.Value{
					Type: "error",
					Str:  "ERR COUNT must be positive",
				}
			}
			values = values[:i-3] // Exclude COUNT and its value from values slice
			break
		}
	}

	newLength, err := h.cache.LInsertBeforeAfter(key, operation == "before", pivot, values, count)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  err.Error(),
		}
	}
	if newLength == -1 {
		return models.Value{
			Type: "integer",
			Num:  -1, // Pivot was not found
		}
	}

	return models.Value{
		Type: "integer",
		Num:  newLength,
	}
}
