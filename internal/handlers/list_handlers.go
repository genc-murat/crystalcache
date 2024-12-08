package handlers

import (
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
