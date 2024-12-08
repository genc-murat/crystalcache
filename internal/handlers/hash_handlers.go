package handlers

import (
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

// HashHandlers implements handlers for hash operations in the cache
type HashHandlers struct {
	cache ports.Cache
}

// NewHashHandlers creates a new instance of HashHandlers
// Parameters:
//   - cache: The cache implementation to be used for hash operations
//
// Returns:
//   - *HashHandlers: A pointer to the newly created HashHandlers instance
func NewHashHandlers(cache ports.Cache) *HashHandlers {
	return &HashHandlers{cache: cache}
}

// HandleHSet handles the HSET command which sets field-value pairs in a hash
// Parameters:
//   - args: Array of Values containing the key followed by field-value pairs
//
// Returns:
//   - models.Value: Number of fields that were added as an integer response
//     Returns error if wrong number of arguments or if operation fails
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

// HandleHGet handles the HGET command which retrieves the value of a field in a hash
// Parameters:
//   - args: Array of Values containing the key and field name
//
// Returns:
//   - models.Value: The value associated with field, or null if field doesn't exist
//     Returns error if wrong number of arguments
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

// HandleHGetAll handles the HGETALL command which retrieves all field-value pairs in a hash
// Parameters:
//   - args: Array of Values containing the key
//
// Returns:
//   - models.Value: Array of alternating field names and values
//     Returns error if wrong number of arguments
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

// HandleHLen handles the HLEN command which returns the number of fields in a hash
// Parameters:
//   - args: Array of Values containing the key
//
// Returns:
//   - models.Value: Number of fields in the hash as an integer
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHLen(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	pairs := h.cache.HGetAll(args[0].Bulk)
	return models.Value{Type: "integer", Num: len(pairs)}
}

// HandleHScan handles the HSCAN command which incrementally iterates over a hash
// Parameters:
//   - args: Array of Values containing:
//   - key: Hash key to scan
//   - cursor: Starting position
//   - Optional MATCH pattern
//   - Optional COUNT count
//
// Returns:
//   - models.Value: Array containing next cursor and array of elements
//     Returns error if invalid arguments or syntax error
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

	// Parse optional arguments
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

	results, nextCursor := h.cache.HScan(key, cursor, pattern, count)

	resultArray := make([]models.Value, len(results))
	for i, str := range results {
		resultArray[i] = models.Value{Type: "string", Str: str}
	}

	return models.Value{
		Type: "array",
		Array: []models.Value{
			{Type: "string", Str: strconv.Itoa(nextCursor)},
			{Type: "array", Array: resultArray},
		},
	}
}

// HandleHDel handles the HDEL command which removes one or more fields from a hash
// Parameters:
//   - args: Array of Values containing the key followed by one or more fields
//
// Returns:
//   - models.Value: Number of fields that were removed as an integer
//     Returns error if wrong number of arguments
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

// HandleHExists handles the HEXISTS command which checks if a field exists in a hash
// Parameters:
//   - args: Array of Values containing the key and field name
//
// Returns:
//   - models.Value: 1 if the field exists, 0 if it doesn't
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHExists(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	_, exists := h.cache.HGet(args[0].Bulk, args[1].Bulk)
	if exists {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

// HandleHExpire handles the HEXPIRE command which sets an expiration time for a hash
// Parameters:
//   - args: Array of Values containing the key and expiration time in seconds
//
// Returns:
//   - models.Value: 1 if timeout was set, 0 if key doesn't exist
//     Returns error if wrong number of arguments or invalid timeout
func (h *HashHandlers) HandleHExpire(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	// Check if the hash exists by trying to get its length
	pairs := h.cache.HGetAll(args[0].Bulk)
	if len(pairs) == 0 {
		return models.Value{Type: "integer", Num: 0}
	}

	// Parse the timeout value
	seconds, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR timeout is not an integer or out of range"}
	}

	if seconds <= 0 {
		return models.Value{Type: "error", Str: "ERR timeout must be positive"}
	}

	// Assuming the Cache interface has an Expire method
	err = h.cache.Expire(args[0].Bulk, seconds)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}
