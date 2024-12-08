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

// HandleHExpireAt handles the HEXPIREAT command which sets an absolute Unix timestamp expiration
// Parameters:
//   - args: Array of Values containing the key and Unix timestamp
//
// Returns:
//   - models.Value: 1 if timeout was set, 0 if key doesn't exist
//     Returns error if wrong number of arguments or invalid timestamp
func (h *HashHandlers) HandleHExpireAt(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	// Parse the timestamp
	timestamp, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR timestamp is not an integer or out of range"}
	}

	// Set expiration at timestamp
	err = h.cache.ExpireAt(args[0].Bulk, timestamp)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}

// HandleHExpireTime handles the HEXPIRETIME command which returns the absolute Unix timestamp
// Parameters:
//   - args: Array of Values containing the key
//
// Returns:
//   - models.Value: Unix timestamp in seconds when the key will expire, or -1/-2 for no expiration/non-existent keys
func (h *HashHandlers) HandleHExpireTime(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	timestamp, err := h.cache.ExpireTime(args[0].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: int(timestamp)}
}

// HandleHIncrBy handles the HINCRBY command which increments a hash field by an integer
// Parameters:
//   - args: Array of Values containing the key, field, and increment value
//
// Returns:
//   - models.Value: The new value after increment
//     Returns error if wrong number of arguments, non-integer field value, or overflow
func (h *HashHandlers) HandleHIncrBy(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	// Parse increment
	increment, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR increment is not an integer or out of range"}
	}

	// Increment the field
	newVal, err := h.cache.HIncrBy(args[0].Bulk, args[1].Bulk, increment)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: int(newVal)}
}

// HandleHIncrByFloat handles the HINCRBYFLOAT command which increments a hash field by a float
// Parameters:
//   - args: Array of Values containing the key, field, and increment value
//
// Returns:
//   - models.Value: The new value after increment as a string
//     Returns error if wrong number of arguments, non-numeric field value, or overflow
func (h *HashHandlers) HandleHIncrByFloat(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	// Parse float increment
	increment, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR increment is not a valid float"}
	}

	// Increment the field
	newVal, err := h.cache.HIncrByFloat(args[0].Bulk, args[1].Bulk, increment)
	if err != nil {
		return util.ToValue(err)
	}

	// Convert float to string with proper precision
	return models.Value{Type: "bulk", Bulk: strconv.FormatFloat(newVal, 'f', -1, 64)}
}
