package handlers

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

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

// HandleHKeys handles the HKEYS command which returns all field names in a hash
// Parameters:
//   - args: Array of Values containing the hash key
//
// Returns:
//   - models.Value: Array of field names
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHKeys(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	// Get all key-value pairs
	pairs := h.cache.HGetAll(args[0].Bulk)

	// Extract keys
	result := make([]models.Value, 0, len(pairs))
	for key := range pairs {
		result = append(result, models.Value{Type: "bulk", Bulk: key})
	}

	return models.Value{Type: "array", Array: result}
}

// HandleHMSet handles the HMSET command which sets multiple field-value pairs in a hash
// Parameters:
//   - args: Array of Values containing:
//   - key: hash key
//   - field value pairs: one or more field-value pairs to set
//
// Returns:
//   - models.Value: "OK" if successful
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHMSet(args []models.Value) models.Value {
	if len(args) < 3 || len(args)%2 != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'hmset' command"}
	}

	hashKey := args[0].Bulk

	// Set each field-value pair
	for i := 1; i < len(args); i += 2 {
		err := h.cache.HSet(hashKey, args[i].Bulk, args[i+1].Bulk)
		if err != nil {
			return util.ToValue(err)
		}
	}

	// HMSET always returns "OK" on success (different from HSET which returns number of fields added)
	return models.Value{Type: "string", Str: "OK"}
}

// HandleHMGet handles the HMGET command which gets values for multiple fields in a hash
// Parameters:
//   - args: Array of Values containing the hash key followed by field names
//
// Returns:
//   - models.Value: Array of values for the requested fields (nil for non-existing fields)
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHMGet(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for HMGET command"}
	}

	hashKey := args[0].Bulk
	result := make([]models.Value, len(args)-1)

	// Get value for each field
	for i := 1; i < len(args); i++ {
		value, exists := h.cache.HGet(hashKey, args[i].Bulk)
		if exists {
			result[i-1] = models.Value{Type: "bulk", Bulk: value}
		} else {
			result[i-1] = models.Value{Type: "null"}
		}
	}

	return models.Value{Type: "array", Array: result}
}

// HandleHPersist handles the HPERSIST command which removes the expiration from a hash
// Parameters:
//   - args: Array of Values containing the hash key
//
// Returns:
//   - models.Value: 1 if the timeout was removed, 0 if key doesn't exist or has no timeout
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHPersist(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	// Check if hash exists and has a timeout
	expireTime, err := h.cache.ExpireTime(args[0].Bulk)
	if err != nil || expireTime < 0 {
		return models.Value{Type: "integer", Num: 0}
	}

	// Remove timeout by setting it to -1
	err = h.cache.ExpireAt(args[0].Bulk, -1)
	if err != nil {
		return models.Value{Type: "integer", Num: 0}
	}

	return models.Value{Type: "integer", Num: 1}
}

// HandleHSetNX handles the HSETNX command which sets a field only if it does not exist
// Parameters:
//   - args: Array of Values containing the hash key, field name, and value
//
// Returns:
//   - models.Value: 1 if field was set, 0 if field exists
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHSetNX(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	// Check if field exists
	_, exists := h.cache.HGet(args[0].Bulk, args[1].Bulk)
	if exists {
		return models.Value{Type: "integer", Num: 0}
	}

	// Set field if it doesn't exist
	err := h.cache.HSet(args[0].Bulk, args[1].Bulk, args[2].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}

// HandleHStrLen handles the HSTRLEN command which returns the string length of a hash field's value
// Parameters:
//   - args: Array of Values containing the hash key and field name
//
// Returns:
//   - models.Value: Length of the field value, 0 if field doesn't exist
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHStrLen(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.HGet(args[0].Bulk, args[1].Bulk)
	if !exists {
		return models.Value{Type: "integer", Num: 0}
	}

	return models.Value{Type: "integer", Num: len(value)}
}

// HandleHTTL handles the HTTL command which returns the remaining time to live of a hash
// Parameters:
//   - args: Array of Values containing the hash key
//
// Returns:
//   - models.Value: Remaining TTL in seconds, -2 if key doesn't exist, -1 if no TTL
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHTTL(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	expireTime, err := h.cache.ExpireTime(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "integer", Num: -2}
	}

	// Check if the hash exists
	pairs := h.cache.HGetAll(args[0].Bulk)
	if len(pairs) == 0 {
		return models.Value{Type: "integer", Num: -2}
	}

	// If no expiration
	if expireTime < 0 {
		return models.Value{Type: "integer", Num: -1}
	}

	// Calculate remaining time
	remaining := expireTime - time.Now().Unix()
	if remaining < 0 {
		return models.Value{Type: "integer", Num: -2}
	}

	return models.Value{Type: "integer", Num: int(remaining)}
}

// HandleHVals handles the HVALS command which returns all values in a hash
// Parameters:
//   - args: Array of Values containing the hash key
//
// Returns:
//   - models.Value: Array of all values in the hash
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHVals(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	// Get all key-value pairs
	pairs := h.cache.HGetAll(args[0].Bulk)

	// Extract values
	result := make([]models.Value, 0, len(pairs))
	for _, value := range pairs {
		result = append(result, models.Value{Type: "bulk", Bulk: value})
	}

	return models.Value{Type: "array", Array: result}
}

// HandleHPTTL handles the HPTTL command which returns the remaining time to live of a hash in milliseconds
// Parameters:
//   - args: Array of Values containing the hash key
//
// Returns:
//   - models.Value: Remaining TTL in milliseconds, -2 if key doesn't exist, -1 if no TTL
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHPTTL(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	expireTime, err := h.cache.ExpireTime(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "integer", Num: -2}
	}

	// Check if the hash exists
	pairs := h.cache.HGetAll(args[0].Bulk)
	if len(pairs) == 0 {
		return models.Value{Type: "integer", Num: -2}
	}

	// If no expiration
	if expireTime < 0 {
		return models.Value{Type: "integer", Num: -1}
	}

	// Calculate remaining time in milliseconds
	remaining := (expireTime - time.Now().Unix()) * 1000
	if remaining < 0 {
		return models.Value{Type: "integer", Num: -2}
	}

	return models.Value{Type: "integer", Num: int(remaining)}
}

// HandleHRandField handles the HRANDFIELD command which returns random fields from a hash
// Parameters:
//   - args: Array of Values containing:
//   - key: hash key
//   - count (optional): number of fields to return (default 1)
//   - withvalues (optional): "WITHVALUES" flag to include values
//
// Returns:
//   - models.Value: Single field name if no count specified,
//     Array of field names if count specified,
//     Array of field-value pairs if WITHVALUES specified
//     Returns error if wrong arguments or key doesn't exist
func (h *HashHandlers) HandleHRandField(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'hrandfield' command"}
	}

	// Get all key-value pairs
	pairs := h.cache.HGetAll(args[0].Bulk)
	if len(pairs) == 0 {
		return models.Value{Type: "null"}
	}

	// Convert map to slice of keys for random selection
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}

	// Default to returning one field
	count := 1
	withValues := false

	// Parse optional arguments
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(args[1].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer"}
		}

		// Check for WITHVALUES flag
		if len(args) > 2 && strings.ToUpper(args[2].Bulk) == "WITHVALUES" {
			withValues = true
		}
	}

	// Handle negative count (absolute value without duplicates)
	allowDuplicates := true
	if count < 0 {
		count = -count
		allowDuplicates = false
	}

	// Generate random fields
	var result []models.Value
	if allowDuplicates {
		// With duplicates
		result = make([]models.Value, 0, count)
		for i := 0; i < count; i++ {
			idx := rand.Intn(len(keys))
			if withValues {
				result = append(result,
					models.Value{Type: "bulk", Bulk: keys[idx]},
					models.Value{Type: "bulk", Bulk: pairs[keys[idx]]},
				)
			} else {
				result = append(result, models.Value{Type: "bulk", Bulk: keys[idx]})
			}
		}
	} else {
		// Without duplicates
		if count > len(keys) {
			count = len(keys)
		}
		// Fisher-Yates shuffle
		for i := len(keys) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			keys[i], keys[j] = keys[j], keys[i]
		}
		result = make([]models.Value, 0, count*2)
		for i := 0; i < count; i++ {
			if withValues {
				result = append(result,
					models.Value{Type: "bulk", Bulk: keys[i]},
					models.Value{Type: "bulk", Bulk: pairs[keys[i]]},
				)
			} else {
				result = append(result, models.Value{Type: "bulk", Bulk: keys[i]})
			}
		}
	}

	// Return single field if no count was specified
	if len(args) == 1 && len(result) > 0 {
		return result[0]
	}

	return models.Value{Type: "array", Array: result}
}

// HandleHPExpire handles the HPEXPIRE command which sets expiration time in milliseconds for a hash
// Parameters:
//   - args: Array of Values containing:
//   - key: hash key
//   - milliseconds: time to live in milliseconds
//
// Returns:
//   - models.Value: 1 if timeout was set, 0 if key doesn't exist
//     Returns error if wrong number of arguments or invalid timeout
func (h *HashHandlers) HandleHPExpire(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	// Check if the hash exists
	pairs := h.cache.HGetAll(args[0].Bulk)
	if len(pairs) == 0 {
		return models.Value{Type: "integer", Num: 0}
	}

	// Parse milliseconds
	milliseconds, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR timeout is not an integer or out of range"}
	}

	if milliseconds <= 0 {
		return models.Value{Type: "error", Str: "ERR timeout must be positive"}
	}

	// Convert milliseconds to Unix timestamp
	expireAt := time.Now().UnixNano()/1e6 + milliseconds // Current time in ms + duration in ms

	// Set expiration using ExpireAt with the calculated timestamp
	err = h.cache.ExpireAt(args[0].Bulk, expireAt/1000) // Convert ms to seconds for ExpireAt
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}

// HandleHPExpireAt handles the HPEXPIREAT command which sets an absolute Unix timestamp in milliseconds for expiration
// Parameters:
//   - args: Array of Values containing:
//   - key: hash key
//   - timestamp_ms: Unix timestamp in milliseconds
//
// Returns:
//   - models.Value: 1 if timeout was set, 0 if key doesn't exist
//     Returns error if wrong number of arguments or invalid timestamp
func (h *HashHandlers) HandleHPExpireAt(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	// Check if the hash exists
	pairs := h.cache.HGetAll(args[0].Bulk)
	if len(pairs) == 0 {
		return models.Value{Type: "integer", Num: 0}
	}

	// Parse timestamp in milliseconds
	timestampMs, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR timestamp is not an integer or out of range"}
	}

	if timestampMs < 0 {
		return models.Value{Type: "error", Str: "ERR timestamp must be positive"}
	}

	// Convert milliseconds timestamp to seconds for ExpireAt
	err = h.cache.ExpireAt(args[0].Bulk, timestampMs/1000)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}

// HandleHPExpireTime handles the HPEXPIRETIME command which returns the absolute Unix timestamp in milliseconds when the key will expire
// Parameters:
//   - args: Array of Values containing the hash key
//
// Returns:
//   - models.Value: Timestamp in milliseconds when the key will expire
//     -1 if the key has no expiration, -2 if the key does not exist
//     Returns error if wrong number of arguments
func (h *HashHandlers) HandleHPExpireTime(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	// Get expiration time in seconds
	expireTime, err := h.cache.ExpireTime(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "integer", Num: -2}
	}

	// Check if the hash exists
	pairs := h.cache.HGetAll(args[0].Bulk)
	if len(pairs) == 0 {
		return models.Value{Type: "integer", Num: -2}
	}

	// If no expiration
	if expireTime < 0 {
		return models.Value{Type: "integer", Num: -1}
	}

	// Convert seconds to milliseconds
	return models.Value{Type: "integer", Num: int(expireTime * 1000)}
}

func (h *HashHandlers) HandleHDelIf(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'hdelif' command",
		}
	}

	key := args[0].Bulk
	field := args[1].Bulk
	expectedValue := args[2].Bulk

	deleted, err := h.cache.HDelIf(key, field, expectedValue)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  err.Error(),
		}
	}

	if deleted {
		return models.Value{
			Type: "integer",
			Num:  1,
		}
	}

	return models.Value{
		Type: "integer",
		Num:  0,
	}
}

func (h *HashHandlers) HandleHIncrByFloatIf(args []models.Value) models.Value {
	if len(args) != 4 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'hincrbyfloatif' command",
		}
	}

	key := args[0].Bulk
	field := args[1].Bulk
	increment, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  "ERR increment is not a valid float",
		}
	}

	expectedValue := args[3].Bulk

	newValue, success, err := h.cache.HIncrByFloatIf(key, field, increment, expectedValue)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  err.Error(),
		}
	}

	if !success {
		return models.Value{
			Type: "null",
		}
	}

	return models.Value{
		Type: "bulk",
		Bulk: strconv.FormatFloat(newValue, 'f', -1, 64),
	}
}
