package handlers

import (
	"log"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type StringHandlers struct {
	cache ports.Cache
}

func NewStringHandlers(cache ports.Cache) *StringHandlers {
	return &StringHandlers{cache: cache}
}

func (h *StringHandlers) HandleSet(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk
	nx := false
	xx := false
	expireSeconds := -1 // Changed to int since Expire expects int

	// Parse optional arguments
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "EX":
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR syntax error"}
			}
			seconds, err := util.ParseInt(args[i+1])
			if err != nil {
				return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
			}
			if seconds <= 0 {
				return models.Value{Type: "error", Str: "ERR invalid expire time in set"}
			}
			expireSeconds = seconds // No conversion needed, keeping as int
			i++                     // Skip the next argument since we consumed it
		}
	}

	// Check NX/XX conditions
	exists := h.cache.Exists(key)
	if (nx && exists) || (xx && !exists) {
		return models.Value{Type: "null"}
	}

	// Set the value
	err := h.cache.Set(key, value)
	if err != nil {
		return util.ToValue(err)
	}

	// Set expiration if specified
	if expireSeconds > 0 {
		err = h.cache.Expire(key, expireSeconds)
		if err != nil {
			return util.ToValue(err)
		}
	}

	log.Printf("[DEBUG] SET key=%s value=%s nx=%v xx=%v ex=%d", key, value, nx, xx, expireSeconds)
	return models.Value{Type: "string", Str: "OK"}
}

func (h *StringHandlers) HandleGet(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.Get(args[0].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

func (h *StringHandlers) HandleIncr(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	result, err := h.cache.Incr(args[0].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: result}
}

func (h *StringHandlers) HandleDel(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	deleted, err := h.cache.Del(args[0].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	if deleted {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *StringHandlers) HandleExists(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	exists := h.cache.Exists(args[0].Bulk)
	if exists {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *StringHandlers) HandleExpire(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	seconds, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	err = h.cache.Expire(args[0].Bulk, seconds)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: 1}
}

func (h *StringHandlers) HandleStrlen(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.Get(args[0].Bulk)
	if !exists {
		return models.Value{Type: "integer", Num: 0}
	}

	return models.Value{Type: "integer", Num: len(value)}
}

func (h *StringHandlers) HandleGetRange(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	start, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}

	end, err := util.ParseInt(args[2])
	if err != nil {
		return util.ToValue(err)
	}

	value, exists := h.cache.Get(args[0].Bulk)
	if !exists {
		return models.Value{Type: "bulk", Bulk: ""}
	}

	length := len(value)
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return models.Value{Type: "bulk", Bulk: ""}
	}

	return models.Value{Type: "bulk", Bulk: value[start : end+1]}
}

func (h *StringHandlers) HandleEcho(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'echo' command"}
	}
	return models.Value{Type: "bulk", Bulk: args[0].Bulk}
}

func (h *StringHandlers) HandleMSet(args []models.Value) models.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'mset' command"}
	}

	// Set each key-value pair
	for i := 0; i < len(args); i += 2 {
		key := args[i].Bulk
		value := args[i+1].Bulk

		err := h.cache.Set(key, value)
		if err != nil {
			return util.ToValue(err)
		}
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *StringHandlers) HandleMGet(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'mget' command"}
	}

	result := make([]models.Value, len(args))

	// Get value for each key
	for i, arg := range args {
		value, exists := h.cache.Get(arg.Bulk)
		if !exists {
			result[i] = models.Value{Type: "null"}
		} else {
			result[i] = models.Value{Type: "bulk", Bulk: value}
		}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *StringHandlers) HandleLCS(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	// Get the two strings to compare
	str1, exists1 := h.cache.Get(args[0].Bulk)
	if !exists1 {
		return models.Value{Type: "null"}
	}

	str2, exists2 := h.cache.Get(args[1].Bulk)
	if !exists2 {
		return models.Value{Type: "null"}
	}

	// Find the LCS using dynamic programming
	lcs := findLCS(str1, str2)

	return models.Value{Type: "bulk", Bulk: lcs}
}

// findLCS helper function implements the dynamic programming solution
// to find the Longest Common Subsequence
func findLCS(text1, text2 string) string {
	m, n := len(text1), len(text2)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	// Fill the dp table
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if text1[i-1] == text2[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				dp[i][j] = max(dp[i-1][j], dp[i][j-1])
			}
		}
	}

	// Reconstruct the LCS
	var result strings.Builder
	i, j := m, n
	for i > 0 && j > 0 {
		if text1[i-1] == text2[j-1] {
			result.WriteByte(text1[i-1])
			i--
			j--
		} else if dp[i-1][j] > dp[i][j-1] {
			i--
		} else {
			j--
		}
	}

	// Reverse the result since we built it backwards
	runes := []rune(result.String())
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	return string(runes)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (h *StringHandlers) HandleMSetNX(args []models.Value) models.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'msetnx' command"}
	}

	// First check if any of the keys exist
	for i := 0; i < len(args); i += 2 {
		key := args[i].Bulk
		if h.cache.Exists(key) {
			return models.Value{Type: "integer", Num: 0}
		}
	}

	// None of the keys exist, so set them all
	for i := 0; i < len(args); i += 2 {
		key := args[i].Bulk
		value := args[i+1].Bulk

		err := h.cache.Set(key, value)
		if err != nil {
			return util.ToValue(err)
		}
	}

	return models.Value{Type: "integer", Num: 1}
}

func (h *StringHandlers) HandleSetRange(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 3); err != nil {
		return util.ToValue(err)
	}

	key := args[0].Bulk
	offset, err := util.ParseInt(args[1])
	if err != nil {
		return util.ToValue(err)
	}
	if offset < 0 {
		return models.Value{Type: "error", Str: "ERR offset is out of range"}
	}

	value := args[2].Bulk

	// Get the current value or empty string if key doesn't exist
	currentVal, exists := h.cache.Get(key)
	if !exists {
		currentVal = ""
	}

	// Calculate the new string length
	newLen := offset + len(value)
	if newLen < len(currentVal) {
		newLen = len(currentVal)
	}

	// Create new string with correct length
	result := make([]byte, newLen)

	// Copy existing string
	copy(result, currentVal)

	// If offset is beyond current length, pad with zero bytes
	for i := len(currentVal); i < offset; i++ {
		result[i] = 0
	}

	// Copy the new value at offset
	copy(result[offset:], value)

	// Store the result
	err = h.cache.Set(key, string(result))
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "integer", Num: len(result)}
}
