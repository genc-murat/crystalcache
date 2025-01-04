package handlers

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
	"github.com/genc-murat/crystalcache/pkg/utils/pattern"
)

type SetHandlers struct {
	cache ports.Cache
}

func NewSetHandlers(cache ports.Cache) *SetHandlers {
	return &SetHandlers{cache: cache}
}

func (h *SetHandlers) HandleSAdd(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'sadd' command"}
	}

	key := args[0].Bulk
	added := 0

	// Handle multiple members
	for i := 1; i < len(args); i++ {
		wasAdded, err := h.cache.SAdd(key, args[i].Bulk)
		if err != nil {
			return util.ToValue(err)
		}
		if wasAdded {
			added++
		}
	}

	return models.Value{Type: "integer", Num: added}
}

func (h *SetHandlers) HandleSMembers(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	members, err := h.cache.SMembers(args[0].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSCard(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	count := h.cache.SCard(args[0].Bulk)
	return models.Value{Type: "integer", Num: count}
}

func (h *SetHandlers) HandleSRem(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	removed, err := h.cache.SRem(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return util.ToValue(err)
	}

	if removed {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *SetHandlers) HandleSIsMember(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	isMember := h.cache.SIsMember(args[0].Bulk, args[1].Bulk)
	if isMember {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (h *SetHandlers) HandleSInter(args []models.Value) models.Value {
	if err := util.ValidateMinArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	intersection := h.cache.SInter(keys...)
	result := make([]models.Value, len(intersection))
	for i, member := range intersection {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSUnion(args []models.Value) models.Value {
	if err := util.ValidateMinArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	union := h.cache.SUnion(keys...)
	result := make([]models.Value, len(union))
	for i, member := range union {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSDiff(args []models.Value) models.Value {
	if err := util.ValidateMinArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	diff := h.cache.SDiff(keys...)
	result := make([]models.Value, len(diff))
	for i, member := range diff {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSScan(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for SSCAN"}
	}

	key := args[0].Bulk
	cursor, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR invalid cursor"}
	}

	// Default values
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

	members, err := h.cache.SMembers(key)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if cursor >= len(members) {
		return models.Value{Type: "array", Array: []models.Value{
			{Type: "string", Str: "0"},
			{Type: "array", Array: []models.Value{}},
		}}
	}

	var matches []string
	for i := cursor; i < len(members) && len(matches) < count; i++ {
		if matchPattern(pattern, members[i]) {
			matches = append(matches, members[i])
		}
	}

	nextCursor := 0
	if cursor+count < len(members) {
		nextCursor = cursor + count
	}

	matchValues := make([]models.Value, len(matches))
	for i, match := range matches {
		matchValues[i] = models.Value{Type: "string", Str: match}
	}

	return models.Value{Type: "array", Array: []models.Value{
		{Type: "string", Str: strconv.Itoa(nextCursor)},
		{Type: "array", Array: matchValues},
	}}
}
func matchPattern(pattern, str string) bool {
	if pattern == "*" {
		return true
	}

	regexPattern := strings.Builder{}
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*':
			regexPattern.WriteString(".*")
		case '?':
			regexPattern.WriteString(".")
		case '[', ']', '(', ')', '{', '}', '.', '+', '|', '^', '$':
			regexPattern.WriteString("\\")
			regexPattern.WriteByte(pattern[i])
		default:
			regexPattern.WriteByte(pattern[i])
		}
	}

	regex, err := regexp.Compile("^" + regexPattern.String() + "$")
	if err != nil {
		return false
	}

	return regex.MatchString(str)
}

func (h *SetHandlers) HandleSDiffStore(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'sdiffstore' command"}
	}

	destination := args[0].Bulk
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = args[i].Bulk
	}

	// Get the difference first
	diff := h.cache.SDiff(keys...)

	// Clear the destination key if it exists
	h.cache.Del(destination)

	// Store each element from the difference in the destination
	stored := 0
	for _, member := range diff {
		added, err := h.cache.SAdd(destination, member)
		if err != nil {
			return util.ToValue(err)
		}
		if added {
			stored++
		}
	}

	return models.Value{Type: "integer", Num: stored}
}

func (h *SetHandlers) HandleSInterCard(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'sintercard' command"}
	}

	// Extract limit if provided
	limit := -1 // -1 means no limit
	numKeys := len(args)

	// Check if LIMIT option is provided
	if len(args) >= 3 && strings.ToUpper(args[len(args)-2].Bulk) == "LIMIT" {
		var err error
		limit, err = strconv.Atoi(args[len(args)-1].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
		if limit < 0 {
			return models.Value{Type: "error", Str: "ERR LIMIT can't be negative"}
		}
		numKeys = len(args) - 2
	}

	// Get the keys
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = args[i].Bulk
	}

	// Get the intersection
	intersection := h.cache.SInter(keys...)

	// Apply limit if set
	if limit >= 0 && len(intersection) > limit {
		return models.Value{Type: "integer", Num: limit}
	}

	return models.Value{Type: "integer", Num: len(intersection)}
}

func (h *SetHandlers) HandleSInterStore(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'sinterstore' command"}
	}

	destination := args[0].Bulk
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = args[i].Bulk
	}

	// Get the intersection first
	intersection := h.cache.SInter(keys...)

	// Clear the destination key if it exists
	h.cache.Del(destination)

	// Store each element from the intersection in the destination
	stored := 0
	for _, member := range intersection {
		added, err := h.cache.SAdd(destination, member)
		if err != nil {
			return util.ToValue(err)
		}
		if added {
			stored++
		}
	}

	return models.Value{Type: "integer", Num: stored}
}

func (h *SetHandlers) HandleSMIsMember(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'smismember' command"}
	}

	key := args[0].Bulk
	members := args[1:]

	result := make([]models.Value, len(members))
	for i, member := range members {
		isMember := h.cache.SIsMember(key, member.Bulk)
		if isMember {
			result[i] = models.Value{Type: "integer", Num: 1}
		} else {
			result[i] = models.Value{Type: "integer", Num: 0}
		}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSMove(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'smove' command"}
	}

	source := args[0].Bulk
	destination := args[1].Bulk
	member := args[2].Bulk

	// Check if member exists in source
	if !h.cache.SIsMember(source, member) {
		return models.Value{Type: "integer", Num: 0}
	}

	// Remove from source and add to destination
	_, _ = h.cache.SRem(source, member)
	_, _ = h.cache.SAdd(destination, member)

	return models.Value{Type: "integer", Num: 1}
}

func (h *SetHandlers) HandleSPop(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'spop' command"}
	}

	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(args[1].Bulk)
		if err != nil || count < 0 {
			return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
	}

	members, err := h.cache.SMembers(args[0].Bulk)
	if err != nil || len(members) == 0 {
		if count == 1 {
			return models.Value{Type: "null"}
		}
		return models.Value{Type: "array", Array: []models.Value{}}
	}

	// Shuffle members using Fisher-Yates algorithm
	for i := len(members) - 1; i > 0; i-- {
		j := util.RandomInt(i + 1)
		members[i], members[j] = members[j], members[i]
	}

	// Limit count to available members
	if count > len(members) {
		count = len(members)
	}

	// Remove and collect popped members
	result := make([]models.Value, count)
	for i := 0; i < count; i++ {
		_, _ = h.cache.SRem(args[0].Bulk, members[i])
		result[i] = models.Value{Type: "bulk", Bulk: members[i]}
	}

	if count == 1 {
		return result[0]
	}
	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSRandMember(args []models.Value) models.Value {
	if len(args) < 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'srandmember' command"}
	}

	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(args[1].Bulk)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
		}
	}

	members, err := h.cache.SMembers(args[0].Bulk)
	if err != nil || len(members) == 0 {
		if count == 1 {
			return models.Value{Type: "null"}
		}
		return models.Value{Type: "array", Array: []models.Value{}}
	}

	if count >= 0 {
		// Positive count: return unique elements
		if count > len(members) {
			count = len(members)
		}
		// Shuffle array
		for i := len(members) - 1; i > 0; i-- {
			j := util.RandomInt(i + 1)
			members[i], members[j] = members[j], members[i]
		}
	} else {
		// Negative count: allow duplicates
		count = -count
		result := make([]models.Value, count)
		for i := 0; i < count; i++ {
			idx := util.RandomInt(len(members))
			result[i] = models.Value{Type: "bulk", Bulk: members[idx]}
		}
		return models.Value{Type: "array", Array: result}
	}

	if count == 1 {
		return models.Value{Type: "bulk", Bulk: members[0]}
	}

	result := make([]models.Value, count)
	for i := 0; i < count; i++ {
		result[i] = models.Value{Type: "bulk", Bulk: members[i]}
	}
	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSUnionStore(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'sunionstore' command"}
	}

	destination := args[0].Bulk
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = args[i].Bulk
	}

	union := h.cache.SUnion(keys...)
	h.cache.Del(destination)

	stored := 0
	for _, member := range union {
		added, err := h.cache.SAdd(destination, member)
		if err != nil {
			return util.ToValue(err)
		}
		if added {
			stored++
		}
	}

	return models.Value{Type: "integer", Num: stored}
}

func (h *SetHandlers) HandleSMemRandomCount(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for SMEMRANDOMCOUNT command"}
	}

	key := args[0].Bulk
	count, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR count argument must be an integer"}
	}

	if count < 0 {
		return models.Value{Type: "error", Str: "ERR count cannot be negative"}
	}

	// Default to allowing duplicates if not specified
	allowDuplicates := true
	if len(args) >= 3 {
		if strings.ToLower(args[2].Bulk) == "unique" {
			allowDuplicates = false
		}
	}

	members, err := h.cache.SMemRandomCount(key, count, allowDuplicates)
	if err != nil {
		return models.Value{Type: "error", Str: fmt.Sprintf("ERR %v", err)}
	}

	// Convert string slice to Value slice
	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}

func (h *SetHandlers) HandleSDiffStoreDel(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'sdiffstoredel' command",
		}
	}

	destination := args[0].Bulk
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = args[i].Bulk
	}

	count, err := h.cache.SDiffStoreDel(destination, keys)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  err.Error(),
		}
	}

	return models.Value{
		Type: "integer",
		Num:  count,
	}
}

func (h *SetHandlers) HandleSMembersPattern(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{
			Type: "error",
			Str:  "ERR wrong number of arguments for 'smemberspattern' command",
		}
	}

	key := args[0].Bulk
	patternStr := args[1].Bulk

	// Check if it's a valid pattern
	if !pattern.IsPattern(patternStr) && patternStr != "*" {
		// If it's not a pattern, treat it like SISMEMBER
		if exists := h.cache.SIsMember(key, patternStr); exists {
			return models.Value{
				Type:  "array",
				Array: []models.Value{{Type: "bulk", Bulk: patternStr}},
			}
		}
		return models.Value{
			Type:  "array",
			Array: []models.Value{},
		}
	}

	// Get matching members
	members, err := h.cache.SMembersPattern(key, patternStr)
	if err != nil {
		return models.Value{
			Type: "error",
			Str:  err.Error(),
		}
	}

	// Convert results to Value array
	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{
			Type: "bulk",
			Bulk: member,
		}
	}

	return models.Value{
		Type:  "array",
		Array: result,
	}
}
