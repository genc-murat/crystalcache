package handlers

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
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
