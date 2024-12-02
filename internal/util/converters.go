package util

import (
	"fmt"
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func FormatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func ParseInt(v models.Value) (int, error) {
	return strconv.Atoi(v.Bulk)
}

func ParseFloat(v models.Value) (float64, error) {
	return strconv.ParseFloat(v.Bulk, 64)
}

func ParseBool(v models.Value) (bool, error) {
	return strconv.ParseBool(v.Bulk)
}

func ToValue(val interface{}) models.Value {
	switch v := val.(type) {
	case string:
		return models.Value{Type: "bulk", Bulk: v}
	case int:
		return models.Value{Type: "integer", Num: v}
	case nil:
		return models.Value{Type: "null"}
	case error:
		return models.Value{Type: "error", Str: v.Error()}
	case []string:
		arr := make([]models.Value, len(v))
		for i, s := range v {
			arr[i] = models.Value{Type: "bulk", Bulk: s}
		}
		return models.Value{Type: "array", Array: arr}
	default:
		return models.Value{Type: "error", Str: fmt.Sprintf("unknown type: %T", val)}
	}
}
