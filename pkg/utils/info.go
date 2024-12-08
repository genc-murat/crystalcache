package util

import (
	"sort"
	"strings"
)

// FormatInfoResponse takes a map of info key-values and returns a formatted string
func FormatInfoResponse(info map[string]string) string {
	var builder strings.Builder
	keys := make([]string, 0, len(info))
	for k := range info {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		builder.WriteString(k)
		builder.WriteString(":")
		builder.WriteString(info[k])
		builder.WriteString("\r\n")
	}
	return builder.String()
}
