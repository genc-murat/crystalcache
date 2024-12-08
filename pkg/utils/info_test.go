package util_test

import (
	"sort"
	"strings"
	"testing"

	util "github.com/genc-murat/crystalcache/pkg/utils"
)

func TestFormatInfoResponse(t *testing.T) {
	tests := []struct {
		info map[string]string
		want string
	}{
		{
			info: map[string]string{
				"version": "1.0.0",
				"name":    "TestApp",
				"status":  "running",
			},
			want: "name:TestApp\r\nstatus:running\r\nversion:1.0.0\r\n",
		},
		{
			info: map[string]string{
				"alpha": "beta",
				"gamma": "delta",
			},
			want: "alpha:beta\r\ngamma:delta\r\n",
		},
		{
			info: map[string]string{
				"key": "value",
			},
			want: "key:value\r\n",
		},
		{
			info: map[string]string{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(strings.Join(keysFromMap(tt.info), ","), func(t *testing.T) {
			got := util.FormatInfoResponse(tt.info)
			if got != tt.want {
				t.Errorf("FormatInfoResponse(%v) = %q; want %q", tt.info, got, tt.want)
			}
		})
	}
}

// keysFromMap retrieves keys from a map as a sorted slice
func keysFromMap(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
