package util

import (
	"testing"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func TestValidateArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []models.Value
		count   int
		wantErr bool
	}{
		{
			name:    "valid number of arguments",
			args:    []models.Value{{}, {}},
			count:   2,
			wantErr: false,
		},
		{
			name:    "invalid number of arguments",
			args:    []models.Value{{}},
			count:   2,
			wantErr: true,
		},
		{
			name:    "no arguments",
			args:    []models.Value{},
			count:   1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateArgs(tt.args, tt.count); (err != nil) != tt.wantErr {
				t.Errorf("ValidateArgs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
