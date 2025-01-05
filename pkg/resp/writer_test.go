package resp

import (
	"bytes"
	"testing"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func TestNewWriter(t *testing.T) {
	var buf bytes.Buffer
	writer := NewWriter(&buf)

	if writer == nil {
		t.Fatalf("expected non-nil Writer, got nil")
	}

	if writer.wr != &buf {
		t.Fatalf("expected writer.wr to be %v, got %v", &buf, writer.wr)
	}
}

func TestWriter_Write(t *testing.T) {
	tests := []struct {
		name    string
		value   models.Value
		want    string
		wantErr bool
	}{
		{
			name: "string value",
			value: models.Value{
				Type: "string",
				Str:  "hello",
			},
			want:    "+hello\r\n",
			wantErr: false,
		},
		{
			name: "integer value",
			value: models.Value{
				Type: "integer",
				Num:  123,
			},
			want:    ":123\r\n",
			wantErr: false,
		},
		{
			name: "unknown type",
			value: models.Value{
				Type: "unknown",
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWriter(&buf)

			err := writer.Write(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Writer.Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got := buf.String(); got != tt.want {
				t.Errorf("Writer.Write() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestWriter_writeStringValue(t *testing.T) {
	tests := []struct {
		name    string
		value   models.Value
		want    string
		wantErr bool
	}{
		{
			name: "valid string value",
			value: models.Value{
				Type: "string",
				Str:  "hello",
			},
			want:    "+hello\r\n",
			wantErr: false,
		},
		{
			name: "empty string value",
			value: models.Value{
				Type: "string",
				Str:  "",
			},
			want:    "+\r\n",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWriter(&buf)

			err := writer.writeStringValue(writer, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Writer.writeStringValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got := buf.String(); got != tt.want {
				t.Errorf("Writer.writeStringValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestWriter_writeErrorValue(t *testing.T) {
	tests := []struct {
		name    string
		value   models.Value
		want    string
		wantErr bool
	}{
		{
			name: "valid error value",
			value: models.Value{
				Type: "error",
				Str:  "some error",
			},
			want:    "-some error\r\n",
			wantErr: false,
		},
		{
			name: "empty error value",
			value: models.Value{
				Type: "error",
				Str:  "",
			},
			want:    "-\r\n",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWriter(&buf)

			err := writer.writeErrorValue(writer, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Writer.writeErrorValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got := buf.String(); got != tt.want {
				t.Errorf("Writer.writeErrorValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestWriter_writeIntegerValue(t *testing.T) {
	tests := []struct {
		name    string
		value   models.Value
		want    string
		wantErr bool
	}{
		{
			name: "valid integer value",
			value: models.Value{
				Type: "integer",
				Num:  123,
			},
			want:    ":123\r\n",
			wantErr: false,
		},
		{
			name: "zero integer value",
			value: models.Value{
				Type: "integer",
				Num:  0,
			},
			want:    ":0\r\n",
			wantErr: false,
		},
		{
			name: "negative integer value",
			value: models.Value{
				Type: "integer",
				Num:  -456,
			},
			want:    ":-456\r\n",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWriter(&buf)

			err := writer.writeIntegerValue(writer, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Writer.writeIntegerValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got := buf.String(); got != tt.want {
				t.Errorf("Writer.writeIntegerValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
