package resp

import (
	"fmt"
	"io"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type Writer struct {
	wr io.Writer
}

func NewWriter(wr io.Writer) *Writer {
	return &Writer{wr: wr}
}

func (w *Writer) Write(v models.Value) error {
	var err error
	switch v.Type {
	case "string":
		err = w.writeString(v.Str)
	case "error":
		err = w.writeError(v.Str)
	case "integer":
		err = w.writeInteger(v.Num)
	case "bulk":
		err = w.writeBulk(v.Bulk)
	case "null":
		err = w.writeNull()
	case "array":
		err = w.writeArray(v.Array)
	default:
		err = fmt.Errorf("unknown type: %s", v.Type)
	}
	return err
}

func (w *Writer) writeString(s string) error {
	_, err := fmt.Fprintf(w.wr, "+%s\r\n", s)
	return err
}

func (w *Writer) writeError(s string) error {
	_, err := fmt.Fprintf(w.wr, "-%s\r\n", s)
	return err
}

func (w *Writer) writeInteger(i int) error {
	_, err := fmt.Fprintf(w.wr, ":%d\r\n", i)
	return err
}

func (w *Writer) writeBulk(s string) error {
	_, err := fmt.Fprintf(w.wr, "$%d\r\n%s\r\n", len(s), s)
	return err
}

func (w *Writer) writeNull() error {
	_, err := fmt.Fprintf(w.wr, "$-1\r\n")
	return err
}

func (w *Writer) writeArray(array []models.Value) error {
	_, err := fmt.Fprintf(w.wr, "*%d\r\n", len(array))
	if err != nil {
		return err
	}

	for _, value := range array {
		err := w.Write(value)
		if err != nil {
			return err
		}
	}
	return nil
}
