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
	case "bool":
		err = w.writeBoolean(v.Bool)
	case "double":
		err = w.writeDouble(v.Double)
	case "bignum":
		err = w.writeBigNumber(v.BigNum)
	case "map":
		err = w.writeMap(v.Map)
	case "set":
		err = w.writeSet(v.Set)
	case "blob":
		err = w.writeBlob(v.Blob)
	case "verbatim":
		err = w.writeVerbatimString(v.Str)
	case "attribute":
		// Write attributes as metadata, then the actual value
		err = w.writeAttribute(v.Attribute, v)
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

func (w *Writer) writeBoolean(b bool) error {
	if b {
		_, err := fmt.Fprintf(w.wr, "#t\r\n")
		return err
	}
	_, err := fmt.Fprintf(w.wr, "#f\r\n")
	return err
}

func (w *Writer) writeDouble(f float64) error {
	_, err := fmt.Fprintf(w.wr, ",%f\r\n", f)
	return err
}

func (w *Writer) writeBigNumber(bn string) error {
	_, err := fmt.Fprintf(w.wr, "(%s\r\n", bn)
	return err
}

func (w *Writer) writeVerbatimString(s string) error {
	_, err := fmt.Fprintf(w.wr, "=%d\r\ntxt:%s\r\n", len(s), s) // Assuming txt encoding
	return err
}

func (w *Writer) writeBlob(b []byte) error {
	_, err := fmt.Fprintf(w.wr, "_%d\r\n%s\r\n", len(b), string(b))
	return err
}

func (w *Writer) writeMap(m map[string]models.Value) error {
	_, err := fmt.Fprintf(w.wr, "%%%d\r\n", len(m))
	if err != nil {
		return err
	}
	for key, value := range m {
		err := w.writeString(key) // Maps keys are typically strings
		if err != nil {
			return err
		}
		err = w.Write(value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeSet(s []models.Value) error {
	_, err := fmt.Fprintf(w.wr, "~%d\r\n", len(s))
	if err != nil {
		return err
	}
	for _, value := range s {
		err := w.Write(value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeAttribute(attr map[string]models.Value, actualValue models.Value) error {
	_, err := fmt.Fprintf(w.wr, "|%d\r\n", len(attr))
	if err != nil {
		return err
	}
	for key, value := range attr {
		err := w.writeString(key)
		if err != nil {
			return err
		}
		err = w.Write(value)
		if err != nil {
			return err
		}
	}
	return w.Write(actualValue)
}
