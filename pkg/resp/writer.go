package resp

import (
	"fmt"
	"io"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type Writer struct {
	wr io.Writer
}

type writerFunc func(w *Writer, v models.Value) error

func NewWriter(wr io.Writer) *Writer {
	return &Writer{wr: wr}
}

func (w *Writer) Write(v models.Value) error {
	writerFn, ok := w.getWriterFunc(v.Type)
	if !ok {
		return fmt.Errorf("unknown type: %s", v.Type)
	}
	return writerFn(w, v)
}

func (w *Writer) getWriterFunc(valueType string) (writerFunc, bool) {
	switch valueType {
	case "string":
		return w.writeStringValue, true
	case "error":
		return w.writeErrorValue, true
	case "integer":
		return w.writeIntegerValue, true
	case "bulk":
		return w.writeBulkValue, true
	case "null":
		return w.writeNullValue, true
	case "array":
		return w.writeArrayValue, true
	case "bool":
		return w.writeBooleanValue, true
	case "double":
		return w.writeDoubleValue, true
	case "bignum":
		return w.writeBigNumberValue, true
	case "map":
		return w.writeMapValue, true
	case "set":
		return w.writeSetValue, true
	case "blob":
		return w.writeBlobValue, true
	case "verbatim":
		return w.writeVerbatimStringValue, true
	case "attribute":
		return w.writeAttributeValue, true
	default:
		return nil, false
	}
}

func (w *Writer) writeStringValue(wr *Writer, v models.Value) error {
	return wr.writeString(v.Str)
}

func (w *Writer) writeErrorValue(wr *Writer, v models.Value) error {
	return wr.writeError(v.Str)
}

func (w *Writer) writeIntegerValue(wr *Writer, v models.Value) error {
	return wr.writeInteger(v.Num)
}

func (w *Writer) writeBulkValue(wr *Writer, v models.Value) error {
	return wr.writeBulk(v.Bulk)
}

func (w *Writer) writeNullValue(wr *Writer, v models.Value) error {
	return wr.writeNull()
}

func (w *Writer) writeArrayValue(wr *Writer, v models.Value) error {
	return wr.writeArray(v.Array)
}

func (w *Writer) writeBooleanValue(wr *Writer, v models.Value) error {
	return wr.writeBoolean(v.Bool)
}

func (w *Writer) writeDoubleValue(wr *Writer, v models.Value) error {
	return wr.writeDouble(v.Double)
}

func (w *Writer) writeBigNumberValue(wr *Writer, v models.Value) error {
	return wr.writeBigNumber(v.BigNum)
}

func (w *Writer) writeMapValue(wr *Writer, v models.Value) error {
	return wr.writeMap(v.Map)
}

func (w *Writer) writeSetValue(wr *Writer, v models.Value) error {
	return wr.writeSet(v.Set)
}

func (w *Writer) writeBlobValue(wr *Writer, v models.Value) error {
	return wr.writeBlob(v.Blob)
}

func (w *Writer) writeVerbatimStringValue(wr *Writer, v models.Value) error {
	return wr.writeVerbatimString(v.Str)
}

func (w *Writer) writeAttributeValue(wr *Writer, v models.Value) error {
	return wr.writeAttribute(v.Attribute, v)
}

func (w *Writer) writeString(s string) error {
	return w.writeFormat("+%s\r\n", s)
}

func (w *Writer) writeError(s string) error {
	return w.writeFormat("-%s\r\n", s)
}

func (w *Writer) writeInteger(i int) error {
	return w.writeFormat(":%d\r\n", i)
}

func (w *Writer) writeBulk(s string) error {
	return w.writeFormat("$%d\r\n%s\r\n", len(s), s)
}

func (w *Writer) writeNull() error {
	n, err := w.wr.Write([]byte("$-1\r\n"))
	if err != nil {
		return err
	}
	expected := len("$-1\r\n")
	if n < expected {
		return fmt.Errorf("short write in writeNull: wrote %d bytes, expected %d", n, expected)
	}
	return nil
}

func (w *Writer) writeArray(array []models.Value) error {
	if err := w.writeFormat("*%d\r\n", len(array)); err != nil {
		return err
	}
	for _, value := range array {
		if err := w.Write(value); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeBoolean(b bool) error {
	if b {
		return w.writeFormat("#t\r\n")
	}
	return w.writeFormat("#f\r\n")
}

func (w *Writer) writeDouble(f float64) error {
	return w.writeFormat(",%f\r\n", f)
}

func (w *Writer) writeBigNumber(bn string) error {
	return w.writeFormat("(%s\r\n", bn)
}

func (w *Writer) writeVerbatimString(s string) error {
	return w.writeFormat("=%d\r\ntxt:%s\r\n", len(s), s)
}

func (w *Writer) writeBlob(b []byte) error {
	return w.writeFormat("_%d\r\n%s\r\n", len(b), string(b))
}

func (w *Writer) writeMap(m map[string]models.Value) error {
	if err := w.writeFormat("%%%d\r\n", len(m)); err != nil {
		return err
	}
	for key, value := range m {
		if err := w.writeString(key); err != nil {
			return err
		}
		if err := w.Write(value); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeSet(s []models.Value) error {
	if err := w.writeFormat("~%d\r\n", len(s)); err != nil {
		return err
	}
	for _, value := range s {
		if err := w.Write(value); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeAttribute(attr map[string]models.Value, actualValue models.Value) error {
	if err := w.writeFormat("|%d\r\n", len(attr)); err != nil {
		return err
	}
	for key, value := range attr {
		if err := w.writeString(key); err != nil {
			return err
		}
		if err := w.Write(value); err != nil {
			return err
		}
	}
	return w.Write(actualValue)
}

func (w *Writer) writeFormat(format string, a ...interface{}) error {
	n, err := fmt.Fprintf(w.wr, format, a...)
	if err != nil {
		return err
	}
	expected := len(fmt.Sprintf(format, a...))
	if n < expected {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, expected)
	}
	return nil
}
