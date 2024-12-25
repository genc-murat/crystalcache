package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type Reader struct {
	rd *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{rd: bufio.NewReader(rd)}
}

func (r *Reader) Read() (models.Value, error) {
	typ, err := r.rd.ReadByte()
	if err != nil {
		return models.Value{}, err
	}

	switch typ {
	case '+':
		return r.readSimpleString()
	case '-':
		return r.readError()
	case ':':
		return r.readInteger()
	case '$':
		return r.readBulkString()
	case '*':
		return r.readArray()
	case '=':
		return r.readVerbatimString()
	case '_':
		return r.readBlob()
	case '#':
		return r.readBoolean()
	case ',':
		return r.readDouble()
	case '(':
		return r.readBigNumber()
	case '%':
		return r.readMap()
	case '~':
		return r.readSet()
	case '|':
		return r.readAttribute()
	default:
		return models.Value{}, fmt.Errorf("unknown type: %c", typ)
	}
}

func (r *Reader) readLine() ([]byte, error) {
	var line []byte
	for {
		b, err := r.rd.ReadByte()
		if err != nil {
			return nil, err
		}
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' && line[len(line)-1] == '\n' {
			return line[:len(line)-2], nil
		}
	}
}

func (r *Reader) readSimpleString() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}
	return models.Value{Type: "string", Str: string(line)}, nil
}

func (r *Reader) readError() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}
	return models.Value{Type: "error", Str: string(line)}, nil
}

func (r *Reader) readInteger() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}
	num, err := strconv.Atoi(string(line))
	if err != nil {
		return models.Value{}, err
	}
	return models.Value{Type: "integer", Num: num}, nil
}

func (r *Reader) readBulkString() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	length, err := strconv.Atoi(string(line))
	if err != nil {
		return models.Value{}, err
	}

	if length == -1 {
		return models.Value{Type: "null"}, nil
	}

	bulk := make([]byte, length)
	_, err = io.ReadFull(r.rd, bulk)
	if err != nil {
		return models.Value{}, err
	}

	_, err = r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	return models.Value{Type: "bulk", Bulk: string(bulk)}, nil
}

func (r *Reader) readArray() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	length, err := strconv.Atoi(string(line))
	if err != nil {
		return models.Value{}, err
	}

	if length == -1 {
		return models.Value{Type: "null"}, nil
	}

	array := make([]models.Value, length)
	for i := 0; i < length; i++ {
		value, err := r.Read()
		if err != nil {
			return models.Value{}, err
		}
		array[i] = value
	}

	return models.Value{Type: "array", Array: array}, nil
}

func (r *Reader) readVerbatimString() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	parts := strings.SplitN(string(line), ":", 2)
	if len(parts) != 2 {
		return models.Value{}, fmt.Errorf("invalid verbatim string format: %s", line)
	}
	length, err := strconv.Atoi(parts[0])
	if err != nil {
		return models.Value{}, err
	}

	bulk := make([]byte, length)
	_, err = io.ReadFull(r.rd, bulk)
	if err != nil {
		return models.Value{}, err
	}

	_, err = r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	return models.Value{Type: "verbatim", Str: string(bulk)}, nil
}

func (r *Reader) readBlob() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	length, err := strconv.Atoi(string(line))
	if err != nil {
		return models.Value{}, err
	}

	blob := make([]byte, length)
	_, err = io.ReadFull(r.rd, blob)
	if err != nil {
		return models.Value{}, err
	}

	_, err = r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	return models.Value{Type: "blob", Blob: blob}, nil
}

func (r *Reader) readBoolean() (models.Value, error) {
	b, err := r.rd.ReadByte()
	if err != nil {
		return models.Value{}, err
	}
	r.rd.Discard(2) // Discard the trailing \r\n
	if b == 't' {
		return models.Value{Type: "bool", Bool: true}, nil
	} else if b == 'f' {
		return models.Value{Type: "bool", Bool: false}, nil
	}
	return models.Value{}, fmt.Errorf("invalid boolean value: %c", b)
}

func (r *Reader) readDouble() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}
	f, err := strconv.ParseFloat(string(line), 64)
	if err != nil {
		return models.Value{}, err
	}
	return models.Value{Type: "double", Double: f}, nil
}

func (r *Reader) readBigNumber() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}
	return models.Value{Type: "bignum", BigNum: string(line)}, nil
}

func (r *Reader) readMap() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	length, err := strconv.Atoi(string(line))
	if err != nil {
		return models.Value{}, err
	}

	if length == -1 {
		return models.Value{Type: "null"}, nil
	}

	m := make(map[string]models.Value, length)
	for i := 0; i < length; i++ {
		keyVal, err := r.Read()
		if err != nil {
			return models.Value{}, err
		}
		if keyVal.Type != "string" && keyVal.Type != "bulk" && keyVal.Type != "verbatim" {
			return models.Value{}, fmt.Errorf("invalid map key type: %s", keyVal.Type)
		}
		valueVal, err := r.Read()
		if err != nil {
			return models.Value{}, err
		}
		m[keyVal.Str] = valueVal
	}

	return models.Value{Type: "map", Map: m}, nil
}

func (r *Reader) readSet() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	length, err := strconv.Atoi(string(line))
	if err != nil {
		return models.Value{}, err
	}

	if length == -1 {
		return models.Value{Type: "null"}, nil
	}

	s := make([]models.Value, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return models.Value{}, err
		}
		s[i] = val
	}

	return models.Value{Type: "set", Set: s}, nil
}

func (r *Reader) readAttribute() (models.Value, error) {
	line, err := r.readLine()
	if err != nil {
		return models.Value{}, err
	}

	length, err := strconv.Atoi(string(line))
	if err != nil {
		return models.Value{}, err
	}

	if length == -1 {
		return models.Value{Type: "null"}, nil
	}

	attr := make(map[string]models.Value, length)
	for i := 0; i < length; i++ {
		keyVal, err := r.Read()
		if err != nil {
			return models.Value{}, err
		}
		if keyVal.Type != "string" && keyVal.Type != "bulk" && keyVal.Type != "verbatim" {
			return models.Value{}, fmt.Errorf("invalid attribute key type: %s", keyVal.Type)
		}
		valueVal, err := r.Read()
		if err != nil {
			return models.Value{}, err
		}
		attr[keyVal.Str] = valueVal
	}

	// Read the actual value after attributes
	actualValue, err := r.Read()
	if err != nil {
		return models.Value{}, err
	}
	actualValue.Type = "attribute"
	actualValue.Attribute = attr
	return actualValue, nil
}
