package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"

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

	// Read the trailing \r\n
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
