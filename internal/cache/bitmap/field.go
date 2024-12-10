package bitmap

import (
	"fmt"
	"strconv"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type FieldOps struct {
	basicOps *BasicOps
}

func NewFieldOps(basicOps *BasicOps) *FieldOps {
	return &FieldOps{
		basicOps: basicOps,
	}
}

// BitField executes multiple bitfield commands on a bitmap
func (f *FieldOps) BitField(key string, commands []models.BitFieldCommand) ([]int64, error) {
	results := make([]int64, len(commands))
	for i, cmd := range commands {
		var err error
		var val int64

		switch cmd.Op {
		case "GET":
			val, err = f.bitfieldGet(key, cmd.Type, cmd.Offset)
		case "SET":
			val, err = f.bitfieldSet(key, cmd.Type, cmd.Offset, cmd.Value)
		case "INCRBY":
			val, err = f.bitfieldIncrBy(key, cmd.Type, cmd.Offset, cmd.Increment)
		default:
			return nil, fmt.Errorf("ERR unknown bitfield command %s", cmd.Op)
		}

		if err != nil {
			return nil, err
		}
		results[i] = val
	}
	return results, nil
}

// BitFieldRO executes read-only bitfield operations
func (f *FieldOps) BitFieldRO(key string, commands []models.BitFieldCommand) ([]int64, error) {
	results := make([]int64, len(commands))
	for i, cmd := range commands {
		if cmd.Op != "GET" {
			return nil, fmt.Errorf("ERR BITFIELD_RO only supports GET operation")
		}
		val, err := f.bitfieldGet(key, cmd.Type, cmd.Offset)
		if err != nil {
			return nil, err
		}
		results[i] = val
	}
	return results, nil
}

// Helper methods

func (f *FieldOps) bitfieldGet(key string, typ string, offset int64) (int64, error) {
	bytes := f.basicOps.GetBitmap(key)
	if bytes == nil {
		return 0, nil
	}

	bits, signed, err := f.parseBitfieldType(typ)
	if err != nil {
		return 0, err
	}

	startByte := offset / 8
	endByte := (offset + int64(bits) + 7) / 8
	if startByte >= int64(len(bytes)) {
		return 0, nil
	}

	var result int64
	for i := startByte; i < endByte && i < int64(len(bytes)); i++ {
		bitOffset := 8 - uint((offset+(i-startByte)*8)%8)
		if bitOffset > 8 {
			bitOffset = 8
		}
		result = (result << bitOffset) | int64(bytes[i]>>(8-bitOffset))
	}

	mask := int64((1 << bits) - 1)
	result &= mask

	if signed && (result&(1<<(bits-1))) != 0 {
		result |= ^mask
	}

	return result, nil
}

func (f *FieldOps) bitfieldSet(key string, typ string, offset int64, value int64) (int64, error) {
	bits, _, err := f.parseBitfieldType(typ)
	if err != nil {
		return 0, err
	}

	bytes := f.basicOps.GetBitmap(key)
	endByte := (offset + int64(bits) + 7) / 8
	if bytes == nil || int64(len(bytes)) < endByte {
		newBytes := make([]byte, endByte)
		if bytes != nil {
			copy(newBytes, bytes)
		}
		bytes = newBytes
	}

	// Retrieve the old value
	oldValue, err := f.bitfieldGet(key, typ, offset)
	if err != nil {
		return 0, err
	}

	mask := int64((1 << bits) - 1)
	value &= mask

	startBit := offset % 8
	startByte := offset / 8

	for i := startByte; i < endByte; i++ {
		remainingBits := bits - int((i-startByte)*8)
		if remainingBits > 8 {
			remainingBits = 8
		}

		shift := remainingBits - int(8-startBit)
		if shift < 0 {
			shift = 0
		}

		byteMask := byte(mask >> ((i - startByte) * 8))
		bytes[i] &= ^(byteMask << startBit)
		bytes[i] |= byte(value>>(shift)) << startBit

		startBit = 0
	}

	f.basicOps.cache.Store(key, bytes)
	f.basicOps.incrementKeyVersion(key)

	return oldValue, nil
}

func (f *FieldOps) bitfieldIncrBy(key string, typ string, offset int64, increment int64) (int64, error) {
	current, err := f.bitfieldGet(key, typ, offset)
	if err != nil {
		return 0, err
	}

	bits, signed, err := f.parseBitfieldType(typ)
	if err != nil {
		return 0, err
	}

	result := current + increment

	if signed {
		max := int64(1<<(bits-1) - 1)
		min := -max - 1
		if result > max {
			result = max
		} else if result < min {
			result = min
		}
	} else {
		max := int64(1<<bits - 1)
		if result > max {
			result = max
		} else if result < 0 {
			result = 0
		}
	}

	_, err = f.bitfieldSet(key, typ, offset, result)
	if err != nil {
		return 0, err
	}

	return result, nil
}

func (f *FieldOps) parseBitfieldType(typ string) (bits int, signed bool, err error) {
	if len(typ) < 2 {
		return 0, false, fmt.Errorf("invalid bitfield type")
	}

	switch typ[0] {
	case 'i':
		signed = true
	case 'u':
		signed = false
	default:
		return 0, false, fmt.Errorf("invalid bitfield type")
	}

	bits, err = strconv.Atoi(typ[1:])
	if err != nil || bits <= 0 || bits > 64 {
		return 0, false, fmt.Errorf("invalid bitfield size")
	}

	return bits, signed, nil
}
