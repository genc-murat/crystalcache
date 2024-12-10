package bitmap

import (
	"fmt"
	"strings"
)

type BitwiseOps struct {
	basicOps *BasicOps
}

func NewBitwiseOps(basicOps *BasicOps) *BitwiseOps {
	return &BitwiseOps{
		basicOps: basicOps,
	}
}

// BitOp performs a bitwise operation on multiple bitmaps
func (b *BitwiseOps) BitOp(operation string, destkey string, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'bitop' command")
	}

	var result []byte
	switch strings.ToUpper(operation) {
	case "AND":
		result = b.bitopAND(keys...)
	case "OR":
		result = b.bitopOR(keys...)
	case "XOR":
		result = b.bitopXOR(keys...)
	case "NOT":
		if len(keys) != 1 {
			return 0, fmt.Errorf("ERR BITOP NOT must be called with a single source key")
		}
		result = b.bitopNOT(keys[0])
	default:
		return 0, fmt.Errorf("ERR unknown operation '%s'", operation)
	}

	b.basicOps.cache.Store(destkey, result)
	b.basicOps.incrementKeyVersion(destkey)
	return int64(len(result)), nil
}

func (b *BitwiseOps) bitopNOT(key string) []byte {
	val, exists := b.basicOps.cache.Load(key)
	if !exists {
		return nil
	}

	bytes := val.([]byte)
	result := make([]byte, len(bytes))
	for i := 0; i < len(bytes); i++ {
		result[i] = ^bytes[i]
	}
	return result
}

func (b *BitwiseOps) bitopAND(keys ...string) []byte {
	var maxLen int
	values := make([][]byte, 0, len(keys))

	for _, key := range keys {
		if val, exists := b.basicOps.cache.Load(key); exists {
			bytes := val.([]byte)
			if len(bytes) > maxLen {
				maxLen = len(bytes)
			}
			values = append(values, bytes)
		}
	}

	if len(values) == 0 {
		return nil
	}

	result := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		result[i] = 0xFF
		for _, val := range values {
			if i < len(val) {
				result[i] &= val[i]
			} else {
				result[i] = 0
				break
			}
		}
	}
	return result
}

func (b *BitwiseOps) bitopOR(keys ...string) []byte {
	var maxLen int
	values := make([][]byte, 0, len(keys))

	for _, key := range keys {
		if val, exists := b.basicOps.cache.Load(key); exists {
			bytes := val.([]byte)
			if len(bytes) > maxLen {
				maxLen = len(bytes)
			}
			values = append(values, bytes)
		}
	}

	if len(values) == 0 {
		return nil
	}

	result := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		for _, val := range values {
			if i < len(val) {
				result[i] |= val[i]
			}
		}
	}
	return result
}

func (b *BitwiseOps) bitopXOR(keys ...string) []byte {
	var maxLen int
	values := make([][]byte, 0, len(keys))

	for _, key := range keys {
		if val, exists := b.basicOps.cache.Load(key); exists {
			bytes := val.([]byte)
			if len(bytes) > maxLen {
				maxLen = len(bytes)
			}
			values = append(values, bytes)
		}
	}

	if len(values) == 0 {
		return nil
	}

	result := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		for j, val := range values {
			if i < len(val) {
				if j == 0 {
					result[i] = val[i]
				} else {
					result[i] ^= val[i]
				}
			}
		}
	}
	return result
}
