package hash

import (
	"encoding/binary"
)

// Constants for MurmurHash3 128-bit x64
const (
	c1 = 0x87c37b91114253d5
	c2 = 0x4cf5ad432745937f
)

// Murmur3 implements 128-bit MurmurHash3 algorithm for x64 architecture
// Reference: https://github.com/aappleby/smhasher/wiki/MurmurHash3
func Murmur3(data []byte) (uint64, uint64) {
	length := len(data)
	h1, h2 := uint64(0x9368e53c2f6af274), uint64(0x586dcd208f7cd3fd)

	// Body
	nblocks := length / 16
	for i := 0; i < nblocks; i++ {
		k1 := binary.LittleEndian.Uint64(data[i*16:])
		k2 := binary.LittleEndian.Uint64(data[i*16+8:])

		k1 = mixK1(k1)
		h1 = mixH1(h1, k1)

		k2 = mixK2(k2)
		h2 = mixH2(h2, k2)
	}

	// Tail
	tail := data[nblocks*16:]
	if len(tail) > 0 {
		h1, h2 = processTail(tail, h1, h2)
	}

	// Finalization
	h1, h2 = finalize(h1, h2, uint64(length))

	return h1, h2
}

// Helper functions for cleaner implementation

func mixK1(k1 uint64) uint64 {
	k1 *= c1
	k1 = rotl64(k1, 31)
	k1 *= c2
	return k1
}

func mixH1(h1, k1 uint64) uint64 {
	h1 ^= k1
	h1 = rotl64(h1, 27)
	h1 = h1*5 + 0x52dce729
	return h1
}

func mixK2(k2 uint64) uint64 {
	k2 *= c2
	k2 = rotl64(k2, 33)
	k2 *= c1
	return k2
}

func mixH2(h2, k2 uint64) uint64 {
	h2 ^= k2
	h2 = rotl64(h2, 31)
	h2 = h2*5 + 0x38495ab5
	return h2
}

func processTail(tail []byte, h1, h2 uint64) (uint64, uint64) {
	var k1, k2 uint64
	switch len(tail) & 15 {
	case 15:
		k2 ^= uint64(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= uint64(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= uint64(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= uint64(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= uint64(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= uint64(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= uint64(tail[8])
		k2 = mixK2(k2)
		h2 ^= k2
		fallthrough
	case 8:
		k1 ^= uint64(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint64(tail[0])
		k1 = mixK1(k1)
		h1 ^= k1
	}
	return h1, h2
}

func finalize(h1, h2 uint64, length uint64) (uint64, uint64) {
	h1 ^= length
	h2 ^= length

	h1 += h2
	h2 += h1

	h1 = fmix64(h1)
	h2 = fmix64(h2)

	h1 += h2
	h2 += h1

	return h1, h2
}

// Bit manipulation utilities

func rotl64(x uint64, r uint8) uint64 {
	return (x << r) | (x >> (64 - r))
}

func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}

// Convenience methods for common use cases

// Hash64 returns a single 64-bit hash value, using only the first half
// of the 128-bit murmur3 hash. This is useful when you need a faster,
// lower-quality hash.
func Hash64(data []byte) uint64 {
	h1, _ := Murmur3(data)
	return h1
}

// Hash128 returns the full 128-bit hash as a pair of uint64 values.
// This is the recommended method for maximum hash quality.
func Hash128(data []byte) [2]uint64 {
	h1, h2 := Murmur3(data)
	return [2]uint64{h1, h2}
}
