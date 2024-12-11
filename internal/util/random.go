package util

import (
	"math/rand"
)

// RandomInt returns a random integer in range [0,n)
func RandomInt(n int) int {
	return rand.Intn(n)
}

// RandomIntRange returns a random integer in range [min,max)
func RandomIntRange(min, max int) int {
	if min >= max {
		panic("RandomIntRange: min must be less than max")
	}
	return min + rand.Intn(max-min)
}

// RandomFloat64 returns a random float64 in range [0.0,1.0)
func RandomFloat64() float64 {
	return rand.Float64()
}

// RandomFloat64Range returns a random float64 in range [min,max)
func RandomFloat64Range(min, max float64) float64 {
	if min >= max {
		panic("RandomFloat64Range: min must be less than max")
	}
	return min + rand.Float64()*(max-min)
}

// RandomString returns a random string of specified length using the given charset
func RandomString(length int, charset string) string {
	if length < 0 {
		panic("RandomString: length must be non-negative")
	}
	if len(charset) == 0 {
		panic("RandomString: charset must not be empty")
	}

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// Common character sets for RandomString
const (
	Lowercase    = "abcdefghijklmnopqrstuvwxyz"
	Uppercase    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	Digits       = "0123456789"
	AlphaNumeric = Lowercase + Uppercase + Digits
)

// RandomBool returns a random boolean value
func RandomBool() bool {
	return rand.Intn(2) == 1
}

// RandomElement returns a random element from the given slice
func RandomElement[T any](slice []T) T {
	if len(slice) == 0 {
		panic("RandomElement: slice must not be empty")
	}
	return slice[rand.Intn(len(slice))]
}

// Shuffle randomly reorders the elements in the slice
func Shuffle[T any](slice []T) {
	rand.Shuffle(len(slice), func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})
}
