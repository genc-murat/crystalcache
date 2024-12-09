// Add to internal/util/random.go

package util

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// RandomInt returns a random integer in range [0,n)
func RandomInt(n int) int {
	return rand.Intn(n)
}
