package util

import (
	"math"
	"strings"
	"testing"
)

func TestRandomInt(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{"small range", 5},
		{"medium range", 100},
		{"large range", 1000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				result := RandomInt(tt.n)
				if result < 0 || result >= tt.n {
					t.Errorf("RandomInt(%d) = %d; want value in range [0,%d)", tt.n, result, tt.n)
				}
			}
		})
	}
}

func TestRandomIntRange(t *testing.T) {
	tests := []struct {
		name string
		min  int
		max  int
	}{
		{"small range", 0, 5},
		{"negative range", -10, -5},
		{"crossing zero", -5, 5},
		{"large range", 1000, 1000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				result := RandomIntRange(tt.min, tt.max)
				if result < tt.min || result >= tt.max {
					t.Errorf("RandomIntRange(%d, %d) = %d; want value in range [%d,%d)",
						tt.min, tt.max, result, tt.min, tt.max)
				}
			}
		})
	}

	t.Run("panic on invalid range", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("RandomIntRange(5, 5) did not panic")
			}
		}()
		RandomIntRange(5, 5)
	})
}

func TestRandomFloat64(t *testing.T) {
	for i := 0; i < 1000; i++ {
		result := RandomFloat64()
		if result < 0 || result >= 1 {
			t.Errorf("RandomFloat64() = %f; want value in range [0.0,1.0)", result)
		}
	}
}

func TestRandomFloat64Range(t *testing.T) {
	tests := []struct {
		name string
		min  float64
		max  float64
	}{
		{"small range", 0.0, 1.0},
		{"negative range", -10.5, -5.5},
		{"crossing zero", -5.5, 5.5},
		{"large range", 1000.0, 1000000.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				result := RandomFloat64Range(tt.min, tt.max)
				if result < tt.min || result >= tt.max {
					t.Errorf("RandomFloat64Range(%f, %f) = %f; want value in range [%f,%f)",
						tt.min, tt.max, result, tt.min, tt.max)
				}
			}
		})
	}

	t.Run("panic on invalid range", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("RandomFloat64Range(5.0, 5.0) did not panic")
			}
		}()
		RandomFloat64Range(5.0, 5.0)
	})
}

func TestRandomString(t *testing.T) {
	tests := []struct {
		name    string
		length  int
		charset string
	}{
		{"lowercase only", 10, Lowercase},
		{"uppercase only", 10, Uppercase},
		{"digits only", 10, Digits},
		{"alphanumeric", 10, AlphaNumeric},
		{"custom charset", 10, "!@#$%"},
		{"empty string", 0, Lowercase},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RandomString(tt.length, tt.charset)
			if len(result) != tt.length {
				t.Errorf("RandomString(%d, %s) length = %d; want %d",
					tt.length, tt.charset, len(result), tt.length)
			}
			for _, c := range result {
				if !strings.ContainsRune(tt.charset, c) {
					t.Errorf("RandomString(%d, %s) contains invalid character: %c",
						tt.length, tt.charset, c)
				}
			}
		})
	}

	t.Run("panic on negative length", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("RandomString(-1, Lowercase) did not panic")
			}
		}()
		RandomString(-1, Lowercase)
	})

	t.Run("panic on empty charset", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("RandomString(10, \"\") did not panic")
			}
		}()
		RandomString(10, "")
	})
}

func TestRandomBool(t *testing.T) {
	trueCount := 0
	iterations := 10000

	for i := 0; i < iterations; i++ {
		if RandomBool() {
			trueCount++
		}
	}

	// Check if the distribution is roughly even (within 5% of 50%)
	percentage := float64(trueCount) / float64(iterations)
	if math.Abs(percentage-0.5) > 0.05 {
		t.Errorf("RandomBool() distribution = %.2f; want approximately 0.50", percentage)
	}
}

func TestRandomElement(t *testing.T) {
	t.Run("int slice", func(t *testing.T) {
		slice := []int{1, 2, 3, 4, 5}
		seen := make(map[int]bool)

		for i := 0; i < 100; i++ {
			result := RandomElement(slice)
			seen[result] = true
		}

		// Check if all elements were selected at least once
		if len(seen) != len(slice) {
			t.Error("RandomElement didn't select all possible elements over multiple iterations")
		}
	})

	t.Run("panic on empty slice", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("RandomElement([]int{}) did not panic")
			}
		}()
		RandomElement([]int{})
	})
}

func TestShuffle(t *testing.T) {
	original := []int{1, 2, 3, 4, 5}
	iterations := 1000
	unchanged := 0

	for i := 0; i < iterations; i++ {
		test := make([]int, len(original))
		copy(test, original)

		Shuffle(test)

		// Count how many times the slice remains unchanged
		same := true
		for j := range test {
			if test[j] != original[j] {
				same = false
				break
			}
		}
		if same {
			unchanged++
		}
	}

	// The probability of the slice remaining unchanged should be very low
	if unchanged > iterations/20 { // Allow up to 5% unchanged
		t.Errorf("Shuffle() left the slice unchanged %d/%d times", unchanged, iterations)
	}
}
