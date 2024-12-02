package models

import "time"

type MemoryStats struct {
	TotalMemory     int64
	UsedMemory      int64
	FragmentedBytes int64
	LastDefrag      time.Time
	DefragCount     int64
}
