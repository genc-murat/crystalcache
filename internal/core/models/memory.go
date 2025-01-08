package models

import "time"

type MemoryStats struct {
	TotalMemory      int64
	UsedMemory       int64
	FragmentedBytes  int64
	DefragCount      int64
	HeapObjectsCount uint64
	LastDefrag       time.Time
}

// HeapObjects returns the current number of heap objects.
func (ms *MemoryStats) HeapObjects() uint64 {
	return ms.HeapObjectsCount
}
