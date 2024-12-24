package models

import "time"

type MemoryStats struct {
	TotalMemory      int64
	UsedMemory       int64
	FragmentedBytes  int64
	LastDefrag       time.Time
	DefragCount      int64
	HeapObjectsCount uint64
}

// HeapObjects returns the current number of heap objects.
func (ms *MemoryStats) HeapObjects() uint64 {
	return ms.HeapObjectsCount
}
