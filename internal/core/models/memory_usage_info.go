package models

type MemoryUsageInfo struct {
	// Total memory usage in bytes
	TotalBytes int64

	// Data structure overhead in bytes
	OverheadBytes int64

	// Actual value size in bytes
	ValueBytes int64

	// Aligned total size (after memory alignment)
	AlignedBytes int64

	// Memory allocator overhead
	AllocatorOverhead int64

	// Pointer size on current architecture
	PointerSize int
}
