package models

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type StreamGroup struct {
	Name            string
	Consumers       int64
	Pending         int64
	LastDeliveredID string
}

type StreamConsumer struct {
	Name     string
	Pending  int64
	IdleTime int64
}

type StreamInfo struct {
	Length          int64
	RadixTreeKeys   int64
	RadixTreeNodes  int64
	Groups          int64
	LastGeneratedID string
	FirstEntry      *StreamEntry
	LastEntry       *StreamEntry
}
