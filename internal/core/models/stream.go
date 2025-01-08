package models

import "time"

type StreamEntry struct {
	Fields map[string]string
	ID     string
}

type StreamGroup struct {
	Consumers       int64
	Pending         int64
	Name            string
	LastDeliveredID string
}

type StreamConsumer struct {
	Pending  int64
	IdleTime int64
	Name     string
}
type StreamInfo struct {
	Length          int64
	RadixTreeKeys   int64
	RadixTreeNodes  int64
	Groups          int64
	FirstEntry      *StreamEntry
	LastEntry       *StreamEntry
	LastGeneratedID string
}

type StreamConsumerGroup struct {
	Consumers map[string]*StreamConsumer
	Pending   map[string]*PendingMessage
	LastID    string
}

type PendingMessage struct {
	Deliveries   int
	Consumer     string
	DeliveryTime time.Time
}
