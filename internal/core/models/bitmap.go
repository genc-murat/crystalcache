package models

type BitFieldCommand struct {
	Op        string
	Type      string
	Offset    int64
	Value     int64
	Increment int64
}
