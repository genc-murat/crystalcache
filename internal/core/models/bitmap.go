package models

type BitFieldCommand struct {
	Offset    int64
	Value     int64
	Increment int64
	Op        string
	Type      string
}
