package models

type Transaction struct {
	Watches  map[string]int64
	Commands []Command
	InMulti  bool
}

type Command struct {
	Args []Value
	Name string
}
