package models

type Transaction struct {
	Commands []Command
	InMulti  bool
	Watches  map[string]int64
}

type Command struct {
	Name string
	Args []Value
}
