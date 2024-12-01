package models

// Transaction durumlarını tutacak struct
type Transaction struct {
	Commands []Command
	InMulti  bool
}

type Command struct {
	Name string
	Args []Value
}
