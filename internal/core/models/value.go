package models

type Value struct {
	Type  string
	Str   string
	Num   int
	Bulk  string
	Array []Value
	Float float64
}

func (v Value) IsCommand(cmd string) bool {
	return v.Type == "array" && len(v.Array) > 0 && v.Array[0].Bulk == cmd
}
