package models

import "fmt"

type Value struct {
	Double    float64
	Map       map[string]Value
	Attribute map[string]Value
	Array     []Value
	Set       []Value
	Blob      []byte
	Type      string
	Str       string
	Bulk      string
	BigNum    string
	Num       int
	Bool      bool
}

func (v Value) String() string {
	switch v.Type {
	case "string":
		return fmt.Sprintf("String: %s", v.Str)
	case "error":
		return fmt.Sprintf("Error: %s", v.Str)
	case "integer":
		return fmt.Sprintf("Integer: %d", v.Num)
	case "bulk":
		return fmt.Sprintf("Bulk: %s", v.Bulk)
	case "null":
		return "Null"
	case "array":
		return fmt.Sprintf("Array: %v", v.Array)
	case "bool":
		return fmt.Sprintf("Boolean: %t", v.Bool)
	case "double":
		return fmt.Sprintf("Double: %f", v.Double)
	case "bignum":
		return fmt.Sprintf("Big Number: %s", v.BigNum)
	case "map":
		return fmt.Sprintf("Map: %v", v.Map)
	case "set":
		return fmt.Sprintf("Set: %v", v.Set)
	case "blob":
		return fmt.Sprintf("Blob: %v", v.Blob)
	case "verbatim":
		return fmt.Sprintf("Verbatim: %s", v.Str) // Assuming Str holds the content
	case "attribute":
		return fmt.Sprintf("Attribute: %v", v.Attribute)
	default:
		return fmt.Sprintf("Unknown Type: %s", v.Type)
	}
}

func (v Value) IsCommand(cmd string) bool {
	return v.Type == "array" && len(v.Array) > 0 && v.Array[0].Bulk == cmd
}
