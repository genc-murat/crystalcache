package models

type PipelineCommand struct {
	Name string
	Args []Value
}

type Pipeline struct {
	Commands []PipelineCommand
}
