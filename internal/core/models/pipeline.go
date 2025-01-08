package models

type PipelineCommand struct {
	Args []Value
	Name string
}

type Pipeline struct {
	Commands []PipelineCommand
}
