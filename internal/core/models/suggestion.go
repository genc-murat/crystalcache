package models

// Suggestion represents an autocomplete suggestion entry
type Suggestion struct {
	Score   float64
	String  string
	Payload string
}

// SuggestionDict represents a dictionary of suggestions
type SuggestionDict struct {
	Entries map[string]*Suggestion
}

// NewSuggestionDict creates a new suggestion dictionary
func NewSuggestionDict() *SuggestionDict {
	return &SuggestionDict{
		Entries: make(map[string]*Suggestion),
	}
}
