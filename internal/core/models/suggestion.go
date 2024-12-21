package models

// Suggestion represents an autocomplete suggestion entry
type Suggestion struct {
	String  string
	Score   float64
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
