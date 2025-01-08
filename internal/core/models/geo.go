package models

// GeoPoint represents a geographical coordinate with additional metadata
type GeoPoint struct {
	Longitude float64
	Latitude  float64
	Distance  float64
	Name      string
	GeoHash   string
}

// GeoSearchOptions represents search criteria for GeoSearch operations
type GeoSearchOptions struct {
	FromLon    float64
	FromLat    float64
	BoxWidth   float64
	BoxHeight  float64
	Radius     float64
	FromMember string
	Unit       string
	Sort       string
	Count      int
	ByBox      bool
	ByRadius   bool
	WithCoord  bool
	WithDist   bool
	WithHash   bool
	CountAny   bool
}
