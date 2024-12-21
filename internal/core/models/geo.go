package models

// GeoPoint represents a geographical coordinate with additional metadata
type GeoPoint struct {
	Longitude float64
	Latitude  float64
	Name      string
	Distance  float64 // Used for radius queries
	GeoHash   string
}

// GeoSearchOptions represents search criteria for GeoSearch operations
type GeoSearchOptions struct {
	FromMember string  // Search from existing member
	FromLon    float64 // Search from longitude
	FromLat    float64 // Search from latitude
	ByBox      bool    // Whether to search within a box
	BoxWidth   float64 // Box width
	BoxHeight  float64 // Box height
	ByRadius   bool    // Whether to search within a radius
	Radius     float64 // Search radius
	Unit       string  // Distance unit (m, km, mi, ft)
	WithCoord  bool    // Include coordinates in results
	WithDist   bool    // Include distances in results
	WithHash   bool    // Include geohash in results
	Count      int     // Limit number of results
	CountAny   bool    // Return any N items when using COUNT
	Sort       string  // Sort order (ASC or DESC)
}
