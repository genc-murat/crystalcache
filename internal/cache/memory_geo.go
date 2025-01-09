package cache

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// GeoAdd adds one or more GeoPoint items to the in-memory cache under the specified key.
// It returns the number of items successfully added and an error if any occurred.
//
// Parameters:
//   - key: A string representing the key under which the GeoPoint items will be stored.
//   - items: A variadic parameter of GeoPoint items to be added to the cache.
//
// Returns:
//   - int: The number of GeoPoint items successfully added to the cache.
//   - error: An error if any occurred during the operation.
//
// The function performs the following steps:
//  1. Loads or stores a new sync.Map for the given key in the geoData map.
//  2. Iterates over the provided GeoPoint items.
//  3. Validates the coordinates of each item.
//  4. Encodes the longitude and latitude into a GeoHash and stores the item in the sync.Map.
//  5. Increments the key version if any items were added.
func (c *MemoryCache) GeoAdd(key string, items ...models.GeoPoint) (int, error) {
	geoSetI, _ := c.geoData.LoadOrStore(key, &sync.Map{})
	geoSet := geoSetI.(*sync.Map)

	added := 0
	for _, item := range items {
		if !isValidCoordinate(item.Longitude, item.Latitude) {
			continue
		}

		item.GeoHash = encodeGeoHash(item.Longitude, item.Latitude)
		geoSet.Store(item.Name, &item)
		added++
	}

	if added > 0 {
		c.incrementKeyVersion(key)
	}

	return added, nil
}

// encodeGeoHash encodes the given longitude and latitude into a GeoHash string.
// The GeoHash is a string representation of the geographic location, using base32 encoding.
// The function returns an 11-character GeoHash string.
//
// Parameters:
//   - lon: The longitude of the location to encode.
//   - lat: The latitude of the location to encode.
//
// Returns:
//   - A string representing the GeoHash of the given longitude and latitude.
func encodeGeoHash(lon, lat float64) string {
	const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
	var hash strings.Builder

	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0
	isEven := true

	bit := 0
	ch := 0

	for hash.Len() < 11 {
		if isEven {
			mid := (minLon + maxLon) / 2
			if lon >= mid {
				ch |= 1 << (4 - bit)
				minLon = mid
			} else {
				maxLon = mid
			}
		} else {
			mid := (minLat + maxLat) / 2
			if lat >= mid {
				ch |= 1 << (4 - bit)
				minLat = mid
			} else {
				maxLat = mid
			}
		}

		isEven = !isEven
		bit++

		if bit == 5 {
			hash.WriteByte(base32[ch])
			bit = 0
			ch = 0
		}
	}

	return hash.String()
}

// GeoDist calculates the distance between two members of a geospatial index represented by the given key.
// The distance is calculated using the Haversine formula and returned in the specified unit.
//
// Parameters:
//   - key: The key of the geospatial index.
//   - member1: The first member whose coordinates are used for distance calculation.
//   - member2: The second member whose coordinates are used for distance calculation.
//   - unit: The unit of measurement for the distance (e.g., "m" for meters, "km" for kilometers).
//
// Returns:
//   - float64: The calculated distance between the two members in the specified unit.
//   - error: An error if the key or members are not found, or if any other issue occurs during calculation.
func (c *MemoryCache) GeoDist(key, member1, member2, unit string) (float64, error) {
	geoSetI, exists := c.geoData.Load(key)
	if !exists {
		return 0, fmt.Errorf("ERR key not found")
	}
	geoSet := geoSetI.(*sync.Map)

	point1I, exists1 := geoSet.Load(member1)
	point2I, exists2 := geoSet.Load(member2)

	if !exists1 || !exists2 {
		return 0, fmt.Errorf("ERR member not found")
	}

	point1 := point1I.(*models.GeoPoint)
	point2 := point2I.(*models.GeoPoint)

	dist := calculateDistance(point1.Longitude, point1.Latitude, point2.Longitude, point2.Latitude)
	return convertDistance(dist, unit), nil
}

// GeoPos retrieves the geographical positions of the specified members from the cache.
//
// Parameters:
//   - key: The key identifying the geo set in the cache.
//   - members: A variadic list of member names whose geographical positions are to be retrieved.
//
// Returns:
//   - A slice of pointers to GeoPoint objects corresponding to the specified members. If a member
//     does not exist in the geo set, its position in the slice will be nil.
//   - An error if any issue occurs during the retrieval process. If the key does not exist in the
//     cache, both the slice and error will be nil.
func (c *MemoryCache) GeoPos(key string, members ...string) ([]*models.GeoPoint, error) {
	geoSetI, exists := c.geoData.Load(key)
	if !exists {
		return nil, nil
	}
	geoSet := geoSetI.(*sync.Map)

	results := make([]*models.GeoPoint, len(members))
	for i, member := range members {
		if pointI, exists := geoSet.Load(member); exists {
			point := pointI.(*models.GeoPoint)
			results[i] = point
		}
	}

	return results, nil
}

// isValidCoordinate checks if the given longitude and latitude values
// are within the valid ranges for geographic coordinates.
// Longitude must be between -180 and 180 degrees.
// Latitude must be between -85.05112878 and 85.05112878 degrees.
//
// Parameters:
//
//	lon - Longitude value to be checked.
//	lat - Latitude value to be checked.
//
// Returns:
//
//	bool - true if both longitude and latitude are within valid ranges, false otherwise.
func isValidCoordinate(lon, lat float64) bool {
	return lon >= -180 && lon <= 180 && lat >= -85.05112878 && lat <= 85.05112878
}

// calculateDistance calculates the distance between two points on the Earth's surface
// specified by their longitude and latitude in decimal degrees using the Haversine formula.
//
// Parameters:
//   - lon1: Longitude of the first point in decimal degrees.
//   - lat1: Latitude of the first point in decimal degrees.
//   - lon2: Longitude of the second point in decimal degrees.
//   - lat2: Latitude of the second point in decimal degrees.
//
// Returns:
//   - The distance between the two points in meters.
func calculateDistance(lon1, lat1, lon2, lat2 float64) float64 {
	const earthRadius = 6371000 // Earth's radius in meters

	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	lonDiff := (lon2 - lon1) * math.Pi / 180
	latDiff := (lat2 - lat1) * math.Pi / 180

	a := math.Sin(latDiff/2)*math.Sin(latDiff/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(lonDiff/2)*math.Sin(lonDiff/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadius * c
}

// convertToMeters converts a given distance to meters based on the specified unit.
// Supported units are kilometers ("km"), miles ("mi"), feet ("ft"), and meters ("m").
// If the unit is not recognized, the function assumes the distance is already in meters.
//
// Parameters:
//   - distance: The distance to be converted.
//   - unit: The unit of the distance (e.g., "km", "mi", "ft", "m").
//
// Returns:
//
//	The distance converted to meters.
func convertToMeters(distance float64, unit string) float64 {
	switch strings.ToLower(unit) {
	case "km":
		return distance * 1000
	case "mi":
		return distance * 1609.34
	case "ft":
		return distance / 3.28084
	default: // "m"
		return distance
	}
}

// convertDistance converts a distance from meters to the specified unit.
// Supported units are:
// - "km" for kilometers
// - "mi" for miles
// - "ft" for feet
// If the unit is not recognized, the function returns the distance in meters.
//
// Parameters:
// - meters: the distance in meters to be converted.
// - unit: the unit to convert the distance to.
//
// Returns:
// - The converted distance in the specified unit.
func convertDistance(meters float64, unit string) float64 {
	switch strings.ToLower(unit) {
	case "km":
		return meters / 1000
	case "mi":
		return meters / 1609.34
	case "ft":
		return meters * 3.28084
	default: // "m"
		return meters
	}
}

// sortGeoResults sorts a slice of GeoPoint results based on the specified sortOrder.
// If sortOrder is "ASC", the results are sorted in ascending order of Distance.
// If sortOrder is "DESC", the results are sorted in descending order of Distance.
//
// Parameters:
//   - results: A slice of GeoPoint objects to be sorted.
//   - sortOrder: A string indicating the sort order, either "ASC" for ascending or "DESC" for descending.
func sortGeoResults(results []models.GeoPoint, sortOrder string) {
	if sortOrder == "ASC" {
		sort.SliceStable(results, func(i, j int) bool {
			return results[i].Distance < results[j].Distance
		})
	} else if sortOrder == "DESC" {
		sort.SliceStable(results, func(i, j int) bool {
			return results[i].Distance > results[j].Distance
		})
	}
}

// GeoRadius retrieves geo points within a specified radius from a given location.
// It searches for geo points stored under the provided key and returns those within the specified radius.
//
// Parameters:
//   - key: The key under which the geo points are stored.
//   - longitude: The longitude of the center point.
//   - latitude: The latitude of the center point.
//   - radius: The radius within which to search for geo points.
//   - unit: The unit of the radius (e.g., "m" for meters, "km" for kilometers).
//   - withDist: If true, includes the distance of each point from the center.
//   - withCoord: If true, includes the coordinates of each point.
//   - withHash: If true, includes the hash of each point.
//   - count: The maximum number of results to return. If 0, returns all results.
//   - sortOption: The sorting option for the results ("ASC" for ascending, "DESC" for descending).
//
// Returns:
//   - A slice of GeoPoint objects that are within the specified radius.
//   - An error if any issues occur during the retrieval process.
func (c *MemoryCache) GeoRadius(key string, longitude, latitude, radius float64, unit string, withDist, withCoord, withHash bool, count int, sortOption string) ([]models.GeoPoint, error) {
	geoSetI, exists := c.geoData.Load(key)
	if !exists {
		return nil, nil
	}
	geoSet := geoSetI.(*sync.Map)

	radiusM := convertToMeters(radius, unit)
	var results []models.GeoPoint

	geoSet.Range(func(_, pointI interface{}) bool {
		point := pointI.(*models.GeoPoint)
		dist := calculateDistance(longitude, latitude, point.Longitude, point.Latitude)

		if dist <= radiusM {
			result := *point
			result.Distance = dist
			results = append(results, result)
		}
		return true
	})

	// Sort results
	sortOrder := strings.ToUpper(sortOption)
	sortGeoResults(results, sortOrder)

	// Apply count limit
	if count > 0 && len(results) > count {
		results = results[:count]
	}

	return results, nil
}

// GeoSearch searches for geographical points stored in the memory cache based on the provided options.
// It supports searching by radius or bounding box, and can sort and limit the results.
//
// Parameters:
//   - key: The key identifying the set of geographical points.
//   - options: A pointer to GeoSearchOptions containing search parameters.
//
// Returns:
//   - A slice of GeoPoint containing the search results.
//   - An error if the search fails.
//
// The search options can include:
//   - FromMember: The member name to start the search from. If provided, its coordinates will be used as the center.
//   - FromLon, FromLat: The longitude and latitude to start the search from. These are set if FromMember is provided.
//   - ByRadius: A boolean indicating if the search should be within a radius.
//   - Radius: The radius distance for the search.
//   - Unit: The unit of the radius (e.g., meters, kilometers).
//   - ByBox: A boolean indicating if the search should be within a bounding box.
//   - BoxWidth, BoxHeight: The width and height of the bounding box.
//   - Sort: The sorting order of the results ("ASC" or "DESC").
//   - Count: The maximum number of results to return.
//
// If FromMember is provided but not found, an error is returned.
// If ByRadius is true, the search will include points within the specified radius from the center.
// If ByBox is true, the search will include points within the specified bounding box.
// The results can be sorted and limited based on the options provided.
func (c *MemoryCache) GeoSearch(key string, options *models.GeoSearchOptions) ([]models.GeoPoint, error) {
	geoSetI, exists := c.geoData.Load(key)
	if !exists {
		return nil, nil
	}
	geoSet := geoSetI.(*sync.Map)

	if options.FromMember != "" {
		if pointI, exists := geoSet.Load(options.FromMember); exists {
			point := pointI.(*models.GeoPoint)
			options.FromLon = point.Longitude
			options.FromLat = point.Latitude
		} else {
			return nil, fmt.Errorf("ERR member not found")
		}
	}

	var results []models.GeoPoint
	geoSet.Range(func(_, pointI interface{}) bool {
		point := pointI.(*models.GeoPoint)
		var inArea bool

		if options.ByRadius {
			dist := calculateDistance(options.FromLon, options.FromLat, point.Longitude, point.Latitude)
			radiusM := convertToMeters(options.Radius, options.Unit)
			inArea = dist <= radiusM
			if inArea {
				result := *point
				result.Distance = dist
				results = append(results, result)
			}
		} else if options.ByBox {
			widthM := convertToMeters(options.BoxWidth, options.Unit)
			heightM := convertToMeters(options.BoxHeight, options.Unit)

			latDiff := math.Abs(point.Latitude - options.FromLat)
			lonDiff := math.Abs(point.Longitude - options.FromLon)

			inArea = latDiff <= heightM/(111320.0) &&
				lonDiff <= widthM/(111320.0*math.Cos(options.FromLat*math.Pi/180.0))
			if inArea {
				result := *point
				result.Distance = calculateDistance(options.FromLon, options.FromLat, point.Longitude, point.Latitude)
				results = append(results, result)
			}
		}

		return true
	})

	// Sort results if requested
	if options.Sort != "" {
		sortGeoResults(results, strings.ToUpper(options.Sort))
	}

	// Apply count limit
	if options.Count > 0 && len(results) > options.Count {
		results = results[:options.Count]
	}

	return results, nil
}

// GeoSearchStore searches for geographical points based on the given options
// from the source key and stores the results in the destination key.
//
// Parameters:
// - destKey: The key where the search results will be stored.
// - srcKey: The key from which the geographical search will be performed.
// - options: The options to use for the geographical search.
//
// Returns:
// - int: The number of points stored in the destination key.
// - error: An error if the search or store operation fails.
func (c *MemoryCache) GeoSearchStore(destKey, srcKey string, options *models.GeoSearchOptions) (int, error) {
	results, err := c.GeoSearch(srcKey, options)
	if err != nil {
		return 0, err
	}

	geoSetI, _ := c.geoData.LoadOrStore(destKey, &sync.Map{})
	geoSet := geoSetI.(*sync.Map)

	stored := 0
	for _, point := range results {
		geoSet.Store(point.Name, &point)
		stored++
	}

	if stored > 0 {
		c.incrementKeyVersion(destKey)
	}

	return stored, nil
}

func (c *MemoryCache) defragGeoData() {
	c.geoData.Range(func(key, valueI interface{}) bool {
		geoSet := valueI.(*sync.Map)
		defraggedGeoSet := c.defragSyncMap(geoSet)
		if defraggedGeoSet != geoSet {
			c.geoData.Store(key, defraggedGeoSet)
		}
		return true
	})
}
