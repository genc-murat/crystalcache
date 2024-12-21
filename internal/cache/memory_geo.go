package cache

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

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

// Helper functions

func isValidCoordinate(lon, lat float64) bool {
	return lon >= -180 && lon <= 180 && lat >= -85.05112878 && lat <= 85.05112878
}

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
	newGeoData := &sync.Map{}

	c.geoData.Range(func(key, valueI interface{}) bool {
		geoSet := valueI.(*sync.Map)
		newGeoSet := &sync.Map{}

		geoSet.Range(func(member, pointI interface{}) bool {
			point := pointI.(*models.GeoPoint)
			newGeoSet.Store(member, point)
			return true
		})

		newGeoData.Store(key, newGeoSet)
		return true
	})

	c.geoData = newGeoData
}
