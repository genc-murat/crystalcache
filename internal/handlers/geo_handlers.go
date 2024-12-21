package handlers

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type GeoHandlers struct {
	cache ports.Cache
}

func NewGeoHandlers(cache ports.Cache) *GeoHandlers {
	return &GeoHandlers{
		cache: cache,
	}
}

func (h *GeoHandlers) HandleGeoAdd(args []models.Value) models.Value {
	if len(args) < 4 || (len(args)-1)%3 != 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk
	var points []models.GeoPoint

	// Process location arguments in groups of 3 (lon, lat, member)
	for i := 1; i < len(args); i += 3 {
		lon, err := strconv.ParseFloat(args[i].Bulk, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR longitude must be numeric"}
		}

		lat, err := strconv.ParseFloat(args[i+1].Bulk, 64)
		if err != nil {
			return models.Value{Type: "error", Str: "ERR latitude must be numeric"}
		}

		member := args[i+2].Bulk

		points = append(points, models.GeoPoint{
			Longitude: lon,
			Latitude:  lat,
			Name:      member,
		})
	}

	added, err := h.cache.GeoAdd(key, points...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: added}
}

func (h *GeoHandlers) HandleGeoDist(args []models.Value) models.Value {
	if len(args) < 3 || len(args) > 4 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk
	member1 := args[1].Bulk
	member2 := args[2].Bulk
	unit := "m" // default unit
	if len(args) > 3 {
		unit = strings.ToLower(args[3].Bulk)
	}

	dist, err := h.cache.GeoDist(key, member1, member2, unit)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "bulk", Bulk: fmt.Sprintf("%.4f", dist)}
}

func (h *GeoHandlers) HandleGeoPos(args []models.Value) models.Value {
	if len(args) < 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk
	var members []string
	for i := 1; i < len(args); i++ {
		members = append(members, args[i].Bulk)
	}

	points, err := h.cache.GeoPos(key, members...)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	results := make([]models.Value, len(points))
	for i, point := range points {
		if point == nil {
			results[i] = models.Value{Type: "null"}
		} else {
			results[i] = models.Value{
				Type: "array",
				Array: []models.Value{
					{Type: "bulk", Bulk: fmt.Sprintf("%.6f", point.Longitude)},
					{Type: "bulk", Bulk: fmt.Sprintf("%.6f", point.Latitude)},
				},
			}
		}
	}

	return models.Value{Type: "array", Array: results}
}

func (h *GeoHandlers) HandleGeoRadius(args []models.Value) models.Value {
	if len(args) < 5 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk
	lon, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR longitude must be numeric"}
	}

	lat, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR latitude must be numeric"}
	}

	radius, err := strconv.ParseFloat(args[3].Bulk, 64)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR radius must be numeric"}
	}

	unit := strings.ToLower(args[4].Bulk)

	// Parse optional arguments
	var withDist, withCoord, withHash bool
	var count int
	var sort string

	for i := 5; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "WITHCOORD":
			withCoord = true
		case "WITHDIST":
			withDist = true
		case "WITHHASH":
			withHash = true
		case "COUNT":
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR COUNT requires argument"}
			}
			count, err = strconv.Atoi(args[i+1].Bulk)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR COUNT must be numeric"}
			}
			i++
		case "ASC", "DESC":
			sort = strings.ToUpper(args[i].Bulk)
		}
	}

	points, err := h.cache.GeoRadius(key, lon, lat, radius, unit, withDist, withCoord, withHash, count, sort)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return h.formatGeoResults(points, withDist, withCoord, withHash, unit)
}

func (h *GeoHandlers) HandleGeoSearch(args []models.Value) models.Value {
	if len(args) < 5 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	key := args[0].Bulk
	options := &models.GeoSearchOptions{}

	i := 1
	for i < len(args) {
		switch strings.ToUpper(args[i].Bulk) {
		case "FROMMEMBER":
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR FROMMEMBER requires member name"}
			}
			options.FromMember = args[i+1].Bulk
			i += 2

		case "FROMLONLAT":
			if i+2 >= len(args) {
				return models.Value{Type: "error", Str: "ERR FROMLONLAT requires longitude and latitude"}
			}
			var err error
			options.FromLon, err = strconv.ParseFloat(args[i+1].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR longitude must be numeric"}
			}
			options.FromLat, err = strconv.ParseFloat(args[i+2].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR latitude must be numeric"}
			}
			i += 3

		case "BYBOX":
			if i+3 >= len(args) {
				return models.Value{Type: "error", Str: "ERR BYBOX requires width, height and unit"}
			}
			var err error
			options.ByBox = true
			options.BoxWidth, err = strconv.ParseFloat(args[i+1].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR width must be numeric"}
			}
			options.BoxHeight, err = strconv.ParseFloat(args[i+2].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR height must be numeric"}
			}
			options.Unit = strings.ToLower(args[i+3].Bulk)
			i += 4

		case "BYRADIUS":
			if i+2 >= len(args) {
				return models.Value{Type: "error", Str: "ERR BYRADIUS requires radius and unit"}
			}
			var err error
			options.ByRadius = true
			options.Radius, err = strconv.ParseFloat(args[i+1].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR radius must be numeric"}
			}
			options.Unit = strings.ToLower(args[i+2].Bulk)
			i += 3

		case "ASC", "DESC":
			options.Sort = strings.ToUpper(args[i].Bulk)
			i++

		case "COUNT":
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR COUNT requires argument"}
			}
			count, err := strconv.Atoi(args[i+1].Bulk)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR count must be numeric"}
			}
			options.Count = count
			i += 2

		case "WITHCOORD":
			options.WithCoord = true
			i++

		case "WITHDIST":
			options.WithDist = true
			i++

		case "WITHHASH":
			options.WithHash = true
			i++

		default:
			return models.Value{Type: "error", Str: fmt.Sprintf("ERR unknown option %s", args[i].Bulk)}
		}
	}

	points, err := h.cache.GeoSearch(key, options)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return h.formatGeoResults(points, options.WithDist, options.WithCoord, options.WithHash, options.Unit)
}

func (h *GeoHandlers) HandleGeoSearchStore(args []models.Value) models.Value {
	if len(args) < 6 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments"}
	}

	destKey := args[0].Bulk
	srcKey := args[1].Bulk

	// Parse search options
	options := &models.GeoSearchOptions{}
	i := 2 // Start from the third argument

	for i < len(args) {
		switch strings.ToUpper(args[i].Bulk) {
		case "FROMMEMBER":
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR FROMMEMBER requires member name"}
			}
			options.FromMember = args[i+1].Bulk
			i += 2

		case "FROMLONLAT":
			if i+2 >= len(args) {
				return models.Value{Type: "error", Str: "ERR FROMLONLAT requires longitude and latitude"}
			}
			var err error
			options.FromLon, err = strconv.ParseFloat(args[i+1].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR longitude must be numeric"}
			}
			options.FromLat, err = strconv.ParseFloat(args[i+2].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR latitude must be numeric"}
			}
			i += 3

		case "BYBOX":
			if i+3 >= len(args) {
				return models.Value{Type: "error", Str: "ERR BYBOX requires width, height and unit"}
			}
			var err error
			options.ByBox = true
			options.BoxWidth, err = strconv.ParseFloat(args[i+1].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR width must be numeric"}
			}
			options.BoxHeight, err = strconv.ParseFloat(args[i+2].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR height must be numeric"}
			}
			options.Unit = strings.ToLower(args[i+3].Bulk)
			i += 4

		case "BYRADIUS":
			if i+2 >= len(args) {
				return models.Value{Type: "error", Str: "ERR BYRADIUS requires radius and unit"}
			}
			var err error
			options.ByRadius = true
			options.Radius, err = strconv.ParseFloat(args[i+1].Bulk, 64)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR radius must be numeric"}
			}
			options.Unit = strings.ToLower(args[i+2].Bulk)
			i += 3

		case "ASC", "DESC":
			options.Sort = strings.ToUpper(args[i].Bulk)
			i++

		case "COUNT":
			if i+1 >= len(args) {
				return models.Value{Type: "error", Str: "ERR COUNT requires argument"}
			}
			count, err := strconv.Atoi(args[i+1].Bulk)
			if err != nil {
				return models.Value{Type: "error", Str: "ERR count must be numeric"}
			}
			options.Count = count
			i += 2

		case "WITHCOORD":
			options.WithCoord = true
			i++

		case "WITHDIST":
			options.WithDist = true
			i++

		case "WITHHASH":
			options.WithHash = true
			i++

		default:
			return models.Value{Type: "error", Str: fmt.Sprintf("ERR unknown option %s", args[i].Bulk)}
		}
	}

	// Store results
	stored, err := h.cache.GeoSearchStore(destKey, srcKey, options)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: stored}
}

// Helper function to format geo results
func (h *GeoHandlers) formatGeoResults(points []models.GeoPoint, withDist, withCoord, withHash bool, unit string) models.Value {
	results := make([]models.Value, len(points))
	for i, point := range points {
		var result []models.Value

		// Add base member name
		result = append(result, models.Value{Type: "bulk", Bulk: point.Name})

		// Add distance if requested
		if withDist {
			result = append(result, models.Value{Type: "bulk", Bulk: fmt.Sprintf("%.4f", point.Distance)})
		}

		// Add coordinates if requested
		if withCoord {
			coords := []models.Value{
				{Type: "bulk", Bulk: fmt.Sprintf("%.6f", point.Longitude)},
				{Type: "bulk", Bulk: fmt.Sprintf("%.6f", point.Latitude)},
			}
			result = append(result, models.Value{Type: "array", Array: coords})
		}

		// Add geohash if requested
		if withHash {
			result = append(result, models.Value{Type: "bulk", Bulk: point.GeoHash})
		}

		// If we're only returning the member name, don't wrap it in an array
		if len(result) == 1 {
			results[i] = result[0]
		} else {
			results[i] = models.Value{Type: "array", Array: result}
		}
	}

	return models.Value{Type: "array", Array: results}
}
