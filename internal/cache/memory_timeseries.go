package cache

import (
	"fmt"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

func (c *MemoryCache) TSCreate(key string, labels map[string]string) error {
	if _, exists := c.timeSeries.Load(key); exists {
		return fmt.Errorf("ERR time series already exists")
	}
	ts := &models.TimeSeries{
		Samples: []models.TimeSeriesSample{},
		Labels:  labels,
	}
	c.timeSeries.Store(key, ts)
	return nil
}

func (c *MemoryCache) TSAdd(key string, timestamp int64, value float64) error {
	tsI, exists := c.timeSeries.Load(key)
	if !exists {
		return fmt.Errorf("ERR no such time series")
	}
	sourceTS := tsI.(*models.TimeSeries)

	sourceTS.Mutex.Lock()
	sourceTS.Samples = append(sourceTS.Samples, models.TimeSeriesSample{Timestamp: timestamp, Value: value})
	sourceTS.Mutex.Unlock()

	// Kuralları uygula
	for _, rule := range sourceTS.Rules {
		destTSI, destExists := c.timeSeries.Load(rule.DestinationKey)
		if destExists {
			destTS := destTSI.(*models.TimeSeries)
			applyRule(rule, sourceTS, destTS)
		}
	}

	return nil
}

func (c *MemoryCache) TSGet(key string) (*models.TimeSeriesSample, error) {
	tsI, exists := c.timeSeries.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR no such time series")
	}
	ts := tsI.(*models.TimeSeries)

	if len(ts.Samples) == 0 {
		return nil, fmt.Errorf("ERR no samples in time series")
	}
	return &ts.Samples[len(ts.Samples)-1], nil
}

func (c *MemoryCache) TSMAdd(entries map[string][]models.TimeSeriesSample) error {
	for key, samples := range entries {
		tsI, exists := c.timeSeries.Load(key)
		if !exists {
			return fmt.Errorf("ERR no such time series: %s", key)
		}
		sourceTS := tsI.(*models.TimeSeries)

		sourceTS.Mutex.Lock()
		sourceTS.Samples = append(sourceTS.Samples, samples...)
		sourceTS.Mutex.Unlock()

		// Kuralları uygula
		for _, rule := range sourceTS.Rules {
			destTSI, destExists := c.timeSeries.Load(rule.DestinationKey)
			if destExists {
				destTS := destTSI.(*models.TimeSeries)
				applyRule(rule, sourceTS, destTS)
			}
		}
	}
	return nil
}

func (c *MemoryCache) TSDel(key string, from, to int64) (int, error) {
	tsI, exists := c.timeSeries.Load(key)
	if !exists {
		return 0, fmt.Errorf("ERR no such time series")
	}
	ts := tsI.(*models.TimeSeries)

	ts.Mutex.Lock()
	defer ts.Mutex.Unlock()

	originalCount := len(ts.Samples)
	filteredSamples := ts.Samples[:0]

	for _, sample := range ts.Samples {
		if sample.Timestamp < from || sample.Timestamp > to {
			filteredSamples = append(filteredSamples, sample)
		}
	}
	ts.Samples = filteredSamples
	return originalCount - len(filteredSamples), nil
}

func (c *MemoryCache) TSRange(key string, from, to int64) ([]models.TimeSeriesSample, error) {
	tsI, exists := c.timeSeries.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR no such time series")
	}
	ts := tsI.(*models.TimeSeries)

	ts.Mutex.Lock()
	defer ts.Mutex.Unlock()

	var results []models.TimeSeriesSample
	for _, sample := range ts.Samples {
		if sample.Timestamp >= from && sample.Timestamp <= to {
			results = append(results, sample)
		}
	}
	return results, nil
}

func (c *MemoryCache) TSMRange(filters map[string]string, from, to int64) (map[string][]models.TimeSeriesSample, error) {
	results := make(map[string][]models.TimeSeriesSample)

	c.timeSeries.Range(func(key, value interface{}) bool {
		ts := value.(*models.TimeSeries)

		// Filtre kontrolü
		matches := true
		for k, v := range filters {
			if tsValue, ok := ts.Labels[k]; !ok || tsValue != v {
				matches = false
				break
			}
		}

		if matches {
			ts.Mutex.Lock()
			var rangeSamples []models.TimeSeriesSample
			for _, sample := range ts.Samples {
				if sample.Timestamp >= from && sample.Timestamp <= to {
					rangeSamples = append(rangeSamples, sample)
				}
			}
			ts.Mutex.Unlock()

			if len(rangeSamples) > 0 {
				results[key.(string)] = rangeSamples
			}
		}

		return true
	})

	if len(results) == 0 {
		return nil, fmt.Errorf("ERR no matching time series found")
	}

	return results, nil
}

func (c *MemoryCache) TSIncrBy(key string, increment float64) error {
	tsI, exists := c.timeSeries.Load(key)
	if !exists {
		return fmt.Errorf("ERR no such time series")
	}
	ts := tsI.(*models.TimeSeries)

	ts.Mutex.Lock()
	defer ts.Mutex.Unlock()

	if len(ts.Samples) == 0 {
		ts.Samples = append(ts.Samples, models.TimeSeriesSample{
			Timestamp: time.Now().Unix(),
			Value:     increment,
		})
	} else {
		lastSample := &ts.Samples[len(ts.Samples)-1]
		ts.Samples = append(ts.Samples, models.TimeSeriesSample{
			Timestamp: time.Now().Unix(),
			Value:     lastSample.Value + increment,
		})
	}
	return nil
}

func (c *MemoryCache) TSDecrBy(key string, decrement float64) error {
	return c.TSIncrBy(key, -decrement)
}

func (c *MemoryCache) TSInfo(key string) (*models.TimeSeriesStats, error) {
	tsI, exists := c.timeSeries.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR no such time series")
	}
	ts := tsI.(*models.TimeSeries)

	ts.Mutex.Lock()
	defer ts.Mutex.Unlock()

	if len(ts.Samples) == 0 {
		return nil, fmt.Errorf("ERR no samples in time series")
	}

	var total, max, min float64
	min = ts.Samples[0].Value
	for _, sample := range ts.Samples {
		total += sample.Value
		if sample.Value > max {
			max = sample.Value
		}
		if sample.Value < min {
			min = sample.Value
		}
	}

	avg := total / float64(len(ts.Samples))
	return &models.TimeSeriesStats{
		TotalSamples: len(ts.Samples),
		MaxValue:     max,
		MinValue:     min,
		AvgValue:     avg,
	}, nil
}

func (c *MemoryCache) TSAlter(key string, labels map[string]string) error {
	tsI, exists := c.timeSeries.Load(key)
	if !exists {
		return fmt.Errorf("ERR no such time series")
	}
	ts := tsI.(*models.TimeSeries)

	ts.Mutex.Lock()
	defer ts.Mutex.Unlock()

	for k, v := range labels {
		ts.Labels[k] = v
	}

	return nil
}

func (c *MemoryCache) TSCreateRule(sourceKey, destKey string, aggregationType string, bucketSize int64) error {
	sourceTSI, sourceExists := c.timeSeries.Load(sourceKey)
	destTSI, destExists := c.timeSeries.Load(destKey)

	if !sourceExists {
		return fmt.Errorf("ERR no such source time series: %s", sourceKey)
	}
	if !destExists {
		return fmt.Errorf("ERR no such destination time series: %s", destKey)
	}

	sourceTS := sourceTSI.(*models.TimeSeries)
	destTS := destTSI.(*models.TimeSeries)

	sourceTS.Mutex.Lock()
	defer sourceTS.Mutex.Unlock()

	destTS.Mutex.Lock()
	defer destTS.Mutex.Unlock()

	rule := models.TimeSeriesRule{
		AggregationType: aggregationType,
		BucketSize:      bucketSize,
		DestinationKey:  destKey,
	}
	sourceTS.Rules = append(sourceTS.Rules, rule)

	return nil
}

func applyRule(rule models.TimeSeriesRule, sourceTS *models.TimeSeries, destTS *models.TimeSeries) {
	var total float64
	var count int64
	var startBucket int64

	destTS.Mutex.Lock()
	defer destTS.Mutex.Unlock()

	for _, sample := range sourceTS.Samples {
		if startBucket == 0 {
			startBucket = sample.Timestamp
		}

		total += sample.Value
		count++

		if sample.Timestamp >= startBucket+rule.BucketSize {
			destTS.Samples = append(destTS.Samples, models.TimeSeriesSample{
				Timestamp: startBucket,
				Value:     aggregate(rule.AggregationType, total, count),
			})
			startBucket = sample.Timestamp
			total = 0
			count = 0
		}
	}

	if count > 0 {
		destTS.Samples = append(destTS.Samples, models.TimeSeriesSample{
			Timestamp: startBucket,
			Value:     aggregate(rule.AggregationType, total, count),
		})
	}
}

func aggregate(aggregationType string, total float64, count int64) float64 {
	switch aggregationType {
	case "avg":
		return total / float64(count)
	case "sum":
		return total
	case "min":
		return total
	case "max":
		return total
	default:
		return 0
	}
}

func (c *MemoryCache) TSMGet(filters map[string]string) (map[string]*models.TimeSeriesSample, error) {
	results := make(map[string]*models.TimeSeriesSample)

	c.timeSeries.Range(func(key, value interface{}) bool {
		ts := value.(*models.TimeSeries)

		// Filtre kontrolü
		matches := true
		for k, v := range filters {
			if tsValue, ok := ts.Labels[k]; !ok || tsValue != v {
				matches = false
				break
			}
		}

		if matches {
			ts.Mutex.Lock()
			if len(ts.Samples) > 0 {
				results[key.(string)] = &ts.Samples[len(ts.Samples)-1]
			}
			ts.Mutex.Unlock()
		}

		return true
	})

	if len(results) == 0 {
		return nil, fmt.Errorf("ERR no matching time series found")
	}

	return results, nil
}

func (c *MemoryCache) TSDeleteRule(sourceKey, destinationKey string) error {
	tsI, exists := c.timeSeries.Load(sourceKey)
	if !exists {
		return fmt.Errorf("ERR no such source time series")
	}
	sourceTS := tsI.(*models.TimeSeries)

	sourceTS.Mutex.Lock()
	defer sourceTS.Mutex.Unlock()

	newRules := []models.TimeSeriesRule{}
	for _, rule := range sourceTS.Rules {
		if rule.DestinationKey != destinationKey {
			newRules = append(newRules, rule)
		}
	}

	if len(newRules) == len(sourceTS.Rules) {
		return fmt.Errorf("ERR no such rule exists for destination: %s", destinationKey)
	}

	sourceTS.Rules = newRules
	return nil
}

func (c *MemoryCache) TSQueryIndex(filters map[string]string) ([]string, error) {
	var results []string

	c.timeSeries.Range(func(key, value interface{}) bool {
		ts := value.(*models.TimeSeries)

		// Filtreleri kontrol et
		matches := true
		for k, v := range filters {
			if tsValue, ok := ts.Labels[k]; !ok || tsValue != v {
				matches = false
				break
			}
		}

		if matches {
			results = append(results, key.(string))
		}

		return true
	})

	if len(results) == 0 {
		return nil, fmt.Errorf("ERR no matching time series found")
	}

	return results, nil
}

func (c *MemoryCache) TSRevRange(key string, from, to int64) ([]models.TimeSeriesSample, error) {
	tsI, exists := c.timeSeries.Load(key)
	if !exists {
		return nil, fmt.Errorf("ERR no such time series")
	}
	ts := tsI.(*models.TimeSeries)

	ts.Mutex.Lock()
	defer ts.Mutex.Unlock()

	var results []models.TimeSeriesSample
	for i := len(ts.Samples) - 1; i >= 0; i-- {
		sample := ts.Samples[i]
		if sample.Timestamp >= from && sample.Timestamp <= to {
			results = append(results, sample)
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("ERR no samples in the specified range")
	}

	return results, nil
}

func (c *MemoryCache) TSMRevRange(filters map[string]string, from, to int64) (map[string][]models.TimeSeriesSample, error) {
	results := make(map[string][]models.TimeSeriesSample)

	c.timeSeries.Range(func(key, value interface{}) bool {
		ts := value.(*models.TimeSeries)

		// Filtre kontrolü
		matches := true
		for k, v := range filters {
			if tsValue, ok := ts.Labels[k]; !ok || tsValue != v {
				matches = false
				break
			}
		}

		if matches {
			ts.Mutex.Lock()
			var rangeSamples []models.TimeSeriesSample
			for i := len(ts.Samples) - 1; i >= 0; i-- {
				sample := ts.Samples[i]
				if sample.Timestamp >= from && sample.Timestamp <= to {
					rangeSamples = append(rangeSamples, sample)
				}
			}
			ts.Mutex.Unlock()

			if len(rangeSamples) > 0 {
				results[key.(string)] = rangeSamples
			}
		}

		return true
	})

	if len(results) == 0 {
		return nil, fmt.Errorf("ERR no matching time series found")
	}

	return results, nil
}
