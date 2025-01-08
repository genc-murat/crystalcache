package models

import "sync"

type TimeSeries struct {
	Mutex   sync.Mutex
	Labels  map[string]string
	Rules   []TimeSeriesRule
	Samples []TimeSeriesSample
}

type TimeSeriesSample struct {
	Timestamp int64
	Value     float64
}

type TimeSeriesStats struct {
	MaxValue     float64
	MinValue     float64
	AvgValue     float64
	TotalSamples int
}

type TimeSeriesRule struct {
	BucketSize      int64
	AggregationType string
	DestinationKey  string
}
