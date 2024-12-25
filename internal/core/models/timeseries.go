package models

import "sync"

type TimeSeries struct {
	Samples []TimeSeriesSample
	Labels  map[string]string
	Mutex   sync.Mutex
	Rules   []TimeSeriesRule
}

type TimeSeriesSample struct {
	Timestamp int64
	Value     float64
}

type TimeSeriesStats struct {
	TotalSamples int
	MaxValue     float64
	MinValue     float64
	AvgValue     float64
}

type TimeSeriesRule struct {
	AggregationType string
	BucketSize      int64
	DestinationKey  string
}
