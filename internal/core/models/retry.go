package models

import (
	"errors"
	"time"
)

type RetryStrategy struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	Timeout         time.Duration
}

var (
	ErrMaxRetriesExceeded = errors.New("maximum retry attempts exceeded")
	ErrOperationTimeout   = errors.New("operation timeout")
)

// Varsayılan retry stratejisi
var DefaultRetryStrategy = RetryStrategy{
	MaxAttempts:     3,
	InitialInterval: 100 * time.Millisecond,
	MaxInterval:     2 * time.Second,
	Multiplier:      2.0,
	Timeout:         5 * time.Second,
}
