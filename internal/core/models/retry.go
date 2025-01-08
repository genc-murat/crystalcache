package models

import (
	"errors"
	"time"
)

type RetryStrategy struct {
	MaxInterval     time.Duration
	InitialInterval time.Duration
	Timeout         time.Duration
	Multiplier      float64
	MaxAttempts     int
}

var (
	ErrMaxRetriesExceeded = errors.New("maximum retry attempts exceeded")
	ErrOperationTimeout   = errors.New("operation timeout")
)

var DefaultRetryStrategy = RetryStrategy{
	MaxAttempts:     3,
	InitialInterval: 100 * time.Millisecond,
	MaxInterval:     2 * time.Second,
	Multiplier:      2.0,
	Timeout:         5 * time.Second,
}
