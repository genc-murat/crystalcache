package pool

import "time"

type Config struct {
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	IdleTimeout   time.Duration
	RetryDelay    time.Duration
	InitialSize   int
	MaxSize       int
	RetryAttempts int
}
