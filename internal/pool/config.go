package pool

import "time"

type Config struct {
	InitialSize   int
	MaxSize       int
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	IdleTimeout   time.Duration
	RetryAttempts int
	RetryDelay    time.Duration
}
