# CrystalCache

CrystalCache is a high-performance, in-memory data store and cache implementation in Go, designed to be compatible with Redis protocols and commands. It provides a robust, concurrent-safe caching solution with support for multiple data structures and features.

## Features

- **Data Structures**
  - Strings
  - Lists
  - Sets
  - Hashes
  - Sorted Sets (ZSets)
  - JSON

- **Core Features**
  - Transaction support (MULTI/EXEC/DISCARD)
  - Key expiration (TTL)
  - Pattern matching for key operations
  - Atomic operations
  - Pipeline support
  - Scan operations
  - Memory optimization with defragmentation

- **Advanced Features**
  - HyperLogLog support
  - Bloom filter implementation
  - Built-in monitoring and metrics
  - Connection pooling

## Installation

```bash
go get github.com/genc-murat/crystalcache
```

## Quick Start

```go
package main

import (
    "github.com/genc-murat/crystalcache/internal/cache"
    "github.com/genc-murat/crystalcache/internal/server"
)

func main() {
    // Create new cache instance
    memCache := cache.NewMemoryCache()

    // Configure server
    config := server.ServerConfig{
        MaxConnections: 1000,
        ReadTimeout:    10 * time.Second,
        WriteTimeout:   10 * time.Second,
        IdleTimeout:    60 * time.Second,
    }

    // Create and start server
    server := server.NewServer(memCache, nil, nil, config)
    server.Start(":6379")
}
```

## Supported Commands

CrystalCache supports a wide range of Redis-compatible commands, including:

- **String Operations**: SET, GET, DEL, EXISTS, EXPIRE, etc.
- **List Operations**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.
- **Set Operations**: SADD, SREM, SMEMBERS, SCARD, etc.
- **Hash Operations**: HSET, HGET, HGETALL, HDEL, etc.
- **Sorted Set Operations**: ZADD, ZRANGE, ZRANK, ZSCORE, etc.
- **JSON Operations**: JSON.SET, JSON.GET, JSON.DEL, JSON.TYPE
- **Admin Commands**: INFO, DBSIZE, FLUSHALL
- **Transaction Commands**: MULTI, EXEC, DISCARD, WATCH, UNWATCH

## Performance Optimization

CrystalCache includes several features for optimizing performance:

- Automatic memory defragmentation
- Bloom filter for quick key lookups
- Connection pooling for efficient client handling
- Concurrent access with fine-grained locking

## Configuration

The server can be configured with various parameters:

```go
type ServerConfig struct {
    MaxConnections int
    ReadTimeout    time.Duration
    WriteTimeout   time.Duration
    IdleTimeout    time.Duration
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Inspired by Redis
- Built with Go's strong concurrency support
- Uses efficient data structures for optimal performance

## Contact

For questions and feedback, please open an issue on GitHub.