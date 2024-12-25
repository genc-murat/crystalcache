# CrystalCache

CrystalCache is a high-performance, Redis-compatible in-memory data store and cache implementation in Go. It provides a robust, concurrent-safe caching solution with comprehensive support for advanced features including replication, ACL management, and various specialized data structures.

## Table of Contents
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Data Types and Commands](#data-types-and-commands)
- [Security](#security)
- [Persistence](#persistence)
- [Replication](#replication)
- [Monitoring](#monitoring)
- [Performance Optimization](#performance-optimization)
- [Client Management](#client-management)
- [Advanced Features](#advanced-features)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Features

### Core Features
- **Master-Slave Replication**
  - Asynchronous replication support
  - Automatic failover capabilities
  - Full and incremental synchronization
  - Replica read-only mode
  
- **Access Control Lists (ACL)**
  - Fine-grained permission management
  - Command-level access control
  - User authentication and authorization
  - Default user configuration
  
- **Transaction Support**
  - MULTI/EXEC/DISCARD commands
  - Optimistic locking with WATCH
  - Atomic operations
  - Pipeline support for batch operations

- **Persistence**
  - Append-Only File (AOF) persistence
  - Configurable sync intervals
  - Automatic recovery on startup
  - Background saving

### Data Structures

#### Basic Data Types
- **Strings**
  - Binary-safe strings
  - Maximum size: 512MB
  - Atomic increment/decrement
  - Bit operations
  
- **Lists**
  - Linked lists
  - Both ends insertion/extraction
  - Blocking operations
  - Range operations
  
- **Sets**
  - Unordered collections
  - Set operations (union, intersection, difference)
  - Random member selection
  - Membership testing
  
- **Hashes**
  - Field-value pairs
  - Incremental operations
  - Multiple field operations
  - Field existence testing
  
- **Sorted Sets**
  - Score-based ordering
  - Range operations
  - Lexicographical operations
  - Aggregation operations

#### Advanced Data Types
- **Streams**
  - Append-only log structures
  - Consumer groups
  - Message acknowledgment
  - Range queries
  
- **Bitmaps**
  - Bit-level operations
  - Space-efficient storage
  - Counting and finding operations
  - Bitfield operations
  
- **JSON**
  - Document storage
  - Path-based operations
  - Array manipulations
  - Atomic updates
  
- **Time Series**
  - Time-based data storage
  - Aggregation functions
  - Retention policies
  - Downsampling

#### Probabilistic Data Structures
- **HyperLogLog**
  - Cardinality estimation
  - Merge operations
  - Standard error: 0.81%
  
- **Bloom Filters**
  - Membership testing
  - False positive rate configuration
  - Space-efficient storage
  
- **Count-Min Sketch**
  - Frequency estimation
  - Configurable accuracy
  - Memory-efficient counting
  
- **Cuckoo Filters**
  - Item deletion support
  - Better space efficiency than Bloom filters
  - Configurable false positive rate
  
- **Top-K**
  - Heavy hitters tracking
  - Count-based filtering
  - Decay support
  
- **T-Digest**
  - Quantile approximation
  - Merge operations
  - Configurable compression

#### Spatial Data Structures
- **Geospatial Indexes**
  - Location-based queries
  - Radius searches
  - Distance calculations
  - Geohash support

## Architecture

### Core Components
1. **Client Handler**
   - Connection management
   - Protocol parsing (RESP)
   - Command queuing

2. **Command Router**
   - Command validation
   - ACL enforcement
   - Handler dispatch

3. **Storage Engine**
   - Memory management
   - Data structure implementation
   - Index management

4. **Persistence Layer**
   - AOF writing
   - Recovery management
   - Sync coordination

## Installation

### From Source
```bash
# Clone the repository
git clone https://github.com/genc-murat/crystalcache.git

# Change to project directory
cd crystalcache

# Install dependencies
go mod download

# Build
go build -o crystalcache cmd/main.go

# Run tests
go test ./...
```

### Using Go Get
```bash
go get github.com/genc-murat/crystalcache
```

## Configuration

### Server Configuration
```go
type ServerConfig struct {
    MaxConnections int           // Maximum number of concurrent connections
    ReadTimeout    time.Duration // Read timeout for client connections
    WriteTimeout   time.Duration // Write timeout for client connections
    IdleTimeout    time.Duration // Idle timeout for client connections
}
```

### Connection Pool Configuration
```go
type Config struct {
    InitialSize   int           // Initial number of connections in pool
    MaxSize       int           // Maximum number of connections
    ReadTimeout   time.Duration // Read timeout for pooled connections
    WriteTimeout  time.Duration // Write timeout for pooled connections
    IdleTimeout   time.Duration // How long connections can remain idle
    RetryAttempts int           // Number of retry attempts for failed operations
    RetryDelay    time.Duration // Delay between retry attempts
}
```

### AOF Configuration
```go
type AOFConfig struct {
    Path          string        // Path to AOF file
    SyncInterval  time.Duration // Interval between fsync operations
    BufferSize    int          // Write buffer size
    Enabled       bool         // Whether AOF is enabled
}
```

### Complete Server Setup Example
```go
package main

import (
    "context"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/genc-murat/crystalcache/internal/cache"
    "github.com/genc-murat/crystalcache/internal/pool"
    "github.com/genc-murat/crystalcache/internal/server"
    "github.com/genc-murat/crystalcache/internal/storage"
)

func main() {
    // Server configuration
    serverConfig := server.ServerConfig{
        MaxConnections: 1000,
        ReadTimeout:    10 * time.Second,
        WriteTimeout:   10 * time.Second,
        IdleTimeout:    60 * time.Second,
    }

    // Initialize cache with defragmentation
    memCache := cache.NewMemoryCache()
    memCache.StartDefragmentation(5*time.Minute, 0.25)

    // Configure AOF persistence
    aofConfig := storage.DefaultAOFConfig()
    aofConfig.Path = "database.aof"
    aofConfig.SyncInterval = 2 * time.Second

    aofStorage, err := storage.NewAOF(aofConfig)
    if err != nil {
        log.Fatal(err)
    }
    defer aofStorage.Close()

    // Initialize server
    server := server.NewServer(memCache, aofStorage, nil, serverConfig)
    server.SetMaster(true)

    // Configure connection pool
    poolConfig := pool.Config{
        InitialSize:   10,
        MaxSize:       100,
        ReadTimeout:   5 * time.Second,
        WriteTimeout:  5 * time.Second,
        IdleTimeout:   60 * time.Second,
        RetryAttempts: 3,
        RetryDelay:    100 * time.Millisecond,
    }

    factory := pool.NewConnFactory("localhost:6379", 5*time.Second)
    connectionPool, err := pool.NewConnectionPool(poolConfig, factory.CreateConnection)
    if err != nil {
        log.Fatal(err)
    }
    defer connectionPool.Close()

    server.SetConnectionPool(connectionPool)

    // Setup metrics endpoint
    go func() {
        http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
            metrics := server.GetMetrics()
            json.NewEncoder(w).Encode(metrics)
        })
        log.Printf("Metrics server starting on :2112")
        if err := http.ListenAndServe(":2112", nil); err != nil {
            log.Printf("Metrics server error: %v", err)
        }
    }()

    // Start the server
    go server.Start(":6379")

    // Graceful shutdown
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh

    log.Println("Shutting down server...")
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    if err := server.Shutdown(ctx); err != nil {
        log.Printf("Error during shutdown: %v", err)
    }
}
```

## Usage Examples

### Basic Operations

#### String Operations
```go
// SET command
SET("key", "value")
SET("key", "value", "EX", 60)  // With expiration

// GET command
value := GET("key")

// INCR command
newValue := INCR("counter")
```

#### List Operations
```go
// LPUSH command
LPUSH("list", "value1", "value2")

// LRANGE command
values := LRANGE("list", 0, -1)

// BRPOP command
value := BRPOP("list", 5)  // With timeout
```

#### Hash Operations
```go
// HSET command
HSET("hash", "field1", "value1", "field2", "value2")

// HGET command
value := HGET("hash", "field1")

// HINCRBY command
newValue := HINCRBY("hash", "counter", 1)
```

### Advanced Operations

#### Transaction Example
```go
MULTI()
SET("key1", "value1")
INCR("counter")
result := EXEC()
```

#### Pipeline Example
```go
pipeline := client.Pipeline()
pipeline.SET("key1", "value1")
pipeline.INCR("counter")
pipeline.EXEC()
```

#### Geospatial Operations
```go
// Add locations
GEOADD("locations", -122.27652, 37.80574, "place1")
GEOADD("locations", -122.27652, 37.80574, "place2")

// Find nearby locations
nearby := GEORADIUS("locations", -122.27652, 37.80574, 5, "km")
```

#### JSON Operations
```go
// Set JSON document
JSON.SET("user", ".", {
    "name": "John",
    "age": 30,
    "address": {
        "city": "New York"
    }
})

// Get specific path
city := JSON.GET("user", "$.address.city")

// Array operations
JSON.ARRAPPEND("user", "$.hobbies", "reading")
```

## Security

### ACL Configuration
```go
// Create user with specific permissions
server.ACL("SETUSER", "myuser", "on", "allcommands", "allkeys")

// Create user with limited permissions
server.ACL("SETUSER", "readonly", "on", "get", "~keys:*")

// Remove user
server.ACL("DELUSER", "myuser")
```

### Authentication Example
```go
// Authenticate with username and password
AUTH("username", "password")

// Old-style authentication
AUTH("password")
```

## Replication

### Master Setup
```go
server.SetMaster(true)
```

### Slave Setup
```go
// Connect to master
server.StartReplication("master-host", "6379")

// Check replication status
info := server.GetReplicationInfo()
```

### Replication Monitoring
```go
// Get replication status
status := server.GetReplicationInfo()

// Check connected slaves
count := server.GetReplicaCount()
```

## Monitoring

### Metrics Endpoint
```go
// Setup metrics endpoint
http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
    metrics := server.GetMetrics()
    json.NewEncoder(w).Encode(metrics)
})
```

### Available Metrics
- Connection statistics
- Command statistics
- Memory usage
- Replication status
- Cache hit/miss ratio
- Operation latencies

## Performance Optimization

### Memory Management
- Regular defragmentation
- Memory-efficient data structures
- Lazy deletion of expired keys

### Connection Management
- Connection pooling
- Keep-alive settings
- Timeout configurations

### Command Processing
- Pipeline support
- Batch operations
- Optimized data structures

## Troubleshooting

### Common Issues

1. **Connection Issues**
```
ERROR: Connection refused
SOLUTION: Check if server is running and port is correct
```

2. **Memory Issues**
```
ERROR: Out of memory
SOLUTION: Configure appropriate maxmemory and policy
```

3. **Replication Issues**
```
ERROR: Replication sync failed
SOLUTION: Check network connectivity and authentication
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

For questions and feedback, please:
- Open an issue on GitHub