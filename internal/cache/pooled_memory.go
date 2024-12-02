package cache

import (
	"context"
	"net"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/pool"
)

type PooledMemoryCache struct {
	*MemoryCache
	pool ports.Pool
}

func NewPooledMemoryCache(config pool.Config) (*PooledMemoryCache, error) {
	memCache := NewMemoryCache()

	factory := func() (net.Conn, error) {
		return net.Dial("tcp", "localhost:6379")
	}

	pool, err := pool.NewConnectionPool(config, factory)
	if err != nil {
		return nil, err
	}

	return &PooledMemoryCache{
		MemoryCache: memCache,
		pool:        pool,
	}, nil
}

// Read operasyonları için connection kullanımı
func (c *PooledMemoryCache) Get(key string) (string, bool) {
	conn, err := c.pool.GetReadConn(context.Background())
	if err != nil {
		return "", false
	}
	defer c.pool.ReturnConn(conn, "read")

	return c.MemoryCache.Get(key)
}

func (c *PooledMemoryCache) HGet(hash, key string) (string, bool) {
	conn, err := c.pool.GetReadConn(context.Background())
	if err != nil {
		return "", false
	}
	defer c.pool.ReturnConn(conn, "read")

	return c.MemoryCache.HGet(hash, key)
}

func (c *PooledMemoryCache) HGetAll(hash string) map[string]string {
	conn, err := c.pool.GetReadConn(context.Background())
	if err != nil {
		return make(map[string]string)
	}
	defer c.pool.ReturnConn(conn, "read")

	return c.MemoryCache.HGetAll(hash)
}

// Write operasyonları için connection kullanımı
func (c *PooledMemoryCache) Set(key string, value string) error {
	conn, err := c.pool.GetWriteConn(context.Background())
	if err != nil {
		return err
	}
	defer c.pool.ReturnConn(conn, "write")

	return c.MemoryCache.Set(key, value)
}

func (c *PooledMemoryCache) HSet(hash, key, value string) error {
	conn, err := c.pool.GetWriteConn(context.Background())
	if err != nil {
		return err
	}
	defer c.pool.ReturnConn(conn, "write")

	return c.MemoryCache.HSet(hash, key, value)
}

// Pool metriklerini döndüren yeni metod
func (c *PooledMemoryCache) GetPoolStats() (active, idle int, avgWaitTime time.Duration) {
	return c.pool.Stats()
}

// Kapatma işlemi
func (c *PooledMemoryCache) Close() error {
	return c.pool.Close()
}

// LRU/Transaction/HyperLogLog operasyonları için wrapper metodlar
func (c *PooledMemoryCache) LPush(key string, value string) (int, error) {
	conn, err := c.pool.GetWriteConn(context.Background())
	if err != nil {
		return 0, err
	}
	defer c.pool.ReturnConn(conn, "write")

	return c.MemoryCache.LPush(key, value)
}

func (c *PooledMemoryCache) RPush(key string, value string) (int, error) {
	conn, err := c.pool.GetWriteConn(context.Background())
	if err != nil {
		return 0, err
	}
	defer c.pool.ReturnConn(conn, "write")

	return c.MemoryCache.RPush(key, value)
}

func (c *PooledMemoryCache) LRange(key string, start, stop int) ([]string, error) {
	conn, err := c.pool.GetReadConn(context.Background())
	if err != nil {
		return nil, err
	}
	defer c.pool.ReturnConn(conn, "read")

	return c.MemoryCache.LRange(key, start, stop)
}

// Sorted Set operasyonları için wrapper metodlar
func (c *PooledMemoryCache) ZAdd(key string, score float64, member string) error {
	conn, err := c.pool.GetWriteConn(context.Background())
	if err != nil {
		return err
	}
	defer c.pool.ReturnConn(conn, "write")

	return c.MemoryCache.ZAdd(key, score, member)
}

func (c *PooledMemoryCache) ZRange(key string, start, stop int) []string {
	conn, err := c.pool.GetReadConn(context.Background())
	if err != nil {
		return nil
	}
	defer c.pool.ReturnConn(conn, "read")

	return c.MemoryCache.ZRange(key, start, stop)
}

// Transaction operasyonları için wrapper metodlar
func (c *PooledMemoryCache) Multi() error {
	return c.MemoryCache.Multi()
}

func (c *PooledMemoryCache) Exec() ([]models.Value, error) {
	return c.MemoryCache.Exec()
}

func (c *PooledMemoryCache) Watch(keys ...string) error {
	return c.MemoryCache.Watch(keys...)
}

// Custom operasyonlar için context ve timeout desteği
func (c *PooledMemoryCache) ExecuteWithTimeout(ctx context.Context, op func() error) error {
	done := make(chan error, 1)
	go func() {
		done <- op()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
