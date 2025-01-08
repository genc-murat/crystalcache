package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

var (
	ErrPoolClosed = errors.New("pool is closed")
	ErrConnClosed = errors.New("connection is closed")
)

type ConnectionPool struct {
	stats struct {
		sync.RWMutex
		active   int
		idle     int
		waitTime time.Duration
	}
	mu        sync.RWMutex
	config    Config
	factory   func() (net.Conn, error)
	readPool  chan net.Conn
	writePool chan net.Conn
	closed    bool
}

func NewConnectionPool(config Config, factory func() (net.Conn, error)) (*ConnectionPool, error) {
	if config.InitialSize > config.MaxSize {
		return nil, errors.New("initial size cannot be greater than max size")
	}

	pool := &ConnectionPool{
		config:    config,
		factory:   factory,
		readPool:  make(chan net.Conn, config.MaxSize),
		writePool: make(chan net.Conn, config.MaxSize),
	}

	// Initial connections oluşturma
	for i := 0; i < config.InitialSize; i++ {
		conn, err := pool.factory()
		if err != nil {
			pool.Close()
			return nil, err
		}
		pool.readPool <- conn
	}

	// Idle connection cleaner
	go pool.cleanIdleConnections()

	return pool, nil
}

func (p *ConnectionPool) GetReadConn(ctx context.Context) (net.Conn, error) {
	return p.getConn(ctx, p.readPool)
}

func (p *ConnectionPool) GetWriteConn(ctx context.Context) (net.Conn, error) {
	return p.getConn(ctx, p.writePool)
}

func (p *ConnectionPool) getConn(ctx context.Context, pool chan net.Conn) (net.Conn, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	startTime := time.Now()

	// Connection pooldan alma denemesi
	select {
	case conn := <-pool:
		if !p.isConnAlive(conn) {
			conn.Close()
			return p.createConn()
		}
		p.updateStats(time.Since(startTime))
		return conn, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	default:
		// Pool dolu ise yeni connection oluştur
		if p.stats.active >= p.config.MaxSize {
			// Retry mekanizması
			for i := 0; i < p.config.RetryAttempts; i++ {
				select {
				case conn := <-pool:
					p.updateStats(time.Since(startTime))
					return conn, nil
				case <-time.After(p.config.RetryDelay):
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, errors.New("connection pool is full")
		}

		return p.createConn()
	}
}

func (p *ConnectionPool) ReturnConn(conn net.Conn, pool string) {
	if conn == nil {
		return
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		conn.Close()
		return
	}
	p.mu.RUnlock()

	if !p.isConnAlive(conn) {
		conn.Close()
		p.decrementActive()
		return
	}

	// Connection'ı uygun pool'a geri koyma
	switch pool {
	case "read":
		select {
		case p.readPool <- conn:
			p.incrementIdle()
		default:
			conn.Close()
			p.decrementActive()
		}
	case "write":
		select {
		case p.writePool <- conn:
			p.incrementIdle()
		default:
			conn.Close()
			p.decrementActive()
		}
	}
}

func (p *ConnectionPool) createConn() (net.Conn, error) {
	conn, err := p.factory()
	if err != nil {
		return nil, err
	}

	p.incrementActive()
	return conn, nil
}

func (p *ConnectionPool) isConnAlive(conn net.Conn) bool {
	if tc, ok := conn.(*net.TCPConn); ok {
		if err := tc.SetDeadline(time.Now().Add(time.Second)); err != nil {
			return false
		}

		one := make([]byte, 1)
		if err := tc.SetReadDeadline(time.Now()); err != nil {
			return false
		}

		if _, err := tc.Read(one); err != io.EOF {
			return true
		}
	}
	return false
}

func (p *ConnectionPool) cleanIdleConnections() {
	ticker := time.NewTicker(p.config.IdleTimeout)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.RLock()
		if p.closed {
			p.mu.RUnlock()
			return
		}
		p.mu.RUnlock()

		p.cleanPool(p.readPool)
		p.cleanPool(p.writePool)
	}
}

func (p *ConnectionPool) cleanPool(pool chan net.Conn) {
	for {
		select {
		case conn := <-pool:
			if !p.isConnAlive(conn) {
				conn.Close()
				p.decrementActive()
				p.decrementIdle()
				continue
			}
			// Sağlıklı connection'ı geri koy
			pool <- conn
		default:
			return
		}
	}
}

func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	close(p.readPool)
	close(p.writePool)

	var lastErr error
	// Tüm connectionları kapat
	for conn := range p.readPool {
		if err := conn.Close(); err != nil {
			lastErr = err
		}
	}
	for conn := range p.writePool {
		if err := conn.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// Metric helpers
func (p *ConnectionPool) incrementActive() {
	p.stats.Lock()
	p.stats.active++
	p.stats.Unlock()
}

func (p *ConnectionPool) decrementActive() {
	p.stats.Lock()
	p.stats.active--
	p.stats.Unlock()
}

func (p *ConnectionPool) incrementIdle() {
	p.stats.Lock()
	p.stats.idle++
	p.stats.Unlock()
}

func (p *ConnectionPool) decrementIdle() {
	p.stats.Lock()
	p.stats.idle--
	p.stats.Unlock()
}

func (p *ConnectionPool) updateStats(waitTime time.Duration) {
	p.stats.Lock()
	p.stats.waitTime += waitTime
	p.stats.Unlock()
}

// Stats getter
func (p *ConnectionPool) Stats() (active, idle int, avgWaitTime time.Duration) {
	p.stats.RLock()
	defer p.stats.RUnlock()

	active = p.stats.active
	idle = p.stats.idle
	if active > 0 {
		avgWaitTime = p.stats.waitTime / time.Duration(active)
	}
	return
}
