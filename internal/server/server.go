package server

import (
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/handlers"
	"github.com/genc-murat/crystalcache/internal/metrics"
	"github.com/genc-murat/crystalcache/pkg/resp"
)

type Server struct {
	cache    ports.Cache
	storage  ports.Storage
	pool     ports.Pool
	executor *CommandExecutor
	metrics  *metrics.Metrics
	registry *handlers.Registry

	// Shutdown coordination
	shutdown chan struct{}
	wg       sync.WaitGroup
}

type ServerConfig struct {
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	IdleTimeout    time.Duration
}

func NewServer(cache ports.Cache, storage ports.Storage, pool ports.Pool, config ServerConfig) *Server {
	metrics := metrics.NewMetrics()
	registry := handlers.NewRegistry(cache)

	return &Server{
		cache:    cache,
		storage:  storage,
		pool:     pool,
		metrics:  metrics,
		registry: registry,
		executor: NewCommandExecutor(registry, pool, metrics),
		shutdown: make(chan struct{}),
	}
}

func (s *Server) SetConnectionPool(pool ports.Pool) {
	s.pool = pool
}

func (s *Server) Start(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()

	// Load data from storage
	if err := s.loadData(); err != nil {
		return err
	}

	log.Printf("Server listening on %s", address)

	// Accept connections
	for {
		select {
		case <-s.shutdown:
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}

			s.wg.Add(1)
			go s.handleConnection(conn)
		}
	}
}

func (s *Server) loadData() error {
	return s.storage.Read(func(value models.Value) {
		if len(value.Array) == 0 {
			return
		}

		cmd := strings.ToUpper(value.Array[0].Bulk)
		s.executor.Execute(context.Background(), cmd, value.Array[1:])
	})
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.wg.Done()
	}()

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	for {
		select {
		case <-s.shutdown:
			return
		default:
			value, err := reader.Read()
			if err != nil {
				log.Printf("Error reading from connection: %v", err)
				return
			}

			if value.Type != "array" || len(value.Array) == 0 {
				continue
			}

			cmd := strings.ToUpper(value.Array[0].Bulk)
			result := s.executor.Execute(context.Background(), cmd, value.Array[1:])

			if err := writer.Write(result); err != nil {
				log.Printf("Error writing response: %v", err)
				return
			}
		}
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	close(s.shutdown)

	// Wait for goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) GetMetrics() map[string]interface{} {
	return s.metrics.GetStats()
}
