package server

import (
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/genc-murat/crystalcache/internal/client"
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
	metrics  *metrics.Metrics
	registry *handlers.Registry

	// Shutdown coordination
	shutdown chan struct{}
	wg       sync.WaitGroup

	clientManager *client.Manager
	adminHandlers *handlers.AdminHandlers
	activeConns   sync.Map
}

type ServerConfig struct {
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	IdleTimeout    time.Duration
}

func NewServer(cache ports.Cache, storage ports.Storage, pool ports.Pool, config ServerConfig) *Server {
	metrics := metrics.NewMetrics()
	clientManager := client.NewManager()
	registry := handlers.NewRegistry(cache, clientManager)
	adminHandlers := handlers.NewAdminHandlers(cache, clientManager)

	return &Server{
		cache:         cache,
		storage:       storage,
		pool:          pool,
		metrics:       metrics,
		registry:      registry,
		shutdown:      make(chan struct{}),
		clientManager: clientManager,
		adminHandlers: adminHandlers,
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

	})
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer s.wg.Done()

	s.adminHandlers.HandleConnection(conn)
	client := s.clientManager.AddClient(conn)
	defer s.clientManager.RemoveClient(conn)

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	for {
		value, err := reader.Read()
		if err != nil {
			return
		}

		if value.Type != "array" || len(value.Array) == 0 {
			continue
		}

		s.adminHandlers.SetCurrentConn(conn)
		result := s.handleCommand(value)
		client.LastCmd = time.Now()

		if err := writer.Write(result); err != nil {
			return
		}
	}
}

func (s *Server) handleCommand(value models.Value) models.Value {
	if len(value.Array) == 0 {
		return models.Value{Type: "error", Str: "ERR empty command"}
	}

	cmd := strings.ToUpper(value.Array[0].Bulk)
	log.Printf("[DEBUG] Received command: %s", strings.ToUpper(value.Array[0].Bulk))
	handler, exists := s.registry.GetHandler(cmd)
	if !exists {
		return models.Value{Type: "error", Str: "ERR unknown command"}
	}

	// Preserve connection context for admin commands
	if cmd == "CLIENT" {
		return s.adminHandlers.HandleClient(value.Array[1:])
	}

	return handler(value.Array[1:])
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
