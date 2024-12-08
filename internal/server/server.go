package server

import (
	"bufio"
	"context"
	"fmt"
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
	util "github.com/genc-murat/crystalcache/pkg/utils"
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

	// Replica
	isMaster   bool
	masterHost string
	masterPort string
	replConn   net.Conn
	replReader *resp.Reader
	replWriter *resp.Writer
	replChan   chan models.Value
	replMutex  sync.RWMutex
	replicas   map[string]*replica
}

type replica struct {
	conn   net.Conn
	writer *resp.Writer
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
		isMaster:      true,
		replicas:      make(map[string]*replica),
	}
}

func isWriteCommand(cmd string) bool {
	writeCommands := map[string]bool{
		"SET":      true,
		"DEL":      true,
		"INCR":     true,
		"DECR":     true,
		"RPUSH":    true,
		"LPUSH":    true,
		"RPOP":     true,
		"LPOP":     true,
		"SADD":     true,
		"SREM":     true,
		"ZADD":     true,
		"ZREM":     true,
		"HSET":     true,
		"HDEL":     true,
		"EXPIRE":   true,
		"FLUSHALL": true,
		"FLUSHDB":  true,
		"JSON.SET": true,
		"JSON.DEL": true,
		"MULTI":    true,
		"EXEC":     true,
	}
	return writeCommands[cmd]
}

// Add propagation methods
func (s *Server) propagateToReplicas(cmd models.Value) {
	s.replMutex.RLock()
	defer s.replMutex.RUnlock()

	for addr, replica := range s.replicas {
		err := replica.writer.Write(cmd)
		if err != nil {
			log.Printf("Error propagating to replica %s: %v", addr, err)
			// Handle disconnected replica
			s.removeReplica(addr)
		}
	}
}

func (s *Server) addReplica(conn net.Conn) {
	addr := conn.RemoteAddr().String()
	s.replMutex.Lock()
	defer s.replMutex.Unlock()

	s.replicas[addr] = &replica{
		conn:   conn,
		writer: resp.NewWriter(conn),
	}
	log.Printf("New replica connected from %s", addr)
}

func (s *Server) removeReplica(addr string) {
	s.replMutex.Lock()
	defer s.replMutex.Unlock()

	if r, exists := s.replicas[addr]; exists {
		r.conn.Close()
		delete(s.replicas, addr)
		log.Printf("Replica %s disconnected", addr)
	}
}

func (s *Server) StartReplication(host, port string) error {
	s.replMutex.Lock()
	defer s.replMutex.Unlock()

	// Close existing replication connection if any
	if s.replConn != nil {
		s.replConn.Close()
	}

	// Connect to master
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", host, port), 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %v", err)
	}

	s.replConn = conn
	s.replReader = resp.NewReader(conn)
	s.replWriter = resp.NewWriter(conn)
	s.masterHost = host
	s.masterPort = port
	s.isMaster = false
	s.replChan = make(chan models.Value, 1000)

	// Start replication goroutine
	go s.handleReplication()

	// Send PING to verify connection
	err = s.replWriter.Write(models.Value{Type: "string", Str: "PING"})
	if err != nil {
		return fmt.Errorf("failed to ping master: %v", err)
	}

	// Start full sync
	go s.fullSync()

	return nil
}

func (s *Server) StopReplication() {
	s.replMutex.Lock()
	defer s.replMutex.Unlock()

	if s.replConn != nil {
		s.replConn.Close()
		s.replConn = nil
	}

	s.masterHost = ""
	s.masterPort = ""
	s.isMaster = true
}

func (s *Server) handleReplication() {
	for {
		// Read command from master
		value, err := s.replReader.Read()
		if err != nil {
			log.Printf("Replication error: %v", err)
			s.StopReplication()
			return
		}

		// Process command
		if value.Type == "array" && len(value.Array) > 0 {
			cmd := strings.ToUpper(value.Array[0].Bulk)
			// Skip certain commands in replica mode
			if cmd != "INFO" && cmd != "REPLCONF" {
				s.handleCommand(value)
			}
		}
	}
}

func (s *Server) fullSync() {
	// Request full sync from master
	err := s.replWriter.Write(models.Value{
		Type: "array",
		Array: []models.Value{
			{Type: "bulk", Bulk: "SYNC"},
		},
	})
	if err != nil {
		log.Printf("Failed to request sync: %v", err)
		return
	}

	// Clear existing data
	s.cache.FlushAll()

	// Read and apply data from master
	for {
		value, err := s.replReader.Read()
		if err != nil {
			log.Printf("Sync error: %v", err)
			return
		}

		if value.Type == "string" && value.Str == "SYNC-END" {
			break
		}

		s.handleCommand(value)
	}
}

func (s *Server) IsMaster() bool {
	s.replMutex.RLock()
	defer s.replMutex.RUnlock()
	return s.isMaster
}

func (s *Server) GetMasterInfo() (string, string) {
	s.replMutex.RLock()
	defer s.replMutex.RUnlock()
	return s.masterHost, s.masterPort
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

func (s *Server) SetMaster(isMaster bool) {
	s.replMutex.Lock()
	defer s.replMutex.Unlock()
	s.isMaster = isMaster
}

func (s *Server) GetReplicaCount() int {
	s.replMutex.RLock()
	defer s.replMutex.RUnlock()
	return len(s.replicas)
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

		cmd := strings.ToUpper(value.Array[0].Bulk)

		// Handle REPLCONF command for replica identification
		if cmd == "REPLCONF" {
			s.addReplica(conn)
			writer.Write(models.Value{Type: "string", Str: "OK"})
			continue
		}

		s.adminHandlers.SetCurrentConn(conn)
		result := s.handleCommand(value)

		// Propagate write commands to replicas if we're the master
		if s.IsMaster() && isWriteCommand(cmd) {
			s.propagateToReplicas(value)
		}

		client.LastCmd = time.Now()

		if err := writer.Write(result); err != nil {
			return
		}
	}
}

func (s *Server) GetReplicationInfo() map[string]string {
	info := make(map[string]string)

	s.replMutex.RLock()
	defer s.replMutex.RUnlock()

	if s.isMaster {
		info["role"] = "master"
		info["connected_slaves"] = fmt.Sprintf("%d", len(s.replicas))

		// Add information about each connected replica
		for i, replica := range s.replicas {
			info[fmt.Sprintf("slave%d", i)] = fmt.Sprintf("ip=%s,state=online",
				replica.conn.RemoteAddr().String())
		}
	} else {
		info["role"] = "slave"
		info["master_host"] = s.masterHost
		info["master_port"] = s.masterPort
		info["master_link_status"] = "up"

		if s.replConn != nil {
			info["slave_read_only"] = "yes"
			info["connected_to_master"] = "yes"
		} else {
			info["master_link_status"] = "down"
			info["connected_to_master"] = "no"
		}
	}

	return info
}

func (s *Server) handleCommand(value models.Value) models.Value {
	if len(value.Array) == 0 {
		return models.Value{Type: "error", Str: "ERR empty command"}
	}

	cmd := strings.ToUpper(value.Array[0].Bulk)
	log.Printf("[DEBUG] Received command: %s with args: %+v", cmd, value.Array[1:])

	// Special handling for INFO command to combine cache and replication info
	if len(value.Array) == 0 {
		return models.Value{Type: "error", Str: "ERR empty command"}
	}

	// Special handling for INFO command to combine cache and replication info
	if cmd == "INFO" {
		cacheInfo := s.adminHandlers.HandleInfo(value.Array[1:])
		if cacheInfo.Type == "error" {
			return cacheInfo
		}

		// Parse the cache info string back into a map
		cacheInfoMap := parseInfoString(cacheInfo.Bulk)

		// Merge with replication info
		replInfo := s.GetReplicationInfo()
		for k, v := range replInfo {
			cacheInfoMap[k] = v
		}

		return models.Value{
			Type: "bulk",
			Bulk: util.FormatInfoResponse(cacheInfoMap),
		}
	}

	// Handle regular commands
	handler, exists := s.registry.GetHandler(cmd)
	if !exists {
		return models.Value{Type: "error", Str: "ERR unknown command"}
	}

	// If we're a slave, only allow read commands unless it's a replication command
	if !s.isMaster && isWriteCommand(cmd) && !isReplicationCommand(cmd) {
		return models.Value{Type: "error", Str: "READONLY You can't write against a read only replica"}
	}

	result := handler(value.Array[1:])

	// Propagate write commands to replicas if we're the master
	if s.isMaster && isWriteCommand(cmd) {
		s.propagateToReplicas(value)
	}

	return result
}

func parseInfoString(info string) map[string]string {
	result := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(info))
	for scanner.Scan() {
		line := scanner.Text()
		if parts := strings.SplitN(line, ":", 2); len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

func isReplicationCommand(cmd string) bool {
	replicationCommands := map[string]bool{
		"REPLICAOF": true,
		"REPLCONF":  true,
		"SYNC":      true,
	}
	return replicationCommands[cmd]
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
