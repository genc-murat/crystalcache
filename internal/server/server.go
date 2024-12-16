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
	"github.com/genc-murat/crystalcache/internal/core/acl"
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

	aclManager    *acl.ACLManager
	aclMiddleware *acl.Middleware
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

	aclManager := acl.NewACLManager()
	// Create default user with full permissions if it doesn't exist
	err := aclManager.SetUser("user default on nopass ~* +@all")
	if err != nil {
		log.Printf("Warning: Failed to create default user: %v", err)
	}

	aclMiddleware := acl.NewMiddleware(aclManager)

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
		aclManager:    aclManager,
		aclMiddleware: aclMiddleware,
	}
}

func isWriteCommand(cmd string) bool {
	writeCommands := map[string]bool{
		// String Commands
		"SET":         true,
		"MSET":        true,
		"MSETNX":      true,
		"APPEND":      true,
		"INCR":        true,
		"INCRBY":      true,
		"INCRBYFLOAT": true,
		"DECR":        true,
		"DECRBY":      true,
		"GETSET":      true,
		"SETRANGE":    true,

		// Key Commands
		"DEL":       true,
		"UNLINK":    true,
		"EXPIRE":    true,
		"EXPIREAT":  true,
		"PEXPIRE":   true,
		"PEXPIREAT": true,

		// List Commands
		"RPUSH":   true,
		"LPUSH":   true,
		"RPUSHX":  true,
		"LPUSHX":  true,
		"RPOP":    true,
		"LPOP":    true,
		"LSET":    true,
		"LTRIM":   true,
		"LINSERT": true,
		"LREM":    true,
		"BLPOP":   true,
		"BRPOP":   true,
		"LMOVE":   true,
		"BLMOVE":  true,

		// Set Commands
		"SADD":        true,
		"SREM":        true,
		"SPOP":        true,
		"SMOVE":       true,
		"SINTERSTORE": true,
		"SUNIONSTORE": true,
		"SDIFFSTORE":  true,

		// Sorted Set Commands
		"ZADD":             true,
		"ZREM":             true,
		"ZINCRBY":          true,
		"ZREMRANGEBYRANK":  true,
		"ZREMRANGEBYSCORE": true,
		"ZREMRANGEBYLEX":   true,
		"ZINTERSTORE":      true,
		"ZUNIONSTORE":      true,
		"ZDIFFSTORE":       true,
		"ZPOPMIN":          true,
		"ZPOPMAX":          true,
		"BZPOPMIN":         true,
		"BZPOPMAX":         true,
		"ZRANGESTORE":      true,

		// Hash Commands
		"HSET":         true,
		"HSETNX":       true,
		"HMSET":        true,
		"HDEL":         true,
		"HINCRBY":      true,
		"HINCRBYFLOAT": true,

		// Stream Commands
		"XADD":       true,
		"XDEL":       true,
		"XTRIM":      true,
		"XSETID":     true,
		"XGROUP":     true,
		"XACK":       true,
		"XCLAIM":     true,
		"XAUTOCLAIM": true,

		// Bitmap Commands
		"SETBIT":   true,
		"BITOP":    true,
		"BITFIELD": true,

		// JSON Commands
		"JSON.SET":       true,
		"JSON.DEL":       true,
		"JSON.ARRAPPEND": true,
		"JSON.ARRINSERT": true,
		"JSON.ARRTRIM":   true,
		"JSON.ARRPOP":    true,
		"JSON.STRAPPEND": true,
		"JSON.NUMINCRBY": true,
		"JSON.NUMMULTBY": true,
		"JSON.CLEAR":     true,
		"JSON.MERGE":     true,
		"JSON.MSET":      true,

		// Admin Commands
		"FLUSHALL": true,
		"FLUSHDB":  true,

		// Transaction Commands
		"MULTI": true,
		"EXEC":  true,
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

		// Get command name
		cmd := strings.ToUpper(value.Array[0].Bulk)

		// Get command handler
		handler, exists := s.registry.GetHandler(cmd)
		if !exists {
			log.Printf("Unknown command in AOF: %s", cmd)
			return
		}

		// Execute command
		handler(value.Array[1:])
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

// Server struct'ındaki handleConnection metodunu güncelleyelim
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer s.wg.Done()

	s.adminHandlers.HandleConnection(conn)
	client := s.clientManager.AddClient(conn)
	defer s.clientManager.RemoveClient(conn)

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	// Set default authentication state
	authenticated := true // Default user is authenticated by default
	username := "default" // Use default username

	for {
		value, err := reader.Read()
		if err != nil {
			return
		}

		if value.Type != "array" || len(value.Array) == 0 {
			continue
		}

		cmd := strings.ToUpper(value.Array[0].Bulk)

		// Special handling for AUTH command
		if cmd == "AUTH" {
			switch len(value.Array) {
			case 2: // Old style auth with just password
				password := value.Array[1].Bulk
				if s.aclManager.Authenticate("default", password) {
					authenticated = true
					username = "default"
					writer.Write(models.Value{Type: "string", Str: "OK"})
				} else {
					authenticated = false
					writer.Write(models.Value{Type: "error", Str: "ERR invalid password"})
				}
				continue
			case 3: // New style auth with username and password
				username = value.Array[1].Bulk
				password := value.Array[2].Bulk
				if s.aclManager.Authenticate(username, password) {
					authenticated = true
					writer.Write(models.Value{Type: "string", Str: "OK"})
				} else {
					authenticated = false
					writer.Write(models.Value{Type: "error", Str: "ERR invalid username or password"})
				}
				continue
			default:
				writer.Write(models.Value{Type: "error", Str: "ERR wrong number of arguments for AUTH"})
				continue
			}
		}

		// Allow PING without authentication
		if cmd == "PING" {
			writer.Write(models.Value{Type: "string", Str: "PONG"})
			continue
		}

		// Allow INFO without authentication
		if cmd == "INFO" {
			result := s.handleCommand(value)
			writer.Write(result)
			continue
		}

		// Handle commands that don't require authentication
		if !requiresAuth(cmd) {
			result := s.handleCommand(value)
			writer.Write(result)
			continue
		}

		// Check authentication state
		if !authenticated {
			writer.Write(models.Value{Type: "error", Str: "NOAUTH Authentication required."})
			continue
		}

		// Check permissions for authenticated users
		if !s.aclMiddleware.CheckCommand(username, value) {
			writer.Write(models.Value{Type: "error", Str: "NOPERM insufficient permissions"})
			continue
		}

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

// Helper function to determine if a command requires authentication
func requiresAuth(cmd string) bool {
	noAuthCommands := map[string]bool{
		"PING": false,
		"AUTH": false,
		"INFO": false,
	}
	return !noAuthCommands[cmd]
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
			info[fmt.Sprintf("slave%s", i)] = fmt.Sprintf("ip=%s,state=online",
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

	// Handle regular commands
	handler, exists := s.registry.GetHandler(cmd)
	if !exists {
		return models.Value{Type: "error", Str: "ERR unknown command"}
	}

	// If we're a slave, only allow read commands
	if !s.isMaster && isWriteCommand(cmd) && !isReplicationCommand(cmd) {
		return models.Value{Type: "error", Str: "READONLY You can't write against a read only replica"}
	}

	// Execute command
	result := handler(value.Array[1:])

	// If master and write command, persist to AOF and propagate
	if s.isMaster && isWriteCommand(cmd) {
		// Write to AOF
		if err := s.storage.Write(value); err != nil {
			log.Printf("Failed to write to AOF: %v", err)
		}

		// Propagate to replicas
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

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		// Close AOF storage
		if err := s.storage.Close(); err != nil {
			log.Printf("Error closing AOF: %v", err)
		}
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
