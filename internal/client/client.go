package client

import (
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Client represents a connected client.
type Client struct {
	ID         int64        // Unique client ID.
	Addr       string       // Client's network address.
	CreateTime time.Time    // Time when the client connected.
	LastCmd    time.Time    // Time of the last command received from the client.
	Flags      []string     // Client flags (e.g., "N" for normal).
	DB         int          // Currently selected database.
	Name       string       // Client name (set by CLIENT SETNAME).
	conn       net.Conn     // The underlying network connection. // Added connection for potential future use
	mu         sync.RWMutex // Mutex for protecting client-specific data if needed
}

// Manager manages connected clients.
type Manager struct {
	Clients map[int64]*Client // Map of client IDs to Client structs.
	NextID  int64             // Atomic counter for generating unique client IDs.
	// Ctxs maps net.Conn to *Client. Used for quick lookup by connection.
	// Consider renaming to ConnectionToClient or ClientContexts for clarity.
	Ctxs sync.Map
	Mu   sync.RWMutex // Mutex for protecting the Clients map and NextID.
}

// NewManager creates a new client manager.
func NewManager() *Manager {
	return &Manager{
		Clients: make(map[int64]*Client),
		NextID:  0,
	}
}

// AddClient creates a new client, registers it with the manager, and returns it.
func (cm *Manager) AddClient(conn net.Conn) *Client {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	client := &Client{
		ID:         atomic.AddInt64(&cm.NextID, 1),
		Addr:       conn.RemoteAddr().String(),
		CreateTime: time.Now(),
		LastCmd:    time.Now(),
		Flags:      []string{"N"},
		DB:         0,
		Name:       "",
		conn:       conn, // Store the connection
	}

	log.Printf("Adding client: %+v", client)

	cm.Clients[client.ID] = client
	cm.Ctxs.Store(conn, client)
	return client
}

// GetClient retrieves a client based on its network connection.
// Returns the client and a boolean indicating if the client was found.
func (cm *Manager) GetClient(conn net.Conn) (*Client, bool) {
	clientInterface, ok := cm.Ctxs.Load(conn)
	if !ok {
		log.Printf("Client not found for connection: %v", conn)
		return nil, false
	}
	client, ok := clientInterface.(*Client)
	if !ok {
		// This should ideally not happen, but it's a good practice to check type assertions.
		log.Printf("Unexpected type stored in Ctxs: %T", clientInterface)
		return nil, false
	}
	return client, true
}

// GetClientByID retrieves a client based on its ID.
func (cm *Manager) GetClientByID(id int64) (*Client, bool) {
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	client, ok := cm.Clients[id]
	return client, ok
}

// RemoveClient removes a client from the manager.
// It does not close the network connection. The caller is responsible for that.
func (cm *Manager) RemoveClient(conn net.Conn) {
	if ctx, ok := cm.Ctxs.LoadAndDelete(conn); ok {
		client := ctx.(*Client)
		cm.Mu.Lock()
		delete(cm.Clients, client.ID)
		cm.Mu.Unlock()
		log.Printf("Removed client: %d, Addr: %s", client.ID, client.Addr)
	} else {
		log.Printf("Attempted to remove non-existent client for connection: %v", conn)
	}
}

// CloseAllClients closes all client connections and clears the client list.
func (cm *Manager) CloseAllClients() {
	cm.Mu.Lock()
	defer cm.Mu.Unlock()

	cm.Ctxs.Range(func(key, value interface{}) bool {
		if conn, ok := key.(net.Conn); ok {
			conn.Close()
		}
		return true
	})

	// It's important to clear the maps after closing connections to avoid potential issues
	cm.Clients = make(map[int64]*Client)
	cm.Ctxs = sync.Map{}
	log.Println("Closed all client connections and cleared client list.")
}

// SetClientName sets the name of a client.
func (cm *Manager) SetClientName(conn net.Conn, name string) error {
	client, ok := cm.GetClient(conn)
	if !ok {
		return errors.New("client not found")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.Name = name
	return nil
}

// UpdateLastCommandTime updates the last command time of a client.
func (cm *Manager) UpdateLastCommandTime(conn net.Conn) error {
	client, ok := cm.GetClient(conn)
	if !ok {
		return errors.New("client not found")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.LastCmd = time.Now()
	return nil
}

// SetClientDB sets the current database of a client.
func (cm *Manager) SetClientDB(conn net.Conn, db int) error {
	client, ok := cm.GetClient(conn)
	if !ok {
		return errors.New("client not found")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.DB = db
	return nil
}
