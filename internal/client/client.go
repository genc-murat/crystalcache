package client

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	ID         int64
	Addr       string
	CreateTime time.Time
	LastCmd    time.Time
	Flags      []string
	DB         int
	Name       string
}

type Manager struct {
	Clients map[int64]*Client // Export field
	NextID  int64
	Ctxs    sync.Map
	Mu      sync.RWMutex // Export field
}

func NewManager() *Manager {
	return &Manager{
		Clients: make(map[int64]*Client), // Use exported field
	}
}

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
	}

	cm.Clients[client.ID] = client
	cm.Ctxs.Store(conn, client)
	return client
}

func (cm *Manager) RemoveClient(conn net.Conn) {
	if ctx, ok := cm.Ctxs.LoadAndDelete(conn); ok {
		client := ctx.(*Client)
		cm.Mu.Lock()
		delete(cm.Clients, client.ID)
		cm.Mu.Unlock()
	}
}

func (cm *Manager) GetClient(conn net.Conn) (*Client, bool) {
	if ctx, ok := cm.Ctxs.Load(conn); ok {
		return ctx.(*Client), true
	}
	return nil, false
}
