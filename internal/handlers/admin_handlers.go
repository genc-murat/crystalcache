package handlers

import (
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/genc-murat/crystalcache/internal/client"
	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type AdminHandlers struct {
	cache         ports.Cache
	clientManager *client.Manager
	currentConn   net.Conn
	connMu        sync.RWMutex
	currentConnCh chan net.Conn
}

func NewAdminHandlers(cache ports.Cache, clientManager *client.Manager) *AdminHandlers {
	return &AdminHandlers{
		cache:         cache,
		clientManager: clientManager,
		currentConnCh: make(chan net.Conn, 1),
	}
}

func (h *AdminHandlers) HandleConnection(conn net.Conn) {
	select {
	case <-h.currentConnCh: // Clear channel
	default:
	}
	h.currentConnCh <- conn
}

func (h *AdminHandlers) HandleFlushAll(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	h.cache.FlushAll()
	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) HandleInfo(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	info := h.cache.Info()
	var builder strings.Builder

	keys := make([]string, 0, len(info))
	for k := range info {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		builder.WriteString(k)
		builder.WriteString(":")
		builder.WriteString(info[k])
		builder.WriteString("\r\n")
	}

	log.Printf("[DEBUG] handleClientInfo response: %+v", models.Value{Type: "bulk", Bulk: builder.String()})
	return models.Value{Type: "bulk", Bulk: builder.String()}
}

func (h *AdminHandlers) HandleDBSize(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	size := h.cache.DBSize()
	return models.Value{Type: "integer", Num: size}
}

func (h *AdminHandlers) HandleMulti(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	err := h.cache.Multi()
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) HandleExec(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	results, err := h.cache.Exec()
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "array", Array: results}
}

func (h *AdminHandlers) HandleDiscard(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	err := h.cache.Discard()
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) HandleWatch(args []models.Value) models.Value {
	if err := util.ValidateMinArgs(args, 1); err != nil {
		return util.ToValue(err)
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	err := h.cache.Watch(keys...)
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) HandleUnwatch(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 0); err != nil {
		return util.ToValue(err)
	}

	err := h.cache.Unwatch()
	if err != nil {
		return util.ToValue(err)
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) HandleClient(args []models.Value) models.Value {
	if len(args) == 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'client' command"}
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	switch subCmd {
	case "LIST":
		return h.handleClientList()
	case "KILL":
		if len(args) != 2 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'client kill' command"}
		}
		return h.handleClientKill(args[1].Bulk)
	case "ID":
		return h.handleClientID()
	case "INFO":
		return h.handleClientInfo()
	case "SETNAME":
		if len(args) != 2 {
			return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'client setname' command"}
		}
		return h.handleClientSetName(args[1].Bulk)

	default:
		return models.Value{Type: "error", Str: "ERR unknown subcommand for 'client'"}
	}
}

func (h *AdminHandlers) handleClientList() models.Value {
	var builder strings.Builder
	now := time.Now()

	h.clientManager.Mu.RLock()
	for _, client := range h.clientManager.Clients {
		builder.WriteString(fmt.Sprintf("id=%d addr=%s age=%d idle=%d flags=%s db=%d\n",
			client.ID,
			client.Addr,
			int(now.Sub(client.CreateTime).Seconds()),
			int(now.Sub(client.LastCmd).Seconds()),
			strings.Join(client.Flags, ""),
			client.DB))
	}
	h.clientManager.Mu.RUnlock()

	return models.Value{Type: "bulk", Bulk: builder.String()}
}

func (h *AdminHandlers) handleClientKill(addr string) models.Value {
	var killed bool
	h.clientManager.Mu.Lock()
	for id, client := range h.clientManager.Clients {
		if client.Addr == addr {
			delete(h.clientManager.Clients, id)
			killed = true
			break
		}
	}
	h.clientManager.Mu.Unlock()

	if killed {
		return models.Value{Type: "string", Str: "OK"}
	}
	return models.Value{Type: "error", Str: "ERR No such client"}
}

func (h *AdminHandlers) handleClientID() models.Value {
	conn := h.getCurrentConn()
	if conn == nil {
		return models.Value{Type: "integer", Num: 0}
	}
	client, exists := h.clientManager.GetClient(conn)
	if !exists {
		return models.Value{Type: "integer", Num: 0}
	}
	return models.Value{Type: "integer", Num: int(client.ID)}
}

func (h *AdminHandlers) handleClientInfo() models.Value {
	conn := h.getCurrentConn()
	if conn == nil {
		return models.Value{Type: "null"}
	}
	client, exists := h.clientManager.GetClient(conn)
	if !exists {
		return models.Value{Type: "null"}
	}

	now := time.Now()
	info := fmt.Sprintf(
		"id=%d\r\naddr=%s\r\nname=%s\r\nage=%d\r\nidle=%d\r\nflags=%s\r\ndb=%d\r\n",
		client.ID,
		client.Addr,
		client.Name,
		int(now.Sub(client.CreateTime).Seconds()),
		int(now.Sub(client.LastCmd).Seconds()),
		strings.Join(client.Flags, ""),
		client.DB,
	)

	return models.Value{Type: "bulk", Bulk: info}
}

func (h *AdminHandlers) handleClientSetName(name string) models.Value {
	conn := h.getCurrentConn()
	if conn == nil {
		return models.Value{Type: "error", Str: "ERR no current client connection"}
	}

	client, exists := h.clientManager.GetClient(conn)
	if !exists {
		client = h.clientManager.AddClient(conn)
	}

	client.Name = name

	log.Printf("[DEBUG] Response from handleClientSetName: %+v", models.Value{Type: "string", Str: "OK"})
	return models.Value{Type: "string", Str: "OK"}
}

func (h *AdminHandlers) SetCurrentConn(conn net.Conn) {
	h.connMu.Lock()
	defer h.connMu.Unlock()
	h.currentConn = conn
}

func (h *AdminHandlers) getCurrentConn() net.Conn {
	h.connMu.RLock()
	defer h.connMu.RUnlock()
	select {
	case conn := <-h.currentConnCh:
		h.currentConnCh <- conn // Put it back
		return conn
	default:
		return h.currentConn
	}
}
