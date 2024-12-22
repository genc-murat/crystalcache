package handlers

import (
	"fmt"
	"log"
	"net"
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
	info := h.cache.Info()

	// Get module list
	modules := []string{
		"json_native",
		"geo",
		"suggestion",
		"cms",
		"cuckoo",
		"tdigest", // Add tdigest module
	}
	modulesEnabled := make([]string, 0)

	for _, module := range modules {
		modulesEnabled = append(modulesEnabled, fmt.Sprintf("%s", module))
	}

	// Update modules string
	info["modules"] = strings.Join(modulesEnabled, ",")

	// Module versions and details
	info["module_list"] = "" // Initialize empty module list
	moduleDetails := []string{
		"name=json_native,ver=1.0,api=1.0",
		"name=geo,ver=1.0,api=1.0",
		"name=suggestion,ver=1.0,api=1.0",
		"name=cms,ver=1.0,api=1.0",
		"name=cuckoo,ver=1.0,api=1.0",
		"name=tdigest,ver=1.0,api=1.0", // Add tdigest details
	}
	info["module_list"] = strings.Join(moduleDetails, ",")

	// Convert info map to response format
	var response []string
	response = append(response, "# Server")
	response = append(response, fmt.Sprintf("redis_version:%s", info["redis_version"]))
	response = append(response, fmt.Sprintf("redis_mode:%s", info["redis_mode"]))
	response = append(response, fmt.Sprintf("uptime_in_seconds:%s", info["uptime_in_seconds"]))

	response = append(response, "\n# Modules")
	response = append(response, fmt.Sprintf("module_list:%s", info["module_list"]))

	response = append(response, "\n# Memory")
	response = append(response, fmt.Sprintf("used_memory:%s", info["used_memory"]))
	response = append(response, fmt.Sprintf("used_memory_human:%s", info["used_memory_human"]))
	response = append(response, fmt.Sprintf("used_memory_peak:%s", info["used_memory_peak"]))
	response = append(response, fmt.Sprintf("used_memory_peak_human:%s", info["used_memory_peak_human"]))
	response = append(response, fmt.Sprintf("mem_fragmentation_ratio:%s", info["mem_fragmentation_ratio"]))
	response = append(response, fmt.Sprintf("mem_fragmentation_bytes:%s", info["mem_fragmentation_bytes"]))

	response = append(response, "\n# Stats")
	response = append(response, fmt.Sprintf("total_commands_processed:%s", info["total_commands_processed"]))
	response = append(response, fmt.Sprintf("total_keys:%s", info["total_keys"]))
	response = append(response, fmt.Sprintf("string_keys:%s", info["string_keys"]))
	response = append(response, fmt.Sprintf("hash_keys:%s", info["hash_keys"]))
	response = append(response, fmt.Sprintf("list_keys:%s", info["list_keys"]))
	response = append(response, fmt.Sprintf("set_keys:%s", info["set_keys"]))
	response = append(response, fmt.Sprintf("zset_keys:%s", info["zset_keys"]))
	response = append(response, fmt.Sprintf("stream_keys:%s", info["stream_keys"]))
	response = append(response, fmt.Sprintf("json_keys:%s", info["json_keys"]))
	response = append(response, fmt.Sprintf("bitmap_keys:%s", info["bitmap_keys"]))
	response = append(response, fmt.Sprintf("suggestion_keys:%s", info["suggestion_keys"]))
	response = append(response, fmt.Sprintf("geo_keys:%s", info["geo_keys"]))
	response = append(response, fmt.Sprintf("cms_keys:%s", info["cms_keys"]))
	response = append(response, fmt.Sprintf("cuckoo_keys:%s", info["cuckoo_keys"]))
	response = append(response, fmt.Sprintf("tdigest_keys:%s", info["tdigest_keys"])) // Add tdigest keys count

	return models.Value{
		Type: "string",
		Str:  strings.Join(response, "\n"),
	}
}

func (h *AdminHandlers) HandlePing(args []models.Value) models.Value {
	// If no argument is provided, return PONG
	if len(args) == 0 {
		return models.Value{Type: "string", Str: "PONG"}
	}

	// If one argument is provided, echo it back
	if len(args) == 1 {
		return args[0]
	}

	// If more arguments are provided, return error
	return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'ping' command"}
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
