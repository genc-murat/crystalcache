package handlers

import (
	"fmt"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

// ReplicaHandlers implements handlers for replication-related operations in the cache
// Manages master-replica relationships between cache instances
type ReplicaHandlers struct {
	cache  ports.Cache
	server ports.Server
}

// NewReplicaHandlers creates a new instance of ReplicaHandlers
// Parameters:
//   - cache: The cache implementation to be used for replication operations
//   - server: The server instance that manages replication connections
//
// Returns:
//   - *ReplicaHandlers: A pointer to the newly created ReplicaHandlers instance
func NewReplicaHandlers(cache ports.Cache, server ports.Server) *ReplicaHandlers {
	return &ReplicaHandlers{
		cache:  cache,
		server: server,
	}
}

// HandleReplicaOf handles the REPLICAOF command which configures the current server
// as a replica of another server or stops replication
//
// Special case: "REPLICAOF NO ONE" stops replication and promotes the replica to master
//
// Parameters:
//   - args: Array of Values containing:
//   - host: The master server's hostname or "NO" to stop replication
//   - port: The master server's port or "ONE" to stop replication
//
// Returns:
//   - models.Value: "OK" on successful configuration or stopping of replication
//     Returns error if:
//   - Wrong number of arguments
//   - Invalid host/port combination
//   - Connection to master fails
func (h *ReplicaHandlers) HandleReplicaOf(args []models.Value) models.Value {
	if err := util.ValidateArgs(args, 2); err != nil {
		return util.ToValue(err)
	}

	host := args[0].Bulk
	port := args[1].Bulk

	// Handle REPLICAOF NO ONE command
	if strings.ToUpper(host) == "NO" && strings.ToUpper(port) == "ONE" {
		h.server.StopReplication()
		return models.Value{Type: "string", Str: "OK"}
	}

	// Start replication
	err := h.server.StartReplication(host, port)
	if err != nil {
		return models.Value{Type: "error", Str: fmt.Sprintf("ERR %v", err)}
	}

	return models.Value{Type: "string", Str: "OK"}
}
