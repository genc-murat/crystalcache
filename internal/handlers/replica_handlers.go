package handlers

import (
	"fmt"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/util"
)

type ReplicaHandlers struct {
	cache  ports.Cache
	server ports.Server
}

func NewReplicaHandlers(cache ports.Cache, server ports.Server) *ReplicaHandlers {
	return &ReplicaHandlers{
		cache:  cache,
		server: server,
	}
}

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
