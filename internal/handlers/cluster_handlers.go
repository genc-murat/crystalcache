package handlers

import (
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type ClusterHandlers struct {
	cache ports.Cache
}

func NewClusterHandlers(cache ports.Cache) *ClusterHandlers {
	return &ClusterHandlers{
		cache: cache,
	}
}

func (h *ClusterHandlers) HandleCluster(args []models.Value) models.Value {
	if len(args) == 0 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'cluster' command"}
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	switch subCmd {
	case "INFO":
		return models.Value{Type: "bulk", Bulk: "cluster_state:fail"}

	case "NODES":
		return models.Value{Type: "bulk", Bulk: ""}

	case "SLOTS":
		return models.Value{Type: "array", Array: []models.Value{}}

	default:
		return models.Value{Type: "error", Str: "ERR unknown subcommand '" + subCmd + "'. Try CLUSTER HELP."}
	}
}
