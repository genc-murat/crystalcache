package handlers

import (
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type ClusterHandlers struct {
	cache ports.Cache
}

// NewClusterHandlers creates a new instance of ClusterHandlers with the provided cache.
// It takes a ports.Cache as an argument and returns a pointer to ClusterHandlers.
func NewClusterHandlers(cache ports.Cache) *ClusterHandlers {
	return &ClusterHandlers{
		cache: cache,
	}
}

// HandleCluster processes cluster-related commands and returns the appropriate response.
// It expects a slice of models.Value as arguments, where the first argument is the subcommand.
// Supported subcommands are:
// - "INFO": Returns the cluster state.
// - "NODES": Returns an empty bulk string.
// - "SLOTS": Returns an empty array.
// If the subcommand is not recognized, it returns an error message indicating the unknown subcommand.
//
// Args:
//
//	args ([]models.Value): A slice of models.Value representing the command arguments.
//
// Returns:
//
//	models.Value: The response based on the subcommand, which can be of type "bulk", "array", or "error".
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
