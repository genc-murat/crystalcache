package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/internal/handlers"
	"github.com/genc-murat/crystalcache/internal/metrics"
	"github.com/genc-murat/crystalcache/internal/types"
)

type CommandExecutor struct {
	registry *handlers.Registry
	pool     ports.Pool
	metrics  *metrics.Metrics
	timeout  time.Duration
}

func NewCommandExecutor(registry *handlers.Registry, pool ports.Pool, metrics *metrics.Metrics) *CommandExecutor {
	return &CommandExecutor{
		registry: registry,
		pool:     pool,
		metrics:  metrics,
		timeout:  5 * time.Second, // Configurable
	}
}

func (e *CommandExecutor) Execute(ctx context.Context, cmd string, args []models.Value) models.Value {
	startTime := time.Now()
	defer func() {
		e.metrics.AddCommandExecution(cmd, time.Since(startTime))
	}()

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered from panic in command execution: %v\n", r)
		}
	}()

	handler, exists := e.registry.GetHandler(cmd)
	if !exists {
		return models.Value{Type: "error", Str: "ERR unknown command"}
	}

	// Get connection from pool based on command type
	cmdType := types.GetCommandType(cmd)
	var conn net.Conn
	var err error

	switch cmdType {
	case types.ReadCommand:
		conn, err = e.pool.GetReadConn(ctx)
		if err != nil {
			return models.Value{Type: "error", Str: fmt.Sprintf("ERR getting read connection: %v", err)}
		}
		defer e.pool.ReturnConn(conn, "read")

	case types.WriteCommand:
		conn, err = e.pool.GetWriteConn(ctx)
		if err != nil {
			return models.Value{Type: "error", Str: fmt.Sprintf("ERR getting write connection: %v", err)}
		}
		defer e.pool.ReturnConn(conn, "write")
	}

	// Execute command with timeout
	resultCh := make(chan models.Value, 1)
	errCh := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("panic in command execution: %v", r)
			}
		}()

		result := handler(args)
		resultCh <- result
	}()

	select {
	case result := <-resultCh:
		return result
	case err := <-errCh:
		return models.Value{Type: "error", Str: err.Error()}
	case <-time.After(e.timeout):
		return models.Value{Type: "error", Str: "ERR command execution timeout"}
	case <-ctx.Done():
		return models.Value{Type: "error", Str: "ERR command execution cancelled"}
	}
}
