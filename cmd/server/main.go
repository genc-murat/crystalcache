package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/genc-murat/crystalcache/internal/cache"
	"github.com/genc-murat/crystalcache/internal/config"
	"github.com/genc-murat/crystalcache/internal/pool"
	"github.com/genc-murat/crystalcache/internal/server"
	"github.com/genc-murat/crystalcache/internal/storage"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("development")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	// Start pprof server if enabled
	if cfg.Pprof.Enabled {
		go func() {
			log.Printf("Pprof server starting on :%d", cfg.Pprof.Port)
			if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Pprof.Port), nil); err != nil {
				log.Printf("Pprof server error: %v", err)
			}
		}()
	}

	// Initialize cache
	memCache := cache.NewMemoryCache()
	memCache.StartDefragmentation(cfg.Cache.DefragInterval, cfg.Cache.DefragThreshold)

	// Initialize storage
	aofConfig := storage.DefaultAOFConfig()
	aofConfig.Path = cfg.Storage.Path
	aofConfig.SyncInterval = cfg.Storage.SyncInterval

	aofStorage, err := storage.NewAOF(aofConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer aofStorage.Close()

	// Initialize server
	serverConfig := server.ServerConfig{
		MaxConnections: cfg.Server.MaxConnections,
		ReadTimeout:    cfg.Server.ReadTimeout,
		WriteTimeout:   cfg.Server.WriteTimeout,
		IdleTimeout:    cfg.Server.IdleTimeout,
	}

	server := server.NewServer(memCache, aofStorage, nil, serverConfig)
	server.SetMaster(true)
	go server.Start(fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port))
	time.Sleep(1 * time.Second)

	// Initialize connection pool
	poolConfig := pool.Config{
		InitialSize:   cfg.Pool.InitialSize,
		MaxSize:       cfg.Pool.MaxSize,
		ReadTimeout:   cfg.Pool.ReadTimeout,
		WriteTimeout:  cfg.Pool.WriteTimeout,
		IdleTimeout:   cfg.Pool.IdleTimeout,
		RetryAttempts: cfg.Pool.RetryAttempts,
		RetryDelay:    cfg.Pool.RetryDelay,
	}

	factory := pool.NewConnFactory(fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port), 5*time.Second)
	connectionPool, err := pool.NewConnectionPool(poolConfig, factory.CreateConnection)
	if err != nil {
		log.Fatal(err)
	}
	defer connectionPool.Close()

	server.SetConnectionPool(connectionPool)

	// Start metrics server if enabled
	if cfg.Metrics.Enabled {
		go func() {
			http.HandleFunc(cfg.Metrics.Path, func(w http.ResponseWriter, r *http.Request) {
				metrics := server.GetMetrics()
				json.NewEncoder(w).Encode(metrics)
			})
			log.Printf("Metrics server starting on :%d", cfg.Metrics.Port)
			if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Metrics.Port), nil); err != nil {
				log.Printf("Metrics server error: %v", err)
			}
		}()
	}

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}
