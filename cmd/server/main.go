package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/genc-murat/crystalcache/internal/cache"
	"github.com/genc-murat/crystalcache/internal/pool"
	"github.com/genc-murat/crystalcache/internal/server"
	"github.com/genc-murat/crystalcache/internal/storage"
	// _ "net/http/pprof"
)

func main() {

	// go func() {
	// 	log.Println("Pprof server starting on :6060")
	// 	if err := http.ListenAndServe(":6060", nil); err != nil {
	// 		log.Printf("Pprof server error: %v", err)
	// 	}
	// }()

	// _, err := config.LoadConfig("development")
	// if err != nil {
	// 	log.Fatalf("Error loading config: %v", err)
	// }

	// Server config
	serverConfig := server.ServerConfig{
		MaxConnections: 1000,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    60 * time.Second,
	}

	memCache := cache.NewMemoryCache()
	memCache.StartDefragmentation(5*time.Minute, 0.25)

	aofConfig := storage.DefaultAOFConfig()
	aofConfig.Path = "database.aof"
	aofConfig.SyncInterval = 2 * time.Second

	aofStorage, err := storage.NewAOF(aofConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer aofStorage.Close()

	server := server.NewServer(memCache, aofStorage, nil, serverConfig)
	server.SetMaster(true)
	go server.Start(":6379")
	time.Sleep(1 * time.Second)

	// Connection pool
	poolConfig := pool.Config{
		InitialSize:   10,
		MaxSize:       100,
		ReadTimeout:   5000 * time.Second,
		WriteTimeout:  5000 * time.Second,
		IdleTimeout:   6000 * time.Second,
		RetryAttempts: 3,
		RetryDelay:    100 * time.Millisecond,
	}

	factory := pool.NewConnFactory("localhost:6379", 5*time.Second)
	connectionPool, err := pool.NewConnectionPool(poolConfig, factory.CreateConnection)
	if err != nil {
		log.Fatal(err)
	}
	defer connectionPool.Close()

	server.SetConnectionPool(connectionPool)

	// Metrics server
	go func() {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			metrics := server.GetMetrics()
			json.NewEncoder(w).Encode(metrics)
		})
		log.Printf("Metrics server starting on :2112")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

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
