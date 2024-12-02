package main

import (
	"log"
	"time"

	"github.com/genc-murat/crystalcache/internal/app"
	"github.com/genc-murat/crystalcache/internal/cache"
	"github.com/genc-murat/crystalcache/internal/pool"
	"github.com/genc-murat/crystalcache/internal/storage"
)

func main() {
	// Pool konfigürasyonu
	poolConfig := pool.Config{
		InitialSize:   10,
		MaxSize:       100,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		IdleTimeout:   60 * time.Second,
		RetryAttempts: 3,
		RetryDelay:    100 * time.Millisecond,
	}

	// Connection factory oluştur
	factory := pool.NewConnFactory("localhost:6379", 5*time.Second)

	// Connection pool oluştur
	connectionPool, err := pool.NewConnectionPool(poolConfig, factory.CreateConnection)
	if err != nil {
		log.Fatal(err)
	}
	defer connectionPool.Close()

	// Cache ve storage oluştur
	memCache := cache.NewMemoryCache()
	memCache.StartDefragmentation(5*time.Minute, 0.25)

	aofStorage, err := storage.NewAOF("database.aof")
	if err != nil {
		log.Fatal(err)
	}
	defer aofStorage.Close()

	// Server'ı başlat
	server := app.NewServer(memCache, aofStorage, connectionPool)
	log.Println("Starting server on :6379")
	if err := server.Start(":6379"); err != nil {
		log.Fatal(err)
	}
}
