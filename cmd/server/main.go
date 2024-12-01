package main

import (
	"log"

	"github.com/genc-murat/crystalcache/internal/app"
	"github.com/genc-murat/crystalcache/internal/cache"
	"github.com/genc-murat/crystalcache/internal/storage"
)

func main() {
	memCache := cache.NewMemoryCache()
	aofStorage, err := storage.NewAOF("database.aof")
	if err != nil {
		log.Fatal(err)
	}
	defer aofStorage.Close()

	server := app.NewServer(memCache, aofStorage)
	log.Println("Starting server on :6379")
	if err := server.Start(":6379"); err != nil {
		log.Fatal(err)
	}
}
