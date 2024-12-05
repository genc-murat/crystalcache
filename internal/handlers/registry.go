package handlers

import (
	"github.com/genc-murat/crystalcache/internal/client"
	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type CommandHandler func(args []models.Value) models.Value

type Registry struct {
	handlers       map[string]CommandHandler
	stringHandlers *StringHandlers
	hashHandlers   *HashHandlers
	listHandlers   *ListHandlers
	setHandlers    *SetHandlers
	zsetHandlers   *ZSetHandlers
	adminHandlers  *AdminHandlers
	moduleHandlers *ModuleHandlers
	configHandlers *ConfigHandlers
	scanHandlers   *ScanHandlers
	memoryHandlers *MemoryHandlers
}

func NewRegistry(cache ports.Cache, clientManager *client.Manager) *Registry {
	r := &Registry{
		handlers:       make(map[string]CommandHandler),
		stringHandlers: NewStringHandlers(cache),
		hashHandlers:   NewHashHandlers(cache),
		listHandlers:   NewListHandlers(cache),
		setHandlers:    NewSetHandlers(cache),
		zsetHandlers:   NewZSetHandlers(cache),
		adminHandlers:  NewAdminHandlers(cache, clientManager),
		moduleHandlers: NewModuleHandlers(cache),
		configHandlers: NewConfigHandlers(cache),
		scanHandlers:   NewScanHandlers(cache),
		memoryHandlers: NewMemoryHandlers(cache),
	}

	r.registerHandlers()
	return r
}

func (r *Registry) registerHandlers() {
	// String Commands
	r.handlers["SET"] = r.stringHandlers.HandleSet
	r.handlers["GET"] = r.stringHandlers.HandleGet
	r.handlers["INCR"] = r.stringHandlers.HandleIncr
	r.handlers["DEL"] = r.stringHandlers.HandleDel
	r.handlers["EXISTS"] = r.stringHandlers.HandleExists
	r.handlers["EXPIRE"] = r.stringHandlers.HandleExpire
	r.handlers["STRLEN"] = r.stringHandlers.HandleStrlen
	r.handlers["GETRANGE"] = r.stringHandlers.HandleGetRange

	// Hash Commands
	r.handlers["HSET"] = r.hashHandlers.HandleHSet
	r.handlers["HGET"] = r.hashHandlers.HandleHGet
	r.handlers["HGETALL"] = r.hashHandlers.HandleHGetAll
	r.handlers["HLEN"] = r.hashHandlers.HandleHLen
	r.handlers["HSCAN"] = r.hashHandlers.HandleHScan

	// List Commands
	r.handlers["LPUSH"] = r.listHandlers.HandleLPush
	r.handlers["RPUSH"] = r.listHandlers.HandleRPush
	r.handlers["LRANGE"] = r.listHandlers.HandleLRange
	r.handlers["LPOP"] = r.listHandlers.HandleLPop
	r.handlers["RPOP"] = r.listHandlers.HandleRPop
	r.handlers["LLEN"] = r.listHandlers.HandleLLen
	r.handlers["LSET"] = r.listHandlers.HandleLSet
	r.handlers["LREM"] = r.listHandlers.HandleLRem

	// Set Commands
	r.handlers["SADD"] = r.setHandlers.HandleSAdd
	r.handlers["SMEMBERS"] = r.setHandlers.HandleSMembers
	r.handlers["SCARD"] = r.setHandlers.HandleSCard
	r.handlers["SREM"] = r.setHandlers.HandleSRem
	r.handlers["SISMEMBER"] = r.setHandlers.HandleSIsMember
	r.handlers["SINTER"] = r.setHandlers.HandleSInter
	r.handlers["SUNION"] = r.setHandlers.HandleSUnion
	r.handlers["SDIFF"] = r.setHandlers.HandleSDiff
	r.handlers["SSCAN"] = r.setHandlers.HandleSScan

	// ZSet Commands
	r.handlers["ZADD"] = r.zsetHandlers.HandleZAdd
	r.handlers["ZCARD"] = r.zsetHandlers.HandleZCard
	r.handlers["ZCOUNT"] = r.zsetHandlers.HandleZCount
	r.handlers["ZRANGE"] = r.zsetHandlers.HandleZRange
	r.handlers["ZINCRBY"] = r.zsetHandlers.HandleZIncrBy
	r.handlers["ZREM"] = r.zsetHandlers.HandleZRem
	r.handlers["ZINTERSTORE"] = r.zsetHandlers.HandleZInterStore

	// Admin Commands
	r.handlers["FLUSHALL"] = r.adminHandlers.HandleFlushAll
	r.handlers["INFO"] = r.adminHandlers.HandleInfo
	r.handlers["DBSIZE"] = r.adminHandlers.HandleDBSize
	r.handlers["MULTI"] = r.adminHandlers.HandleMulti
	r.handlers["EXEC"] = r.adminHandlers.HandleExec
	r.handlers["DISCARD"] = r.adminHandlers.HandleDiscard
	r.handlers["WATCH"] = r.adminHandlers.HandleWatch
	r.handlers["UNWATCH"] = r.adminHandlers.HandleUnwatch
	r.handlers["CLIENT"] = r.adminHandlers.HandleClient
	r.handlers["MODULE"] = r.moduleHandlers.HandleModule
	r.handlers["CONFIG"] = r.configHandlers.HandleConfig
	r.handlers["SCAN"] = r.scanHandlers.HandleScan

	r.handlers["MEMORY"] = r.memoryHandlers.HandleMemory
	r.handlers["TYPE"] = r.memoryHandlers.HandleType
	r.handlers["TTL"] = r.memoryHandlers.HandleTTL
}

func (r *Registry) GetHandler(cmd string) (CommandHandler, bool) {
	handler, exists := r.handlers[cmd]
	if exists && cmd == "CLIENT" {
		// Wrap client commands to preserve connection context
		return func(args []models.Value) models.Value {
			return r.adminHandlers.HandleClient(args)
		}, true
	}
	return handler, exists
}
