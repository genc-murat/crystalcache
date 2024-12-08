package handlers

import (
	"github.com/genc-murat/crystalcache/internal/client"
	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type CommandHandler func(args []models.Value) models.Value

type Registry struct {
	handlers        map[string]CommandHandler
	stringHandlers  *StringHandlers
	hashHandlers    *HashHandlers
	listHandlers    *ListHandlers
	setHandlers     *SetHandlers
	zsetHandlers    *ZSetHandlers
	adminHandlers   *AdminHandlers
	moduleHandlers  *ModuleHandlers
	configHandlers  *ConfigHandlers
	scanHandlers    *ScanHandlers
	memoryHandlers  *MemoryHandlers
	clusterHandlers *ClusterHandlers
	jsonHandlers    *JSONHandlers
	replicaHandlers *ReplicaHandlers
}

func NewRegistry(cache ports.Cache, clientManager *client.Manager) *Registry {
	r := &Registry{
		handlers:        make(map[string]CommandHandler),
		stringHandlers:  NewStringHandlers(cache),
		hashHandlers:    NewHashHandlers(cache),
		listHandlers:    NewListHandlers(cache),
		setHandlers:     NewSetHandlers(cache),
		zsetHandlers:    NewZSetHandlers(cache),
		adminHandlers:   NewAdminHandlers(cache, clientManager),
		moduleHandlers:  NewModuleHandlers(cache),
		configHandlers:  NewConfigHandlers(cache),
		scanHandlers:    NewScanHandlers(cache),
		memoryHandlers:  NewMemoryHandlers(cache),
		clusterHandlers: NewClusterHandlers(cache),
		jsonHandlers:    NewJSONHandlers(cache),
		replicaHandlers: NewReplicaHandlers(cache, nil),
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
	r.handlers["SETRANGE"] = r.stringHandlers.HandleSetRange
	r.handlers["ECHO"] = r.stringHandlers.HandleEcho
	r.handlers["MSET"] = r.stringHandlers.HandleMSet
	r.handlers["MGET"] = r.stringHandlers.HandleMGet
	r.handlers["LCS"] = r.stringHandlers.HandleLCS
	r.handlers["MSETNX"] = r.stringHandlers.HandleMSetNX
	r.handlers["GETEX"] = r.stringHandlers.HandleGetEx
	r.handlers["GETDEL"] = r.stringHandlers.HandleGetDel

	// Hash Commands
	r.handlers["HSET"] = r.hashHandlers.HandleHSet
	r.handlers["HGET"] = r.hashHandlers.HandleHGet
	r.handlers["HGETALL"] = r.hashHandlers.HandleHGetAll
	r.handlers["HLEN"] = r.hashHandlers.HandleHLen
	r.handlers["HSCAN"] = r.hashHandlers.HandleHScan
	r.handlers["HDEL"] = r.hashHandlers.HandleHDel
	r.handlers["HEXISTS"] = r.hashHandlers.HandleHExists
	r.handlers["HEXPIRE"] = r.hashHandlers.HandleHExpire
	r.handlers["HEXPIREAT"] = r.hashHandlers.HandleHExpireAt
	r.handlers["HINCRBY"] = r.hashHandlers.HandleHIncrBy
	r.handlers["HEXPIRETIME"] = r.hashHandlers.HandleHExpireTime
	r.handlers["HINCRBYFLOAT"] = r.hashHandlers.HandleHIncrByFloat

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
	r.handlers["ZDIFF"] = r.zsetHandlers.HandleZDiff
	r.handlers["ZDIFFSTORE"] = r.zsetHandlers.HandleZDiffStore
	r.handlers["ZINTER"] = r.zsetHandlers.HandleZInter
	r.handlers["ZINTERCARD"] = r.zsetHandlers.HandleZInterCard
	r.handlers["ZLEXCOUNT"] = r.zsetHandlers.HandleZLexCount
	r.handlers["ZMSCORE"] = r.zsetHandlers.HandleZMScore
	r.handlers["ZMPOP"] = r.zsetHandlers.HandleZMPop
	r.handlers["ZPOPMAX"] = r.zsetHandlers.HandleZPopMax
	r.handlers["ZPOPMIN"] = r.zsetHandlers.HandleZPopMin
	r.handlers["ZRANDMEMBER"] = r.zsetHandlers.HandleZRandMember
	r.handlers["ZRANGEBYLEX"] = r.zsetHandlers.HandleZRangeByLex
	r.handlers["ZRANGEBYSCORE"] = r.zsetHandlers.HandleZRangeByScore
	r.handlers["ZRANGESTORE"] = r.zsetHandlers.HandleZRangeStore
	r.handlers["ZRANK"] = r.zsetHandlers.HandleZRank
	r.handlers["ZREMRANGEBYLEX"] = r.zsetHandlers.HandleZRemRangeByLex
	r.handlers["ZREMRANGEBYRANK"] = r.zsetHandlers.HandleZRemRangeByRank
	r.handlers["ZREMRANGEBYSCORE"] = r.zsetHandlers.HandleZRemRangeByScore
	r.handlers["ZREVRANGE"] = r.zsetHandlers.HandleZRevRange
	r.handlers["ZREVRANGEBYLEX"] = r.zsetHandlers.HandleZRevRangeByLex
	r.handlers["ZREVRANGEBYSCORE"] = r.zsetHandlers.HandleZRevRangeByScore
	r.handlers["ZREVRANK"] = r.zsetHandlers.HandleZRevRank
	r.handlers["ZSCAN"] = r.zsetHandlers.HandleZScan
	r.handlers["ZSCORE"] = r.zsetHandlers.HandleZScore
	r.handlers["ZUNION"] = r.zsetHandlers.HandleZUnion
	r.handlers["ZUNIONSTORE"] = r.zsetHandlers.HandleZUnionStore

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
	r.handlers["PING"] = r.adminHandlers.HandlePing
	r.handlers["MEMORY"] = r.memoryHandlers.HandleMemory
	r.handlers["TYPE"] = r.memoryHandlers.HandleType
	r.handlers["TTL"] = r.memoryHandlers.HandleTTL

	r.handlers["CLUSTER"] = r.clusterHandlers.HandleCluster

	r.handlers["JSON.SET"] = r.jsonHandlers.HandleJSON
	r.handlers["JSON.GET"] = r.jsonHandlers.HandleJSONGet
	r.handlers["JSON.DEL"] = r.jsonHandlers.HandleJSONDel
	r.handlers["JSON.TYPE"] = r.jsonHandlers.HandleJSONType
	r.handlers["JSON.ARRAPPEND"] = r.jsonHandlers.HandleJSONArrAppend
	r.handlers["JSON.ARRLEN"] = r.jsonHandlers.HandleJSONArrLen
	r.handlers["JSON.STRLEN"] = r.jsonHandlers.HandleJSONStrLen
	r.handlers["JSON.TOGGLE"] = r.jsonHandlers.HandleJSONToggle
	r.handlers["JSON.ARRINDEX"] = r.jsonHandlers.HandleJSONArrIndex
	r.handlers["JSON.ARRTRIM"] = r.jsonHandlers.HandleJSONArrTrim
	r.handlers["JSON.NUMINCRBY"] = r.jsonHandlers.HandleJSONNumIncrBy
	r.handlers["JSON.OBJKEYS"] = r.jsonHandlers.HandleJSONObjKeys
	r.handlers["JSON.OBJLEN"] = r.jsonHandlers.HandleJSONObjLen
	r.handlers["JSON.ARRPOP"] = r.jsonHandlers.HandleJSONArrPop
	r.handlers["JSON.MERGE"] = r.jsonHandlers.HandleJSONMerge
	r.handlers["JSON.ARRINSERT"] = r.jsonHandlers.HandleJSONArrInsert
	r.handlers["JSON.NUMMULTBY"] = r.jsonHandlers.HandleJSONNumMultBy
	r.handlers["JSON.CLEAR"] = r.jsonHandlers.HandleJSONClear
	r.handlers["JSON.COMPARE"] = r.jsonHandlers.HandleJSONCompare
	r.handlers["JSON.STRAPPEND"] = r.jsonHandlers.HandleJSONStrAppend
	r.handlers["JSON.CONTAINS"] = r.jsonHandlers.HandleJSONContains
	r.handlers["JSON.ARRREVERSE"] = r.jsonHandlers.HandleJSONArrReverse
	r.handlers["JSON.ARRSORT"] = r.jsonHandlers.HandleJSONArrSort
	r.handlers["JSON.ARRUNIQUE"] = r.jsonHandlers.HandleJSONArrUnique
	r.handlers["JSON.COUNT"] = r.jsonHandlers.HandleJSONCount
	r.handlers["JSON.SWAP"] = r.jsonHandlers.HandleJSONSwap
	r.handlers["JSON.VALIDATE"] = r.jsonHandlers.HandleJSONValidate
	r.handlers["JSON.ARRSUM"] = r.jsonHandlers.HandleJSONArrSum
	r.handlers["JSON.ARRAVG"] = r.jsonHandlers.HandleJSONArrAvg
	r.handlers["JSON.SEARCH"] = r.jsonHandlers.HandleJSONSearch
	r.handlers["JSON.MINMAX"] = r.jsonHandlers.HandleJSONMinMax
	r.handlers["JSON.DEBUG"] = r.jsonHandlers.HandleJSONDebug
	r.handlers["JSON.FORGET"] = r.jsonHandlers.HandleJSONForget
	r.handlers["JSON.MGET"] = r.jsonHandlers.HandleJSONMGet
	r.handlers["JSON.MSET"] = r.jsonHandlers.HandleJSONMSet
	r.handlers["JSON.RESP"] = r.jsonHandlers.HandleJSONResp

	// In NewRegistry
	r.handlers["REPLICAOF"] = r.replicaHandlers.HandleReplicaOf
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
