package handlers

import (
	"github.com/genc-murat/crystalcache/internal/client"
	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
)

type CommandHandler func(args []models.Value) models.Value

type Registry struct {
	handlers            map[string]CommandHandler
	stringHandlers      *StringHandlers
	hashHandlers        *HashHandlers
	listHandlers        *ListHandlers
	setHandlers         *SetHandlers
	zsetHandlers        *ZSetHandlers
	adminHandlers       *AdminHandlers
	moduleHandlers      *ModuleHandlers
	configHandlers      *ConfigHandlers
	scanHandlers        *ScanHandlers
	memoryHandlers      *MemoryHandlers
	clusterHandlers     *ClusterHandlers
	jsonHandlers        *JSONHandlers
	replicaHandlers     *ReplicaHandlers
	streamHandlers      *StreamHandlers
	bitMapHandlers      *BitMapHandlers
	geoHandlers         *GeoHandlers
	suggestionHandlers  *SuggestionHandlers
	cmsHandlers         *CMSHandlers
	cuckooHandlers      *CuckooHandlers
	hllHandlers         *HLLHandlers
	tdigestHandlers     *TDigestHandlers
	bloomFilterHandlers *BloomFilterHandlers
	topkHandlers        *TopKHandlers
	timeSeriesHandlers  *TimeSeriesHandlers
}

func NewRegistry(cache ports.Cache, clientManager *client.Manager) *Registry {
	r := &Registry{
		handlers:            make(map[string]CommandHandler),
		stringHandlers:      NewStringHandlers(cache),
		hashHandlers:        NewHashHandlers(cache),
		listHandlers:        NewListHandlers(cache),
		setHandlers:         NewSetHandlers(cache),
		zsetHandlers:        NewZSetHandlers(cache),
		adminHandlers:       NewAdminHandlers(cache, clientManager),
		moduleHandlers:      NewModuleHandlers(cache),
		configHandlers:      NewConfigHandlers(cache),
		scanHandlers:        NewScanHandlers(cache),
		memoryHandlers:      NewMemoryHandlers(cache),
		clusterHandlers:     NewClusterHandlers(cache),
		jsonHandlers:        NewJSONHandlers(cache),
		replicaHandlers:     NewReplicaHandlers(cache, nil),
		streamHandlers:      NewStreamHandlers(cache),
		bitMapHandlers:      NewBitMapHandlers(cache),
		geoHandlers:         NewGeoHandlers(cache),
		suggestionHandlers:  NewSuggestionHandlers(cache),
		cmsHandlers:         NewCMSHandlers(cache),
		cuckooHandlers:      NewCuckooHandlers(cache),
		hllHandlers:         NewHLLHandlers(cache),
		tdigestHandlers:     NewTDigestHandlers(cache),
		bloomFilterHandlers: NewBloomFilterHandlers(cache),
		topkHandlers:        NewTopKHandlers(cache),
		timeSeriesHandlers:  NewTimeSeriesHandlers(cache),
	}

	r.registerHandlers()
	return r
}

func (r *Registry) registerHandlers() {
	// String Commands
	r.handlers["SET"] = r.stringHandlers.HandleSet
	r.handlers["SETEX"] = r.stringHandlers.HandleSetEx
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
	r.handlers["APPEND"] = r.stringHandlers.HandleAppend
	r.handlers["DECR"] = r.stringHandlers.HandleDecr
	r.handlers["DECRBY"] = r.stringHandlers.HandleDecrBy
	r.handlers["INCRBY"] = r.stringHandlers.HandleIncrBy
	r.handlers["INCRBYFLOAT"] = r.stringHandlers.HandleIncrByFloat
	r.handlers["PTTL"] = r.stringHandlers.HandlePTTL
	r.handlers["EXPIREAT"] = r.stringHandlers.HandleExpireAt
	r.handlers["PEXPIREAT"] = r.stringHandlers.HandlePExpireAt

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
	r.handlers["HKEYS"] = r.hashHandlers.HandleHKeys
	r.handlers["HMGET"] = r.hashHandlers.HandleHMGet
	r.handlers["HMSET"] = r.hashHandlers.HandleHMSet
	r.handlers["HPERSIST"] = r.hashHandlers.HandleHPersist
	r.handlers["HSETNX"] = r.hashHandlers.HandleHSetNX
	r.handlers["HSTRLEN"] = r.hashHandlers.HandleHStrLen
	r.handlers["HTTL"] = r.hashHandlers.HandleHTTL
	r.handlers["HVALS"] = r.hashHandlers.HandleHVals
	r.handlers["HPTTL"] = r.hashHandlers.HandleHPTTL
	r.handlers["HRANDFIELD"] = r.hashHandlers.HandleHRandField
	r.handlers["HPEXPIRE"] = r.hashHandlers.HandleHPExpire
	r.handlers["HPEXPIREAT"] = r.hashHandlers.HandleHPExpireAt
	r.handlers["HPEXPIRETIME"] = r.hashHandlers.HandleHPExpireTime

	// List Commands
	r.handlers["LPUSH"] = r.listHandlers.HandleLPush
	r.handlers["RPUSH"] = r.listHandlers.HandleRPush
	r.handlers["LPUSHX"] = r.listHandlers.HandleLPushX
	r.handlers["RPUSHX"] = r.listHandlers.HandleRPushX
	r.handlers["LRANGE"] = r.listHandlers.HandleLRange
	r.handlers["LPOP"] = r.listHandlers.HandleLPop
	r.handlers["RPOP"] = r.listHandlers.HandleRPop
	r.handlers["LLEN"] = r.listHandlers.HandleLLen
	r.handlers["LSET"] = r.listHandlers.HandleLSet
	r.handlers["LREM"] = r.listHandlers.HandleLRem
	r.handlers["BLMOVE"] = r.listHandlers.HandleBLMOVE
	r.handlers["BRPOP"] = r.listHandlers.HandleBRPop
	r.handlers["BLPOP"] = r.listHandlers.HandleBLPop
	r.handlers["BLMPOP"] = r.listHandlers.HandleBLMPOP
	r.handlers["LINDEX"] = r.listHandlers.HandleLIndex
	r.handlers["LINSERT"] = r.listHandlers.HandleLInsert
	r.handlers["LMOVE"] = r.listHandlers.HandleLMove
	r.handlers["LMPOP"] = r.listHandlers.HandleLMPop

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
	r.handlers["SDIFFSTORE"] = r.setHandlers.HandleSDiffStore
	r.handlers["SINTERCARD"] = r.setHandlers.HandleSInterCard
	r.handlers["SINTERSTORE"] = r.setHandlers.HandleSInterStore
	r.handlers["SMISMEMBER"] = r.setHandlers.HandleSMIsMember
	r.handlers["SMOVE"] = r.setHandlers.HandleSMove
	r.handlers["SPOP"] = r.setHandlers.HandleSPop
	r.handlers["SRANDMEMBER"] = r.setHandlers.HandleSRandMember
	r.handlers["SUNIONSTORE"] = r.setHandlers.HandleSUnionStore

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

	// stream commands
	r.handlers["XADD"] = r.streamHandlers.HandleXAdd
	r.handlers["XACK"] = r.streamHandlers.HandleXACK
	r.handlers["XLEN"] = r.streamHandlers.HandleXLEN
	r.handlers["XPENDING"] = r.streamHandlers.HandleXPENDING
	r.handlers["XRANGE"] = r.streamHandlers.HandleXRANGE
	r.handlers["XREAD"] = r.streamHandlers.HandleXREAD
	r.handlers["XDEL"] = r.streamHandlers.HandleXDEL
	r.handlers["XAUTOCLAIM"] = r.streamHandlers.HandleXAutoClaim
	r.handlers["XCLAIM"] = r.streamHandlers.HandleXClaim
	r.handlers["XREVRANGE"] = r.streamHandlers.HandleXREVRANGE
	r.handlers["XSETID"] = r.streamHandlers.HandleXSETID
	r.handlers["XTRIM"] = r.streamHandlers.HandleXTRIM
	r.handlers["XINFO"] = r.streamHandlers.HandleXInfo
	r.handlers["XGROUP"] = r.streamHandlers.HandleXGroup

	//bitmap commands
	r.handlers["GETBIT"] = r.bitMapHandlers.HandleGetBit
	r.handlers["SETBIT"] = r.bitMapHandlers.HandleSetBit
	r.handlers["BITCOUNT"] = r.bitMapHandlers.HandleBitCount
	r.handlers["BITFIELD"] = r.bitMapHandlers.HandleBitField
	r.handlers["BITFIELD_RO"] = r.bitMapHandlers.HandleBitFieldRO
	r.handlers["BITOP"] = r.bitMapHandlers.HandleBitOp
	r.handlers["BITPOS"] = r.bitMapHandlers.HandleBitPos

	// Suggestion Commands
	r.handlers["FT.SUGADD"] = r.suggestionHandlers.HandleFTSugAdd
	r.handlers["FT.SUGDEL"] = r.suggestionHandlers.HandleFTSugDel
	r.handlers["FT.SUGGET"] = r.suggestionHandlers.HandleFTSugGet
	r.handlers["FT.SUGLEN"] = r.suggestionHandlers.HandleFTSugLen

	// Geospatial Commands
	r.handlers["GEOADD"] = r.geoHandlers.HandleGeoAdd
	r.handlers["GEODIST"] = r.geoHandlers.HandleGeoDist
	r.handlers["GEOPOS"] = r.geoHandlers.HandleGeoPos
	r.handlers["GEORADIUS"] = r.geoHandlers.HandleGeoRadius
	r.handlers["GEORADIUS_RO"] = r.geoHandlers.HandleGeoRadius      // Read-only variant uses same handler
	r.handlers["GEORADIUSBYMEMBER"] = r.geoHandlers.HandleGeoRadius // Uses same radius logic
	r.handlers["GEORADIUSBYMEMBER_RO"] = r.geoHandlers.HandleGeoRadius
	r.handlers["GEOSEARCH"] = r.geoHandlers.HandleGeoSearch
	r.handlers["GEOSEARCHSTORE"] = r.geoHandlers.HandleGeoSearchStore
	r.handlers["GEOHASH"] = r.geoHandlers.HandleGeoHash

	// Count-Min Sketch Commands
	r.handlers["CMS.INCRBY"] = r.cmsHandlers.HandleCMSIncrBy
	r.handlers["CMS.QUERY"] = r.cmsHandlers.HandleCMSQuery
	r.handlers["CMS.MERGE"] = r.cmsHandlers.HandleCMSMerge
	r.handlers["CMS.INFO"] = r.cmsHandlers.HandleCMSInfo
	r.handlers["CMS.INITBYDIM"] = r.cmsHandlers.HandleCMSInitByDim
	r.handlers["CMS.INITBYPROB"] = r.cmsHandlers.HandleCMSInitByProb

	// Cuckoo Filter Commands
	r.handlers["CF.RESERVE"] = r.cuckooHandlers.HandleCFReserve
	r.handlers["CF.ADD"] = r.cuckooHandlers.HandleCFAdd
	r.handlers["CF.ADDNX"] = r.cuckooHandlers.HandleCFAddNX
	r.handlers["CF.INSERT"] = r.cuckooHandlers.HandleCFInsert
	r.handlers["CF.INSERTNX"] = r.cuckooHandlers.HandleCFInsertNX
	r.handlers["CF.DEL"] = r.cuckooHandlers.HandleCFDel
	r.handlers["CF.COUNT"] = r.cuckooHandlers.HandleCFCount
	r.handlers["CF.EXISTS"] = r.cuckooHandlers.HandleCFExists
	r.handlers["CF.MEXISTS"] = r.cuckooHandlers.HandleCFMExists
	r.handlers["CF.INFO"] = r.cuckooHandlers.HandleCFInfo
	r.handlers["CF.SCANDUMP"] = r.cuckooHandlers.HandleCFScanDump
	r.handlers["CF.LOADCHUNK"] = r.cuckooHandlers.HandleCFLoadChunk

	// HyperLogLog Commands
	r.handlers["PFADD"] = r.hllHandlers.HandlePFAdd
	r.handlers["PFCOUNT"] = r.hllHandlers.HandlePFCount
	r.handlers["PFMERGE"] = r.hllHandlers.HandlePFMerge
	r.handlers["PFDEBUG"] = r.hllHandlers.HandlePFDebug
	r.handlers["PFSELFTEST"] = r.hllHandlers.HandlePFSelfTest

	// T-Digest Commands
	r.handlers["TDIGEST.CREATE"] = r.tdigestHandlers.HandleTDigestCreate
	r.handlers["TDIGEST.ADD"] = r.tdigestHandlers.HandleTDigestAdd
	r.handlers["TDIGEST.MERGE"] = r.tdigestHandlers.HandleTDigestMerge
	r.handlers["TDIGEST.RESET"] = r.tdigestHandlers.HandleTDigestReset
	r.handlers["TDIGEST.QUANTILE"] = r.tdigestHandlers.HandleTDigestQuantile
	r.handlers["TDIGEST.MIN"] = r.tdigestHandlers.HandleTDigestMin
	r.handlers["TDIGEST.MAX"] = r.tdigestHandlers.HandleTDigestMax
	r.handlers["TDIGEST.INFO"] = r.tdigestHandlers.HandleTDigestInfo
	r.handlers["TDIGEST.CDF"] = r.tdigestHandlers.HandleTDigestCDF
	r.handlers["TDIGEST.TRIMMED_MEAN"] = r.tdigestHandlers.HandleTDigestTrimmedMean

	// Bloom Filter Commands
	r.handlers["BF.ADD"] = r.bloomFilterHandlers.HandleBFAdd
	r.handlers["BF.EXISTS"] = r.bloomFilterHandlers.HandleBFExists
	r.handlers["BF.RESERVE"] = r.bloomFilterHandlers.HandleBFReserve
	r.handlers["BF.MADD"] = r.bloomFilterHandlers.HandleBFMAdd
	r.handlers["BF.MEXISTS"] = r.bloomFilterHandlers.HandleBFMExists
	r.handlers["BF.INFO"] = r.bloomFilterHandlers.HandleBFInfo
	r.handlers["BF.CARD"] = r.bloomFilterHandlers.HandleBFCard
	r.handlers["BF.SCANDUMP"] = r.bloomFilterHandlers.HandleBFScanDump
	r.handlers["BF.LOADCHUNK"] = r.bloomFilterHandlers.HandleBFLoadChunk
	r.handlers["BF.INSERT"] = r.bloomFilterHandlers.HandleBFInsert

	// TopK Commands
	r.handlers["TOPK.RESERVE"] = r.topkHandlers.HandleTOPKReserve
	r.handlers["TOPK.ADD"] = r.topkHandlers.HandleTOPKAdd
	r.handlers["TOPK.INCRBY"] = r.topkHandlers.HandleTOPKIncrBy
	r.handlers["TOPK.QUERY"] = r.topkHandlers.HandleTOPKQuery
	r.handlers["TOPK.COUNT"] = r.topkHandlers.HandleTOPKCount
	r.handlers["TOPK.LIST"] = r.topkHandlers.HandleTOPKList
	r.handlers["TOPK.INFO"] = r.topkHandlers.HandleTOPKInfo

	r.handlers["TS.CREATE"] = r.timeSeriesHandlers.HandleTSCreate
	r.handlers["TS.ADD"] = r.timeSeriesHandlers.HandleTSAdd
	r.handlers["TS.MADD"] = r.timeSeriesHandlers.HandleTSMAdd
	r.handlers["TS.RANGE"] = r.timeSeriesHandlers.HandleTSRange
	r.handlers["TS.INFO"] = r.timeSeriesHandlers.HandleTSInfo
	r.handlers["TS.INCRBY"] = r.timeSeriesHandlers.HandleTSIncrBy
	r.handlers["TS.DECRBY"] = r.timeSeriesHandlers.HandleTSDecrBy
	r.handlers["TS.DEL"] = r.timeSeriesHandlers.HandleTSDel
	r.handlers["TS.ALTER"] = r.timeSeriesHandlers.HandleTSAlter
	r.handlers["TS.CREATERULE"] = r.timeSeriesHandlers.HandleTSCreateRule
	r.handlers["TS.DELETERULE"] = r.timeSeriesHandlers.HandleTSDeleteRule
	r.handlers["TS.GET"] = r.timeSeriesHandlers.HandleTSGet
	r.handlers["TS.MGET"] = r.timeSeriesHandlers.HandleTSMGet
	r.handlers["TS.MRANGE"] = r.timeSeriesHandlers.HandleTSMRange
	r.handlers["TS.MREVRANGE"] = r.timeSeriesHandlers.HandleTSMRevRange
	r.handlers["TS.QUERYINDEX"] = r.timeSeriesHandlers.HandleTSQueryIndex
	r.handlers["TS.REVRANGE"] = r.timeSeriesHandlers.HandleTSRevRange

	// new commands
	r.handlers["DELTYPE"] = r.stringHandlers.HandleDelType
	r.handlers["KEYCOUNT"] = r.adminHandlers.HandleKeyCount
	r.handlers["MEMORYUSAGE"] = r.adminHandlers.HandleMemoryUsage
	r.handlers["LPUSHXGET"] = r.listHandlers.HandleLPushXGet
	r.handlers["RPUSHXGET"] = r.listHandlers.HandleRPushXGet
	r.handlers["SMEMRANDOMCOUNT"] = r.setHandlers.HandleSMemRandomCount
	r.handlers["ZREMRANGEBYRANKCOUNT"] = r.zsetHandlers.HandleZRemRangeByRankCount
	r.handlers["ZPOPMINMAXBY"] = r.zsetHandlers.HandleZPopMinMaxBy
	r.handlers["HDELIF"] = r.hashHandlers.HandleHDelIf
	r.handlers["HINCRBYFLOATIF"] = r.hashHandlers.HandleHIncrByFloatIf
	r.handlers["SDIFFSTOREDEL"] = r.setHandlers.HandleSDiffStoreDel
	r.handlers["MGETTYPE"] = r.stringHandlers.HandleMGetType
	r.handlers["SMEMBERSPATTERN"] = r.setHandlers.HandleSMembersPattern
	r.handlers["HSCANMATCH"] = r.hashHandlers.HandleHScanMatch
	r.handlers["ZSCANBYSCORE"] = r.zsetHandlers.HandleZScanByScore
	r.handlers["SEEXPIRE"] = r.stringHandlers.HandleSEExpire
	r.handlers["HINCRBYMULTI"] = r.hashHandlers.HandleHIncrByMulti
	r.handlers["LROTATE"] = r.listHandlers.HandleLRotate
	r.handlers["SPOPCOUNT"] = r.setHandlers.HandleSPopCount
	r.handlers["SDIFFMULTI"] = r.setHandlers.HandleSDiffMulti
	r.handlers["SINTERMULTI"] = r.setHandlers.HandleSInterMulti
	r.handlers["SUNIONMULTI"] = r.setHandlers.HandleSUnionMulti
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
