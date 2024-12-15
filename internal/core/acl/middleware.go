package acl

import (
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

type Middleware struct {
	aclManager *ACLManager
}

func NewMiddleware(aclManager *ACLManager) *Middleware {
	return &Middleware{
		aclManager: aclManager,
	}
}

func (m *Middleware) CheckCommand(username string, cmd models.Value) bool {
	if len(cmd.Array) == 0 {
		return false
	}

	command := strings.ToUpper(cmd.Array[0].Bulk)

	// Determine required permission based on command
	var requiredPerm Permission
	switch {
	case isWriteCommand(command):
		requiredPerm = PermissionWrite
	case isAdminCommand(command):
		requiredPerm = PermissionAdmin
	default:
		requiredPerm = PermissionRead
	}

	// For commands that operate on keys, check key permissions
	if len(cmd.Array) > 1 {
		key := cmd.Array[1].Bulk
		return m.aclManager.CheckPermission(username, requiredPerm, key)
	}

	// For commands that don't operate on keys
	return m.aclManager.CheckPermission(username, requiredPerm, "*")
}

func isWriteCommand(cmd string) bool {
	writeCommands := map[string]bool{
		// String Commands
		"SET":         true,
		"MSET":        true,
		"MSETNX":      true,
		"APPEND":      true,
		"INCR":        true,
		"INCRBY":      true,
		"INCRBYFLOAT": true,
		"DECR":        true,
		"DECRBY":      true,
		"GETSET":      true,
		"SETRANGE":    true,

		// Key Commands
		"DEL":       true,
		"UNLINK":    true,
		"EXPIRE":    true,
		"EXPIREAT":  true,
		"PEXPIRE":   true,
		"PEXPIREAT": true,

		// List Commands
		"RPUSH":   true,
		"LPUSH":   true,
		"RPUSHX":  true,
		"LPUSHX":  true,
		"RPOP":    true,
		"LPOP":    true,
		"LSET":    true,
		"LTRIM":   true,
		"LINSERT": true,
		"LREM":    true,
		"BLPOP":   true,
		"BRPOP":   true,
		"LMOVE":   true,
		"BLMOVE":  true,

		// Set Commands
		"SADD":        true,
		"SREM":        true,
		"SPOP":        true,
		"SMOVE":       true,
		"SINTERSTORE": true,
		"SUNIONSTORE": true,
		"SDIFFSTORE":  true,

		// Sorted Set Commands
		"ZADD":             true,
		"ZREM":             true,
		"ZINCRBY":          true,
		"ZREMRANGEBYRANK":  true,
		"ZREMRANGEBYSCORE": true,
		"ZREMRANGEBYLEX":   true,
		"ZINTERSTORE":      true,
		"ZUNIONSTORE":      true,
		"ZDIFFSTORE":       true,
		"ZPOPMIN":          true,
		"ZPOPMAX":          true,
		"BZPOPMIN":         true,
		"BZPOPMAX":         true,
		"ZRANGESTORE":      true,

		// Hash Commands
		"HSET":         true,
		"HSETNX":       true,
		"HMSET":        true,
		"HDEL":         true,
		"HINCRBY":      true,
		"HINCRBYFLOAT": true,

		// Stream Commands
		"XADD":       true,
		"XDEL":       true,
		"XTRIM":      true,
		"XSETID":     true,
		"XGROUP":     true,
		"XACK":       true,
		"XCLAIM":     true,
		"XAUTOCLAIM": true,

		// Bitmap Commands
		"SETBIT":   true,
		"BITOP":    true,
		"BITFIELD": true,

		// JSON Commands
		"JSON.SET":       true,
		"JSON.DEL":       true,
		"JSON.ARRAPPEND": true,
		"JSON.ARRINSERT": true,
		"JSON.ARRTRIM":   true,
		"JSON.ARRPOP":    true,
		"JSON.STRAPPEND": true,
		"JSON.NUMINCRBY": true,
		"JSON.NUMMULTBY": true,
		"JSON.CLEAR":     true,
		"JSON.MERGE":     true,
		"JSON.MSET":      true,

		// Admin Commands
		"FLUSHALL": true,
		"FLUSHDB":  true,

		// Transaction Commands
		"MULTI": true,
		"EXEC":  true,
	}
	return writeCommands[cmd]
}

func isAdminCommand(cmd string) bool {
	adminCommands := map[string]bool{
		// Server Management
		"ACL":      true,
		"CONFIG":   true,
		"FLUSHALL": true,
		"FLUSHDB":  true,
		"SHUTDOWN": true,
		"DEBUG":    true,
		"MONITOR":  true,
		"SAVE":     true,
		"BGSAVE":   true,
		"LASTSAVE": true,

		// Replication Commands
		"REPLICAOF": true,
		"SLAVEOF":   true,
		"ROLE":      true,
		"SYNC":      true,
		"PSYNC":     true,
		"REPLCONF":  true,

		// Client Management
		"CLIENT": true, // CLIENT KILL, CLIENT LIST, CLIENT GETNAME, CLIENT SETNAME, etc.
		"KILL":   true,

		// Slow Log
		"SLOWLOG": true,

		// Memory Management
		"MEMORY": true, // MEMORY DOCTOR, MEMORY HELP, MEMORY MALLOC-STATS, etc.
		"SWAPDB": true,

		// Security
		"AUTH":  true,
		"HELLO": true,

		// Persistence Control
		"BGREWRITEAOF": true,
		"LOADING":      true,

		// Cluster Management
		"CLUSTER":   true,
		"READONLY":  true,
		"READWRITE": true,

		// Module Management
		"MODULE": true, // MODULE LOAD, MODULE UNLOAD, MODULE LIST

		// Scripting Admin Commands
		"SCRIPT":   true, // SCRIPT FLUSH, SCRIPT KILL, SCRIPT LOAD
		"FUNCTION": true, // FUNCTION LOAD, FUNCTION DELETE, etc.

		// Info & Statistics
		"INFO":    true,
		"TIME":    true,
		"COMMAND": true, // COMMAND INFO, COMMAND COUNT, COMMAND LIST
		"LATENCY": true,

		// Key Space & Database Management
		"SELECT":    true,
		"MOVE":      true,
		"SCAN":      true,
		"RANDOMKEY": true,

		// Persistence & Backup
		"DUMP":    true,
		"RESTORE": true,
		"MIGRATE": true,

		// Transaction Control
		"MULTI":   true,
		"EXEC":    true,
		"DISCARD": true,
		"WATCH":   true,
		"UNWATCH": true,

		// Pub/Sub Administration
		"PUBSUB": true, // PUBSUB CHANNELS, PUBSUB NUMSUB, PUBSUB NUMPAT

		// Stream Admin Commands
		"XGROUP": true,
		"XSETID": true,
		"XINFO":  true,

		// Sentinel Commands (if implementing Sentinel features)
		"SENTINEL": true,

		// Advanced Monitoring
		"OBJECT":              true,
		"MEMORY DOCTOR":       true,
		"MEMORY PURGE":        true,
		"MEMORY MALLOC-STATS": true,

		// Advanced Security
		"ACL WHOAMI":  true,
		"ACL LIST":    true,
		"ACL USERS":   true,
		"ACL SETUSER": true,
		"ACL DELUSER": true,
		"ACL GETUSER": true,
		"ACL CAT":     true,
		"ACL GENPASS": true,
		"ACL LOG":     true,
		"ACL HELP":    true,

		// Advanced Configuration
		"CONFIG GET":       true,
		"CONFIG SET":       true,
		"CONFIG REWRITE":   true,
		"CONFIG RESETSTAT": true,
	}
	return adminCommands[cmd]
}
