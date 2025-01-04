package acl

import (
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
)

// Middleware handles ACL checks for commands
type Middleware struct {
	aclManager *ACLManager
}

// NewMiddleware creates a new instance of Middleware with the provided ACLManager.
// It initializes the Middleware with the given ACLManager to handle access control logic.
//
// Parameters:
//   - aclManager: A pointer to an ACLManager instance that manages access control lists.
//
// Returns:
//   - A pointer to a newly created Middleware instance.
func NewMiddleware(aclManager *ACLManager) *Middleware {
	return &Middleware{
		aclManager: aclManager,
	}
}

// CheckCommand verifies if a given user has permission to execute a specific command.
// It handles special cases for the default user with no authentication and always allows the AUTH command.
//
// Parameters:
// - username: The name of the user attempting to execute the command.
// - cmd: The command to be checked, represented as a models.Value.
//
// Returns:
// - bool: True if the user has permission to execute the command, false otherwise.
func (m *Middleware) CheckCommand(username string, cmd models.Value) bool {
	if len(cmd.Array) == 0 {
		return false
	}

	// Special handling for default user with no auth
	if username == "" || username == DefaultUsername {
		if user, exists := m.aclManager.users[DefaultUsername]; exists && user.NoPass {
			return true
		}
	}

	command := strings.ToUpper(cmd.Array[0].Bulk)

	// Always allow AUTH command
	if command == "AUTH" {
		return true
	}

	// For commands that operate on keys, check key permissions
	if len(cmd.Array) > 1 {
		key := cmd.Array[1].Bulk
		return m.aclManager.CheckCommandPerm(username, command) &&
			m.aclManager.CheckKeyPerm(username, key, isWriteCommand(command))
	}

	// For commands that don't operate on keys
	return m.aclManager.CheckCommandPerm(username, command)
}

// readOnlyCommands is a map that defines a set of Redis commands that are considered read-only.
// Each command is mapped to a boolean value of true, indicating that the command does not modify
// the state of the Redis database. This map includes commands for various data types such as
// strings, hashes, lists, sets, sorted sets, keys, and server operations.
var readOnlyCommands = map[string]bool{
	// String Commands
	"GET":      true,
	"STRLEN":   true,
	"GETRANGE": true,
	"MGET":     true,

	// Hash Commands
	"HGET":    true,
	"HMGET":   true,
	"HLEN":    true,
	"HKEYS":   true,
	"HVALS":   true,
	"HGETALL": true,
	"HEXISTS": true,
	"HSCAN":   true,

	// List Commands
	"LLEN":   true,
	"LINDEX": true,
	"LRANGE": true,

	// Set Commands
	"SCARD":     true,
	"SISMEMBER": true,
	"SMEMBERS":  true,
	"SSCAN":     true,
	"SINTER":    true,
	"SUNION":    true,
	"SDIFF":     true,

	// Sorted Set Commands
	"ZCARD":         true,
	"ZCOUNT":        true,
	"ZLEXCOUNT":     true,
	"ZSCORE":        true,
	"ZRANGE":        true,
	"ZRANGEBYLEX":   true,
	"ZRANGEBYSCORE": true,
	"ZRANK":         true,
	"ZREVRANK":      true,
	"ZSCAN":         true,

	// Key Commands
	"EXISTS":    true,
	"TYPE":      true,
	"TTL":       true,
	"PTTL":      true,
	"OBJECT":    true,
	"MEMORY":    true,
	"RANDOMKEY": true,
	"SCAN":      true,

	// Server Commands
	"PING":    true,
	"TIME":    true,
	"INFO":    true,
	"COMMAND": true,
}

// isWriteCommand checks if a given Redis command is a write command.
// It returns true if the command modifies the state of the Redis database,
// and false otherwise.
//
// Parameters:
// - cmd: A string representing the Redis command to check.
//
// Returns:
// - bool: true if the command is a write command, false otherwise.
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

		// Admin Commands
		"FLUSHALL": true,
		"FLUSHDB":  true,

		// Transaction Commands
		"MULTI": true,
		"EXEC":  true,
	}
	return writeCommands[cmd]
}

// isAdminCommand checks if a given command is an administrative command.
// It returns true if the command is an admin command, otherwise false.
//
// The following categories of commands are considered admin commands:
// - Server Management: ACL, CONFIG, FLUSHALL, FLUSHDB, SHUTDOWN, DEBUG, MONITOR, SAVE, BGSAVE, LASTSAVE
// - Replication Commands: REPLICAOF, SLAVEOF, ROLE, SYNC, PSYNC, REPLCONF
// - Client Management: CLIENT, KILL
// - Other Admin Commands: SLOWLOG, MEMORY, SWAPDB, MODULE, SCRIPT, FUNCTION, CLUSTER, SENTINEL, COMMAND
//
// Parameters:
// - cmd: The command to check.
//
// Returns:
// - bool: true if the command is an admin command, false otherwise.
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
		"CLIENT": true,
		"KILL":   true,

		// Other Admin Commands
		"SLOWLOG":  true,
		"MEMORY":   true,
		"SWAPDB":   true,
		"MODULE":   true,
		"SCRIPT":   true,
		"FUNCTION": true,
		"CLUSTER":  true,
		"SENTINEL": true,
		"COMMAND":  true,
	}
	return adminCommands[cmd]
}

// getCommandCategory categorizes a given command string into one of the predefined
// categories: "@admin", "@write", "@read", or "@all". The function checks the command
// against specific conditions to determine its category:
// - If the command is an admin command, it returns "@admin".
// - If the command is a write command, it returns "@write".
// - If the command is a read-only command, it returns "@read".
// - If none of the above conditions are met, it returns "@all".
//
// Parameters:
// - cmd: The command string to be categorized.
//
// Returns:
// - A string representing the category of the command.
func getCommandCategory(cmd string) string {
	switch {
	case isAdminCommand(cmd):
		return "@admin"
	case isWriteCommand(cmd):
		return "@write"
	case readOnlyCommands[cmd]:
		return "@read"
	default:
		return "@all"
	}
}
