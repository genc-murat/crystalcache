package acl

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ACLSelector represents a key pattern selector
type ACLSelector struct {
	Pattern string
	AllowR  bool // Read permission
	AllowW  bool // Write permission
}

// User represents an ACL user with Redis-style permissions
type User struct {
	Username   string
	HashedPass []string // Multiple passwords supported
	Enabled    bool
	Commands   map[string]bool // Commands explicitly allowed
	Keys       []ACLSelector   // Key patterns with permissions
	Categories []string        // Command categories (+@all, -@admin, etc.)
	NoPass     bool            // True if user requires no password
	ResetTime  time.Time       // Time when keys/commands were reset
	Created    time.Time
	LastAuth   time.Time
}

// Command categories
const (
	CatAdmin      = "@admin"
	CatDangerous  = "@dangerous"
	CatWrite      = "@write"
	CatRead       = "@read"
	CatPubsub     = "@pubsub"
	CatFast       = "@fast"
	CatSlow       = "@slow"
	CatBlocking   = "@blocking"
	CatConnection = "@connection"
)

// ACLRule represents a Redis-style ACL rule
type ACLRule struct {
	Allow    bool
	Category string
	Command  string
}

// DefaultUser constants
const (
	DefaultUsername = "default"
	DefaultPass     = "" // Empty password for default user
)

// ACLManager manages ACL users and permissions
type ACLManager struct {
	users      map[string]*User
	categories map[string][]string // Maps categories to commands
	mu         sync.RWMutex
}

// NewACLManager creates a new ACL manager instance
func NewACLManager() *ACLManager {
	am := &ACLManager{
		users: make(map[string]*User),
	}

	// Create default user with all permissions
	defaultUser := &User{
		Username:   DefaultUsername,
		HashedPass: []string{hashPassword(DefaultPass)}, // Hash of empty password
		Enabled:    true,
		Commands:   make(map[string]bool),
		Keys:       []ACLSelector{{Pattern: "*", AllowR: true, AllowW: true}},
		Categories: []string{"@all"},
		NoPass:     true, // Default user doesn't require password
		Created:    time.Now(),
	}

	// Enable all commands for default user
	for cmd := range getAllCommands() {
		defaultUser.Commands[cmd] = true
	}

	am.users[DefaultUsername] = defaultUser

	return am
}

func (am *ACLManager) initializeCategories() {
	am.categories = map[string][]string{
		"@admin": {
			"ACL", "FLUSHALL", "FLUSHDB", "SHUTDOWN", "CONFIG", "MONITOR", "DEBUG", "SAVE",
			"BGSAVE", "REPLICAOF", "SLAVEOF", "SYNC",
		},
		"@dangerous": {
			"FLUSHALL", "FLUSHDB", "KEYS", "SHUTDOWN", "DEBUG",
		},
		"@write": {
			"SET", "DEL", "EXPIRE", "LPUSH", "RPUSH", "SADD", "ZADD", "HSET",
		},
		"@read": {
			"GET", "EXISTS", "TTL", "LRANGE", "SMEMBERS", "ZRANGE", "HGET",
		},
		"@fast": {
			"GET", "SET", "INCR", "LPUSH", "RPUSH", "ZPOPMIN", "SADD",
		},
		"@slow": {
			"SORT", "LREM", "ZRANGEBYSCORE", "ZRANK", "ZUNIONSTORE",
		},
	}
}

func (am *ACLManager) createDefaultUser() {
	defaultUser := &User{
		Username: "default",
		Commands: make(map[string]bool),
		Enabled:  true,
		Created:  time.Now(),
		NoPass:   false,
	}

	// Add default permissions
	for cmd := range am.categories["@admin"] {
		defaultUser.Commands[am.categories["@admin"][cmd]] = true
	}

	am.users["default"] = defaultUser
}

// SetUser sets up a user according to Redis ACL rules
func (am *ACLManager) SetUser(rules string) error {
	parts := strings.Fields(rules)
	if len(parts) < 2 {
		return errors.New("invalid ACL rule format")
	}

	username := parts[1]
	user := &User{
		Username: username,
		Commands: make(map[string]bool),
		Enabled:  true,
		Created:  time.Now(),
	}

	// Check if user exists
	am.mu.Lock()
	if existingUser, exists := am.users[username]; exists {
		user = existingUser
	}
	am.mu.Unlock()

	for i := 2; i < len(parts); i++ {
		rule := parts[i]
		switch {
		case rule == "on":
			user.Enabled = true
		case rule == "off":
			user.Enabled = false
		case strings.HasPrefix(rule, ">"):
			pass := rule[1:]
			hash := hashPassword(pass)
			if !containsString(user.HashedPass, hash) {
				user.HashedPass = append(user.HashedPass, hash)
			}
		case rule == "nopass":
			user.NoPass = true
			user.HashedPass = nil
		case rule == "resetpass":
			user.HashedPass = nil
			user.NoPass = false
		case strings.HasPrefix(rule, "~"):
			pattern := rule[1:]
			user.Keys = append(user.Keys, ACLSelector{
				Pattern: pattern,
				AllowR:  true,
				AllowW:  true,
			})
		case strings.HasPrefix(rule, "+"):
			if strings.HasPrefix(rule, "+@") {
				category := rule[1:]
				user.Categories = append(user.Categories, category)
				am.addCategoryCommands(user, category, true)
			} else {
				cmd := strings.ToUpper(rule[1:])
				user.Commands[cmd] = true
			}
		case strings.HasPrefix(rule, "-"):
			if strings.HasPrefix(rule, "-@") {
				category := rule[1:]
				user.Categories = append(user.Categories, "-"+category)
				am.addCategoryCommands(user, category, false)
			} else {
				cmd := strings.ToUpper(rule[1:])
				user.Commands[cmd] = false
			}
		}
	}

	am.mu.Lock()
	am.users[username] = user
	am.mu.Unlock()

	return nil
}

func (am *ACLManager) addCategoryCommands(user *User, category string, allow bool) {
	if cmds, ok := am.categories[category]; ok {
		for _, cmd := range cmds {
			user.Commands[cmd] = allow
		}
	}
}

// Authenticate checks user credentials with support for empty password
func (am *ACLManager) Authenticate(username, password string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	// Special handling for default user
	if username == DefaultUsername && password == DefaultPass {
		if user, exists := am.users[DefaultUsername]; exists && user.NoPass {
			return true
		}
	}

	user, exists := am.users[username]
	if !exists || !user.Enabled {
		return false
	}

	// Allow if nopass is set
	if user.NoPass {
		return true
	}

	// Check against all stored password hashes
	hashedInput := hashPassword(password)
	for _, hash := range user.HashedPass {
		if hash == hashedInput {
			return true
		}
	}

	return false
}

// UpdateLastAuth updates the last authentication time for a user
func (am *ACLManager) UpdateLastAuth(username string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	if user, exists := am.users[username]; exists {
		user.LastAuth = time.Now()
	}
}

func (am *ACLManager) CheckCommandPerm(username, command string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	user, exists := am.users[username]
	if !exists || !user.Enabled {
		return false
	}

	if allowed, exists := user.Commands[command]; exists {
		return allowed
	}

	for _, cat := range user.Categories {
		if cmds, exists := am.categories[cat]; exists {
			for _, cmd := range cmds {
				if cmd == command {
					return !strings.HasPrefix(cat, "-")
				}
			}
		}
	}

	return false
}

func (am *ACLManager) CheckKeyPerm(username, key string, write bool) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()

	user, exists := am.users[username]
	if !exists || !user.Enabled {
		return false
	}

	for _, selector := range user.Keys {
		if matchKeyPattern(selector.Pattern, key) {
			return write && selector.AllowW || !write && selector.AllowR
		}
	}

	return false
}

func matchKeyPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	return strings.HasPrefix(key, strings.TrimSuffix(pattern, "*"))
}

func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

// GetACLList returns a list of all ACL rules
func (am *ACLManager) GetACLList() []string {
	am.mu.RLock()
	defer am.mu.RUnlock()

	var aclList []string
	for username, user := range am.users {
		aclList = append(aclList, fmt.Sprintf("user %s %s", username, am.formatUserACL(user)))
	}
	return aclList
}

func (am *ACLManager) formatUserACL(user *User) string {
	var parts []string

	if user.Enabled {
		parts = append(parts, "on")
	} else {
		parts = append(parts, "off")
	}

	if user.NoPass {
		parts = append(parts, "nopass")
	}
	for _, hash := range user.HashedPass {
		parts = append(parts, ">"+hash)
	}

	for _, key := range user.Keys {
		parts = append(parts, "~"+key.Pattern)
	}

	for _, cat := range user.Categories {
		if strings.HasPrefix(cat, "-") {
			parts = append(parts, "-"+cat)
		} else {
			parts = append(parts, "+"+cat)
		}
	}

	return strings.Join(parts, " ")
}

func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func getAllCommands() map[string]bool {
	commands := map[string]bool{
		// String Commands
		"SET":         true,
		"GET":         true,
		"MSET":        true,
		"MGET":        true,
		"MSETNX":      true,
		"APPEND":      true,
		"STRLEN":      true,
		"INCR":        true,
		"INCRBY":      true,
		"INCRBYFLOAT": true,
		"DECR":        true,
		"DECRBY":      true,
		"GETSET":      true,
		"SETRANGE":    true,
		"GETRANGE":    true,
		"SETEX":       true,
		"PSETEX":      true,
		"SETNX":       true,

		// Key Commands
		"DEL":       true,
		"UNLINK":    true,
		"EXPIRE":    true,
		"EXPIREAT":  true,
		"PEXPIRE":   true,
		"PEXPIREAT": true,
		"TTL":       true,
		"PTTL":      true,
		"PERSIST":   true,
		"EXISTS":    true,
		"TYPE":      true,
		"RENAME":    true,
		"RENAMENX":  true,
		"KEYS":      true,
		"SCAN":      true,

		// List Commands
		"RPUSH":     true,
		"LPUSH":     true,
		"RPUSHX":    true,
		"LPUSHX":    true,
		"RPOP":      true,
		"LPOP":      true,
		"LLEN":      true,
		"LINDEX":    true,
		"LSET":      true,
		"LRANGE":    true,
		"LTRIM":     true,
		"LINSERT":   true,
		"LREM":      true,
		"BLPOP":     true,
		"BRPOP":     true,
		"LMOVE":     true,
		"BLMOVE":    true,
		"RPOPLPUSH": true,

		// Set Commands
		"SADD":        true,
		"SREM":        true,
		"SPOP":        true,
		"SMEMBERS":    true,
		"SISMEMBER":   true,
		"SCARD":       true,
		"SMOVE":       true,
		"SINTER":      true,
		"SINTERSTORE": true,
		"SUNION":      true,
		"SUNIONSTORE": true,
		"SDIFF":       true,
		"SDIFFSTORE":  true,
		"SSCAN":       true,

		// Sorted Set Commands
		"ZADD":             true,
		"ZREM":             true,
		"ZINCRBY":          true,
		"ZCARD":            true,
		"ZCOUNT":           true,
		"ZRANGE":           true,
		"ZREVRANGE":        true,
		"ZRANGEBYSCORE":    true,
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
		"ZRANK":            true,
		"ZREVRANK":         true,
		"ZSCORE":           true,
		"ZSCAN":            true,

		// Hash Commands
		"HSET":         true,
		"HSETNX":       true,
		"HGET":         true,
		"HMSET":        true,
		"HMGET":        true,
		"HDEL":         true,
		"HEXISTS":      true,
		"HLEN":         true,
		"HKEYS":        true,
		"HVALS":        true,
		"HGETALL":      true,
		"HINCRBY":      true,
		"HINCRBYFLOAT": true,
		"HSCAN":        true,

		// Stream Commands
		"XADD":         true,
		"XDEL":         true,
		"XREAD":        true,
		"XREADGROUP":   true,
		"XRANGE":       true,
		"XREVRANGE":    true,
		"XLEN":         true,
		"XTRIM":        true,
		"XSETID":       true,
		"XGROUP":       true,
		"XACK":         true,
		"XCLAIM":       true,
		"XAUTOCLAIM":   true,
		"XPENDING":     true,
		"XINFO":        true,
		"XINFO GROUPS": true,
		"XINFO STREAM": true,

		// Bitmap Commands
		"SETBIT":      true,
		"GETBIT":      true,
		"BITCOUNT":    true,
		"BITFIELD":    true,
		"BITFIELD_RO": true,
		"BITOP":       true,
		"BITPOS":      true,

		// HyperLogLog Commands
		"PFADD":   true,
		"PFCOUNT": true,
		"PFMERGE": true,

		// Geo Commands
		"GEOADD":            true,
		"GEODIST":           true,
		"GEOHASH":           true,
		"GEOPOS":            true,
		"GEORADIUS":         true,
		"GEORADIUSBYMEMBER": true,

		// Transaction Commands
		"MULTI":   true,
		"EXEC":    true,
		"DISCARD": true,
		"WATCH":   true,
		"UNWATCH": true,

		// Pub/Sub Commands
		"PUBLISH":      true,
		"SUBSCRIBE":    true,
		"PSUBSCRIBE":   true,
		"UNSUBSCRIBE":  true,
		"PUNSUBSCRIBE": true,
		"PUBSUB":       true,

		// Scripting Commands
		"EVAL":          true,
		"EVALSHA":       true,
		"SCRIPT LOAD":   true,
		"SCRIPT EXISTS": true,
		"SCRIPT FLUSH":  true,
		"SCRIPT KILL":   true,

		// Connection Commands
		"AUTH":   true,
		"PING":   true,
		"QUIT":   true,
		"SELECT": true,
		"ECHO":   true,
		"INFO":   true,
		"CLIENT": true,
		"HELLO":  true,

		// Server Commands
		"FLUSHDB":          true,
		"FLUSHALL":         true,
		"DBSIZE":           true,
		"TIME":             true,
		"COMMAND":          true,
		"CONFIG GET":       true,
		"CONFIG SET":       true,
		"CONFIG REWRITE":   true,
		"CONFIG RESETSTAT": true,
		"MONITOR":          true,
		"DEBUG":            true,
		"SLOWLOG":          true,

		// ACL Commands
		"ACL LOAD":    true,
		"ACL SAVE":    true,
		"ACL LIST":    true,
		"ACL USERS":   true,
		"ACL GETUSER": true,
		"ACL SETUSER": true,
		"ACL DELUSER": true,
		"ACL CAT":     true,
		"ACL GENPASS": true,
		"ACL WHOAMI":  true,
		"ACL LOG":     true,
		"ACL HELP":    true,

		// Replication Commands
		"REPLICAOF": true,
		"ROLE":      true,
		"SYNC":      true,
		"PSYNC":     true,
		"REPLCONF":  true,

		// JSON Commands - if you're supporting them
		"JSON.SET":       true,
		"JSON.GET":       true,
		"JSON.DEL":       true,
		"JSON.TYPE":      true,
		"JSON.ARRAPPEND": true,
		"JSON.ARRINSERT": true,
		"JSON.ARRLEN":    true,
		"JSON.ARRTRIM":   true,
		"JSON.ARRPOP":    true,
		"JSON.STRAPPEND": true,
		"JSON.STRLEN":    true,
		"JSON.NUMINCRBY": true,
		"JSON.NUMMULTBY": true,
	}

	return commands
}
