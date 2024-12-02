package types

type CommandType int

const (
	ReadCommand CommandType = iota
	WriteCommand
	AdminCommand
)

var CommandTypes = map[string]CommandType{
	// Read Commands
	"GET":       ReadCommand,
	"HGET":      ReadCommand,
	"HGETALL":   ReadCommand,
	"LRANGE":    ReadCommand,
	"SMEMBERS":  ReadCommand,
	"SCARD":     ReadCommand,
	"SISMEMBER": ReadCommand,
	"TYPE":      ReadCommand,
	"EXISTS":    ReadCommand,
	"TTL":       ReadCommand,
	"KEYS":      ReadCommand,
	"ZCARD":     ReadCommand,
	"ZCOUNT":    ReadCommand,
	"ZRANGE":    ReadCommand,
	"ZRANK":     ReadCommand,
	"ZSCORE":    ReadCommand,
	"ZREVRANGE": ReadCommand,
	"INFO":      ReadCommand,
	"PFCOUNT":   ReadCommand,

	// Write Commands
	"SET":     WriteCommand,
	"HSET":    WriteCommand,
	"LPUSH":   WriteCommand,
	"RPUSH":   WriteCommand,
	"SADD":    WriteCommand,
	"SREM":    WriteCommand,
	"ZADD":    WriteCommand,
	"ZREM":    WriteCommand,
	"ZINCRBY": WriteCommand,
	"PFADD":   WriteCommand,
	"PFMERGE": WriteCommand,
	"DEL":     WriteCommand,
	"EXPIRE":  WriteCommand,
	"RENAME":  WriteCommand,

	// Admin Commands
	"FLUSHALL": AdminCommand,
	"MULTI":    AdminCommand,
	"EXEC":     AdminCommand,
	"DISCARD":  AdminCommand,
	"WATCH":    AdminCommand,
	"UNWATCH":  AdminCommand,
}

func GetCommandType(cmd string) CommandType {
	if cmdType, exists := CommandTypes[cmd]; exists {
		return cmdType
	}
	return ReadCommand // default to read command for safety
}
