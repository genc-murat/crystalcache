package ports

type Cache interface {
	Set(key string, value string) error
	Get(key string) (string, bool)
	HSet(hash string, key string, value string) error
	HGet(hash string, key string) (string, bool)
	HGetAll(hash string) map[string]string
	Incr(key string) (int, error)
	Expire(key string, seconds int) error
	Del(key string) (bool, error)
	Keys(pattern string) []string
	TTL(key string) int // TTL in seconds, -2 if not exists, -1 if no expire
	LPush(key string, value string) (int, error)
	RPush(key string, value string) (int, error)
	LRange(key string, start, stop int) ([]string, error)
	SAdd(key string, member string) (bool, error)
	SMembers(key string) ([]string, error)
	LLen(key string) int
	LPop(key string) (string, bool)
	RPop(key string) (string, bool)
	SCard(key string) int
	SRem(key string, member string) (bool, error)
}
