package app

import (
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/internal/core/ports"
	"github.com/genc-murat/crystalcache/pkg/resp"
)

type Server struct {
	cache   ports.Cache
	storage ports.Storage
	cmds    map[string]CommandHandler
}

type CommandHandler func(args []models.Value) models.Value

func NewServer(cache ports.Cache, storage ports.Storage) *Server {
	s := &Server{
		cache:   cache,
		storage: storage,
		cmds:    make(map[string]CommandHandler),
	}

	s.registerHandlers()
	return s
}

func (s *Server) registerHandlers() {
	s.cmds["PING"] = s.handlePing
	s.cmds["SET"] = s.handleSet
	s.cmds["GET"] = s.handleGet
	s.cmds["HSET"] = s.handleHSet
	s.cmds["HGET"] = s.handleHGet
	s.cmds["HGETALL"] = s.handleHGetAll
	s.cmds["INCR"] = s.handleIncr
	s.cmds["EXPIRE"] = s.handleExpire
	s.cmds["DEL"] = s.handleDel
	s.cmds["KEYS"] = s.handleKeys
	s.cmds["TTL"] = s.handleTTL
	s.cmds["LPUSH"] = s.handleLPush
	s.cmds["RPUSH"] = s.handleRPush
	s.cmds["LRANGE"] = s.handleLRange
	s.cmds["SADD"] = s.handleSAdd
	s.cmds["SMEMBERS"] = s.handleSMembers
}

func (s *Server) Start(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()

	// Load data from AOF
	s.storage.Read(func(value models.Value) {
		if len(value.Array) == 0 {
			return
		}
		cmd := strings.ToUpper(value.Array[0].Bulk)
		handler, exists := s.cmds[cmd]
		if !exists {
			return
		}
		handler(value.Array[1:])
	})

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	for {
		value, err := reader.Read()
		if err != nil {
			log.Printf("Error reading from connection: %v", err)
			return
		}

		if value.Type != "array" || len(value.Array) == 0 {
			continue
		}

		cmd := strings.ToUpper(value.Array[0].Bulk)
		handler, exists := s.cmds[cmd]
		if !exists {
			writer.Write(models.Value{Type: "error", Str: "ERR unknown command"})
			continue
		}

		// Persist commands that modify state
		if cmd == "SET" || cmd == "HSET" {
			if err := s.storage.Write(value); err != nil {
				log.Printf("Error writing to AOF: %v", err)
			}
		}

		result := handler(value.Array[1:])
		writer.Write(result)
	}
}

func (s *Server) handlePing(args []models.Value) models.Value {
	if len(args) == 0 {
		return models.Value{Type: "string", Str: "PONG"}
	}
	return models.Value{Type: "string", Str: args[0].Bulk}
}

func (s *Server) handleSet(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	err := s.cache.Set(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (s *Server) handleGet(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'get' command"}
	}

	value, exists := s.cache.Get(args[0].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

func (s *Server) handleHSet(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}

	err := s.cache.HSet(args[0].Bulk, args[1].Bulk, args[2].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "string", Str: "OK"}
}

func (s *Server) handleHGet(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}

	value, exists := s.cache.HGet(args[0].Bulk, args[1].Bulk)
	if !exists {
		return models.Value{Type: "null"}
	}

	return models.Value{Type: "bulk", Bulk: value}
}

func (s *Server) handleHGetAll(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].Bulk
	pairs := s.cache.HGetAll(hash)

	// RESP protokolüne göre key-value çiftlerini array olarak dönüyoruz
	result := make([]models.Value, 0, len(pairs)*2)
	for key, value := range pairs {
		result = append(result,
			models.Value{Type: "bulk", Bulk: key},
			models.Value{Type: "bulk", Bulk: value},
		)
	}

	return models.Value{Type: "array", Array: result}
}

func (s *Server) handleIncr(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'incr' command"}
	}

	result, err := s.cache.Incr(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: result}
}

func (s *Server) handleExpire(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'expire' command"}
	}

	seconds, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
	}

	err = s.cache.Expire(args[0].Bulk, seconds)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: 1}
}

func (s *Server) handleDel(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'del' command"}
	}

	deleted, err := s.cache.Del(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if deleted {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (s *Server) handleKeys(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'keys' command"}
	}

	keys := s.cache.Keys(args[0].Bulk)
	result := make([]models.Value, len(keys))
	for i, key := range keys {
		result[i] = models.Value{Type: "bulk", Bulk: key}
	}

	return models.Value{Type: "array", Array: result}
}

func (s *Server) handleTTL(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'ttl' command"}
	}

	ttl := s.cache.TTL(args[0].Bulk)
	return models.Value{Type: "integer", Num: ttl}
}

func (s *Server) handleLPush(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'lpush' command"}
	}

	length, err := s.cache.LPush(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: length}
}

func (s *Server) handleRPush(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'rpush' command"}
	}

	length, err := s.cache.RPush(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	return models.Value{Type: "integer", Num: length}
}

func (s *Server) handleLRange(args []models.Value) models.Value {
	if len(args) != 3 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'lrange' command"}
	}

	start, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	stop, err := strconv.Atoi(args[2].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: "ERR value is not an integer"}
	}

	values, err := s.cache.LRange(args[0].Bulk, start, stop)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, len(values))
	for i, value := range values {
		result[i] = models.Value{Type: "bulk", Bulk: value}
	}

	return models.Value{Type: "array", Array: result}
}

func (s *Server) handleSAdd(args []models.Value) models.Value {
	if len(args) != 2 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'sadd' command"}
	}

	added, err := s.cache.SAdd(args[0].Bulk, args[1].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	if added {
		return models.Value{Type: "integer", Num: 1}
	}
	return models.Value{Type: "integer", Num: 0}
}

func (s *Server) handleSMembers(args []models.Value) models.Value {
	if len(args) != 1 {
		return models.Value{Type: "error", Str: "ERR wrong number of arguments for 'smembers' command"}
	}

	members, err := s.cache.SMembers(args[0].Bulk)
	if err != nil {
		return models.Value{Type: "error", Str: err.Error()}
	}

	result := make([]models.Value, len(members))
	for i, member := range members {
		result[i] = models.Value{Type: "bulk", Bulk: member}
	}

	return models.Value{Type: "array", Array: result}
}
