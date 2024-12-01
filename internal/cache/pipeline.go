package cache

import (
	"github.com/genc-murat/crystalcache/internal/core/models"
)

type Pipeline struct {
	cache    *MemoryCache
	commands []models.PipelineCommand
}

func NewPipeline(cache *MemoryCache) *Pipeline {
	return &Pipeline{
		cache:    cache,
		commands: make([]models.PipelineCommand, 0),
	}
}

// Pipeline komut metodları
func (p *Pipeline) Set(key, value string) {
	p.commands = append(p.commands, models.PipelineCommand{
		Name: "SET",
		Args: []models.Value{
			{Type: "bulk", Bulk: key},
			{Type: "bulk", Bulk: value},
		},
	})
}

func (p *Pipeline) Get(key string) {
	p.commands = append(p.commands, models.PipelineCommand{
		Name: "GET",
		Args: []models.Value{
			{Type: "bulk", Bulk: key},
		},
	})
}

func (p *Pipeline) HSet(hash, key, value string) {
	p.commands = append(p.commands, models.PipelineCommand{
		Name: "HSET",
		Args: []models.Value{
			{Type: "bulk", Bulk: hash},
			{Type: "bulk", Bulk: key},
			{Type: "bulk", Bulk: value},
		},
	})
}

func (p *Pipeline) HGet(hash, key string) {
	p.commands = append(p.commands, models.PipelineCommand{
		Name: "HGET",
		Args: []models.Value{
			{Type: "bulk", Bulk: hash},
			{Type: "bulk", Bulk: key},
		},
	})
}

func (p *Pipeline) Execute() []models.Value {
	results := make([]models.Value, 0, len(p.commands))

	// Tüm mutex'leri kilitle
	p.cache.setsMu.Lock()
	p.cache.hsetsMu.Lock()
	p.cache.listsMu.Lock()
	p.cache.setsMu_.Lock()
	defer p.cache.setsMu.Unlock()
	defer p.cache.hsetsMu.Unlock()
	defer p.cache.listsMu.Unlock()
	defer p.cache.setsMu_.Unlock()

	for _, cmd := range p.commands {
		var result models.Value
		switch cmd.Name {
		case "SET":
			err := p.cache.Set(cmd.Args[0].Bulk, cmd.Args[1].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "string", Str: "OK"}
			}

		case "GET":
			value, exists := p.cache.Get(cmd.Args[0].Bulk)
			if !exists {
				result = models.Value{Type: "null"}
			} else {
				result = models.Value{Type: "bulk", Bulk: value}
			}

		case "HSET":
			err := p.cache.HSet(cmd.Args[0].Bulk, cmd.Args[1].Bulk, cmd.Args[2].Bulk)
			if err != nil {
				result = models.Value{Type: "error", Str: err.Error()}
			} else {
				result = models.Value{Type: "string", Str: "OK"}
			}

		case "HGET":
			value, exists := p.cache.HGet(cmd.Args[0].Bulk, cmd.Args[1].Bulk)
			if !exists {
				result = models.Value{Type: "null"}
			} else {
				result = models.Value{Type: "bulk", Bulk: value}
			}

		default:
			result = models.Value{Type: "error", Str: "ERR unknown command " + cmd.Name}
		}

		results = append(results, result)
	}

	// Pipeline'ı temizle
	p.commands = p.commands[:0]

	return results
}
