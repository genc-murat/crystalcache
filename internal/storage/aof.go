package storage

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/pkg/resp"
)

type AOF struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &AOF{
		file: f,
		rd:   bufio.NewReader(f),
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
		}
	}()

	return aof, nil
}

func (aof *AOF) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

func (aof *AOF) Write(value models.Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	writer := resp.NewWriter(aof.file)
	return writer.Write(value)
}

func (aof *AOF) Read(callback func(value models.Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	reader := resp.NewReader(aof.file)
	for {
		value, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		callback(value)
	}
	return nil
}
