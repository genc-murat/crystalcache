package storage

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"log"

	"github.com/genc-murat/crystalcache/internal/core/models"
	"github.com/genc-murat/crystalcache/pkg/resp"
	"github.com/gofrs/flock"
)

// AOFConfig holds configuration for AOF persistence
type AOFConfig struct {
	Path           string
	SyncStrategy   string // "always", "everysec", "no"
	SyncInterval   time.Duration
	BufferSize     int
	EnableRotation bool
	RotationSize   int64
}

// DefaultAOFConfig returns default configuration
func DefaultAOFConfig() AOFConfig {
	return AOFConfig{
		Path:           "crystalcache.aof",
		SyncStrategy:   "everysec",
		SyncInterval:   time.Second,
		BufferSize:     8 * 1024, // 8KB
		EnableRotation: true,
		RotationSize:   1 << 29, // 512MB
	}
}

type AOF struct {
	config   AOFConfig
	file     *os.File
	writer   *bufio.Writer
	reader   *bufio.Reader
	fileLock *flock.Flock
	mu       sync.RWMutex
	logger   *log.Logger

	// Background sync
	syncCh chan struct{}
	done   chan struct{}

	writeQueue chan models.Value
}

// NewAOF creates a new AOF instance
func NewAOF(config AOFConfig) (*AOF, error) {
	if config.Path == "" {
		config = DefaultAOFConfig()
	}

	// Ensure directory exists
	dir := filepath.Dir(config.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %v", err)
	}

	// File locking to prevent concurrent access
	lock := flock.New(config.Path + ".lock")
	if err := lock.Lock(); err != nil {
		return nil, fmt.Errorf("failed to lock AOF file: %v", err)
	}

	f, err := os.OpenFile(config.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open AOF file: %v", err)
	}

	logFile, err := os.OpenFile("aof.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %v", err)
	}
	logger := log.New(logFile, "AOF: ", log.Ldate|log.Ltime|log.Lshortfile)

	aof := &AOF{
		config:   config,
		file:     f,
		writer:   bufio.NewWriterSize(f, config.BufferSize),
		reader:   bufio.NewReader(f),
		fileLock: lock,
		logger:   logger,
		syncCh:   make(chan struct{}, 100),
		done:     make(chan struct{}),
	}

	// Start background sync if needed
	if config.SyncStrategy == "everysec" {
		go aof.backgroundSync()
	}

	aof.writeQueue = make(chan models.Value, 1000)
	go aof.processWriteQueue()
	return aof, nil
}

func (aof *AOF) Write(value models.Value) error {
	aof.writeQueue <- value
	return nil
}

func (aof *AOF) processWriteQueue() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var batch []models.Value
	for {
		select {
		case value := <-aof.writeQueue:
			batch = append(batch, value)
		case <-ticker.C:
			if len(batch) > 0 {
				aof.writeBatch(batch)
				batch = nil
			}
		case <-aof.done:
			if len(batch) > 0 {
				aof.writeBatch(batch)
			}
			return
		}
	}
}

func (aof *AOF) writeBatch(batch []models.Value) {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	writer := resp.NewWriter(aof.writer)
	for _, value := range batch {
		if err := writer.Write(value); err != nil {
			aof.logger.Printf("Batch write failed: %v", err)
			return
		}
	}

	if aof.config.SyncStrategy == "always" {
		aof.sync()
	} else if aof.config.SyncStrategy == "everysec" {
		select {
		case aof.syncCh <- struct{}{}:
		default:
		}
	}
}

// Read implements Storage interface
func (aof *AOF) Read(callback func(value models.Value)) error {
	aof.mu.RLock()
	defer aof.mu.RUnlock()

	if _, err := aof.file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek failed: %v", err)
	}

	reader := resp.NewReader(aof.reader)
	for {
		value, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read failed: %v", err)
		}
		callback(value)
	}

	return nil
}

// Close implements Storage interface
func (aof *AOF) Close() error {
	close(aof.done) // Stop background sync

	aof.mu.Lock()
	defer aof.mu.Unlock()

	// Ensure all data is written
	if err := aof.sync(); err != nil {
		aof.logger.Printf("Final sync failed: %v", err)
		return fmt.Errorf("final sync failed: %v", err)
	}

	aof.fileLock.Unlock()
	return aof.file.Close()
}

// Internal methods

func (aof *AOF) sync() error {
	if err := aof.writer.Flush(); err != nil {
		aof.logger.Printf("Flush failed: %v", err)
		return err
	}
	if err := aof.file.Sync(); err != nil {
		aof.logger.Printf("File sync failed: %v", err)
		return err
	}
	return nil
}

func (aof *AOF) backgroundSync() {
	ticker := time.NewTicker(aof.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			aof.mu.Lock()
			_ = aof.sync()
			aof.mu.Unlock()
		case <-aof.syncCh:
			aof.mu.Lock()
			_ = aof.sync()
			aof.mu.Unlock()
		case <-aof.done:
			return
		}
	}
}

func (aof *AOF) checkRotation() error {
	info, err := aof.file.Stat()
	if err != nil {
		return err
	}

	if info.Size() >= aof.config.RotationSize {
		return aof.rotate()
	}
	return nil
}

func (aof *AOF) rotate() error {
	// Backup current file
	if err := aof.backupFile(); err != nil {
		return err
	}

	// Sync current file
	if err := aof.sync(); err != nil {
		return err
	}

	// Create rotation file name with timestamp
	timestamp := time.Now().Format("20060102150405")
	rotatedPath := fmt.Sprintf("%s.%s", aof.config.Path, timestamp)

	// Close current file
	if err := aof.file.Close(); err != nil {
		return err
	}

	// Rename current file
	if err := os.Rename(aof.config.Path, rotatedPath); err != nil {
		return err
	}

	// Open new file
	f, err := os.OpenFile(aof.config.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	// Update handlers
	aof.file = f
	aof.writer = bufio.NewWriterSize(f, aof.config.BufferSize)
	aof.reader = bufio.NewReader(f)

	return nil
}

func (aof *AOF) backupFile() error {
	backupPath := fmt.Sprintf("%s.bak", aof.config.Path)
	if err := os.Rename(aof.config.Path, backupPath); err != nil {
		aof.logger.Printf("Backup failed: %v", err)
		return fmt.Errorf("backup failed: %v", err)
	}
	return nil
}

func (aof *AOF) validateData(value models.Value) error {
	if value.Type == "" || (value.Type == "Bulk" && value.Bulk == "") {
		return fmt.Errorf("invalid value: %v", value)
	}
	return nil
}
