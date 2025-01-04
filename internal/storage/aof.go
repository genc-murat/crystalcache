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
	config AOFConfig

	file     *os.File
	writer   *bufio.Writer
	reader   *bufio.Reader
	fileLock *flock.Flock

	writeQueue chan models.Value

	syncGroup sync.WaitGroup
	done      chan struct{}

	logger *log.Logger
}

// NewAOF creates a new AOF instance
func NewAOF(config AOFConfig) (*AOF, error) {
	if config.Path == "" {
		config = DefaultAOFConfig()
	}

	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(config.Path), 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Initialize logger
	logFile, err := os.OpenFile("aof.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	logger := log.New(logFile, "AOF: ", log.Ldate|log.Ltime|log.Lshortfile)

	// Acquire file lock
	lock := flock.New(config.Path + ".lock")
	if err := lock.Lock(); err != nil {
		return nil, fmt.Errorf("acquire lock: %w", err)
	}

	// Open AOF file
	file, err := os.OpenFile(config.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		lock.Unlock() // Release lock on failure
		return nil, fmt.Errorf("open AOF file: %w", err)
	}

	aof := &AOF{
		config:     config,
		file:       file,
		writer:     bufio.NewWriterSize(file, config.BufferSize),
		reader:     bufio.NewReader(file),
		fileLock:   lock,
		writeQueue: make(chan models.Value, 1000),
		done:       make(chan struct{}),
		logger:     logger,
	}

	// Start the write queue processor
	aof.syncGroup.Add(1)
	go aof.processWriteQueue()

	// Start background sync if needed
	if config.SyncStrategy == "everysec" {
		aof.syncGroup.Add(1)
		go aof.backgroundSync()
	}

	return aof, nil
}

// Write adds a value to the write queue for AOF persistence.
func (aof *AOF) Write(value models.Value) error {
	select {
	case aof.writeQueue <- value:
		return nil
	case <-aof.done:
		return fmt.Errorf("AOF is closed")
	}
}

// processWriteQueue processes the write queue, batching writes for efficiency.
func (aof *AOF) processWriteQueue() {
	defer aof.syncGroup.Done()

	var batch []models.Value
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case value := <-aof.writeQueue:
			batch = append(batch, value)
		case <-ticker.C:
			if len(batch) > 0 {
				if err := aof.writeBatch(batch); err != nil {
					aof.logger.Printf("Error writing batch: %v", err)
				}
				batch = nil
			}
		case <-aof.done:
			if len(batch) > 0 {
				if err := aof.writeBatch(batch); err != nil {
					aof.logger.Printf("Error writing final batch: %v", err)
				}
			}
			return
		}
	}
}

// writeBatch writes a batch of values to the AOF file.
func (aof *AOF) writeBatch(batch []models.Value) error {
	writer := resp.NewWriter(aof.writer)
	for _, value := range batch {
		if err := writer.Write(value); err != nil {
			return fmt.Errorf("write value to AOF: %w", err)
		}
	}

	// Trigger sync based on the configured strategy
	if aof.config.SyncStrategy == "always" {
		if err := aof.sync(); err != nil {
			aof.logger.Printf("Error syncing AOF (always): %v", err)
		}
	} else if aof.config.SyncStrategy == "everysec" {
		// Synchronization will be handled by the background sync goroutine
	}
	return nil
}

// Read reads all entries from the AOF file and applies the callback.
func (aof *AOF) Read(callback func(value models.Value)) error {
	if _, err := aof.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start of AOF: %w", err)
	}
	aof.reader.Reset(aof.file)
	reader := resp.NewReader(aof.reader)

	for {
		value, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read from AOF: %w", err)
		}
		callback(value)
	}
	return nil
}

// Close closes the AOF file and releases the lock.
func (aof *AOF) Close() error {
	close(aof.done)
	aof.syncGroup.Wait() // Wait for all background processes to finish

	if err := aof.sync(); err != nil {
		aof.logger.Printf("Error on final AOF sync: %v", err)
	}

	if err := aof.writer.Flush(); err != nil && err != io.ErrClosedPipe {
		aof.logger.Printf("Error flushing AOF writer: %v", err)
	}

	if err := aof.file.Close(); err != nil {
		aof.logger.Printf("Error closing AOF file: %v", err)
	}

	if err := aof.fileLock.Unlock(); err != nil {
		aof.logger.Printf("Error unlocking AOF file: %v", err)
	}

	return nil
}

// sync flushes the writer buffer and synchronizes the file to disk.
func (aof *AOF) sync() error {
	if err := aof.writer.Flush(); err != nil {
		return fmt.Errorf("flush AOF writer: %w", err)
	}
	if err := aof.file.Sync(); err != nil {
		return fmt.Errorf("sync AOF file: %w", err)
	}
	return nil
}

// backgroundSync periodically synchronizes the AOF file to disk.
func (aof *AOF) backgroundSync() {
	defer aof.syncGroup.Done()

	ticker := time.NewTicker(aof.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := aof.sync(); err != nil {
				aof.logger.Printf("Error on background AOF sync: %v", err)
			}
		case <-aof.done:
			return
		}
	}
}

// checkRotation checks if the AOF file size exceeds the rotation threshold and rotates the file if necessary.
func (aof *AOF) checkRotation() error {
	if !aof.config.EnableRotation {
		return nil
	}
	info, err := aof.file.Stat()
	if err != nil {
		return fmt.Errorf("get AOF file stat: %w", err)
	}

	if info.Size() >= aof.config.RotationSize {
		return aof.rotate()
	}
	return nil
}

// rotate performs the AOF file rotation.
func (aof *AOF) rotate() error {
	// Generate new file name
	timestamp := time.Now().Format("20060102150405")
	rotatedPath := fmt.Sprintf("%s.%s", aof.config.Path, timestamp)

	// Close current file
	if err := aof.file.Close(); err != nil {
		return fmt.Errorf("close current AOF file for rotation: %w", err)
	}

	// Rename current file
	if err := os.Rename(aof.config.Path, rotatedPath); err != nil {
		return fmt.Errorf("rename AOF file during rotation: %w", err)
	}

	// Open a new AOF file
	file, err := os.OpenFile(aof.config.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("open new AOF file after rotation: %w", err)
	}

	// Update AOF struct with the new file
	aof.file = file
	aof.writer = bufio.NewWriterSize(file, aof.config.BufferSize)
	aof.reader = bufio.NewReader(file)

	return nil
}

// backupFile creates a backup of the current AOF file.
func (aof *AOF) backupFile() error {
	backupPath := fmt.Sprintf("%s.bak", aof.config.Path)
	if err := os.Rename(aof.config.Path, backupPath); err != nil {
		return fmt.Errorf("rename AOF for backup: %w", err)
	}
	return nil
}

// validateData performs basic validation on the data before writing.
func (aof *AOF) validateData(value models.Value) error {
	if value.Type == "" || (value.Type == "Bulk" && value.Bulk == "") {
		return fmt.Errorf("invalid value for AOF: %+v", value)
	}
	return nil
}
