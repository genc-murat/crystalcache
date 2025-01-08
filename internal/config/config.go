// internal/config/config.go
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Environment string        `yaml:"environment"`
	Server      ServerConfig  `yaml:"server"`
	Cache       CacheConfig   `yaml:"cache"`
	Storage     StorageConfig `yaml:"storage"`
	Pool        PoolConfig    `yaml:"pool"`
	Metrics     MetricsConfig `yaml:"metrics"`
	Pprof       PprofConfig   `yaml:"pprof"`
}

type ServerConfig struct {
	Host           string        `yaml:"host"`
	Port           int           `yaml:"port"`
	MaxConnections int           `yaml:"max_connections"`
	ReadTimeout    time.Duration `yaml:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout"`
	IdleTimeout    time.Duration `yaml:"idle_timeout"`
}

type CacheConfig struct {
	DefragInterval  time.Duration `yaml:"defrag_interval"`
	DefragThreshold float64       `yaml:"defrag_threshold"`
}

type StorageConfig struct {
	Type         string        `yaml:"type"`
	Path         string        `yaml:"path"`
	SyncInterval time.Duration `yaml:"sync_interval"`
}

type PoolConfig struct {
	InitialSize   int           `yaml:"initial_size"`
	MaxSize       int           `yaml:"max_size"`
	ReadTimeout   time.Duration `yaml:"read_timeout"`
	WriteTimeout  time.Duration `yaml:"write_timeout"`
	IdleTimeout   time.Duration `yaml:"idle_timeout"`
	RetryAttempts int           `yaml:"retry_attempts"`
	RetryDelay    time.Duration `yaml:"retry_delay"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	Path    string `yaml:"path"`
}

type PprofConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

func findProjectRoot() (string, error) {
	// Start from the current working directory
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Walk up the directory tree until we find the config directory
	for {
		// Check if config directory exists
		if _, err := os.Stat(filepath.Join(dir, "config")); err == nil {
			return dir, nil
		}

		// Get the parent directory
		parent := filepath.Dir(dir)
		if parent == dir {
			// We've reached the root directory without finding the config directory
			return "", fmt.Errorf("could not find project root (no config directory found)")
		}
		dir = parent
	}
}

func LoadConfig(env string) (*Config, error) {
	// Find the project root directory
	projectRoot, err := findProjectRoot()
	if err != nil {
		return nil, fmt.Errorf("error finding project root: %v", err)
	}

	// Try loading with .yaml extension first
	configPath := filepath.Join(projectRoot, "config", fmt.Sprintf("%s.yaml", env))

	data, err := os.ReadFile(configPath)
	if err != nil {
		// If .yaml doesn't exist, try .yml
		configPath = filepath.Join(projectRoot, "config", fmt.Sprintf("%s.yml", env))
		data, err = os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("error reading config file: %v", err)
		}
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing config file: %v", err)
	}

	// Set environment
	config.Environment = env

	return &config, nil
}
