package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Cache    CacheConfig    `yaml:"cache"`
	Storage  StorageConfig  `yaml:"storage"`
	Cluster  ClusterConfig  `yaml:"cluster"`
	Pool     PoolConfig     `yaml:"pool"`
	Security SecurityConfig `yaml:"security"`
	Metrics  MetricsConfig  `yaml:"metrics"`
	Logging  LoggingConfig  `yaml:"logging"`
	Limits   LimitsConfig   `yaml:"limits"`
}

type ServerConfig struct {
	Host           string        `yaml:"host"`
	Port           int           `yaml:"port"`
	MaxConnections int           `yaml:"max_connections"`
	Timeouts       TimeoutConfig `yaml:"timeouts"`
	Debug          bool          `yaml:"debug"`
}

type TimeoutConfig struct {
	Read  time.Duration `yaml:"read"`
	Write time.Duration `yaml:"write"`
	Idle  time.Duration `yaml:"idle"`
}

type CacheConfig struct {
	Type            string                `yaml:"type"`
	MaxSize         string                `yaml:"max_size"`
	EvictionPolicy  string                `yaml:"eviction_policy"`
	Defragmentation DefragmentationConfig `yaml:"defragmentation"`
}

type DefragmentationConfig struct {
	Enabled   bool          `yaml:"enabled"`
	Interval  time.Duration `yaml:"interval"`
	Threshold float64       `yaml:"threshold"`
}

type StorageConfig struct {
	Type         string       `yaml:"type"`
	Path         string       `yaml:"path"`
	FileName     string       `yaml:"file_name"`
	SyncStrategy string       `yaml:"sync_strategy"`
	Compression  bool         `yaml:"compression"`
	MaxFileSize  string       `yaml:"max_file_size"`
	Backup       BackupConfig `yaml:"backup"`
}

type BackupConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
	KeepDays int           `yaml:"keep_days"`
	Path     string        `yaml:"path"`
}

type ClusterConfig struct {
	Enabled     bool              `yaml:"enabled"`
	Role        string            `yaml:"role"`
	Replication ReplicationConfig `yaml:"replication"`
	Nodes       []NodeConfig      `yaml:"nodes"`
}

type ReplicationConfig struct {
	MasterHost   string        `yaml:"master_host"`
	MasterPort   int           `yaml:"master_port"`
	SyncInterval time.Duration `yaml:"sync_interval"`
}

type NodeConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type PoolConfig struct {
	InitialSize int          `yaml:"initial_size"`
	MaxSize     int          `yaml:"max_size"`
	MinIdle     int          `yaml:"min_idle"`
	MaxIdle     int          `yaml:"max_idle"`
	Timeouts    PoolTimeouts `yaml:"timeouts"`
	Retry       RetryConfig  `yaml:"retry"`
}

type PoolTimeouts struct {
	Acquire time.Duration `yaml:"acquire"`
	Read    time.Duration `yaml:"read"`
	Write   time.Duration `yaml:"write"`
	Idle    time.Duration `yaml:"idle"`
}

type RetryConfig struct {
	Attempts int           `yaml:"attempts"`
	Delay    time.Duration `yaml:"delay"`
	MaxDelay time.Duration `yaml:"max_delay"`
}

type SecurityConfig struct {
	AuthEnabled bool      `yaml:"auth_enabled"`
	Password    string    `yaml:"password"`
	TLS         TLSConfig `yaml:"tls"`
	ACL         ACLConfig `yaml:"acl"`
}

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	CAFile   string `yaml:"ca_file"`
}

type ACLConfig struct {
	Enabled   bool   `yaml:"enabled"`
	RulesFile string `yaml:"rules_file"`
}

type MetricsConfig struct {
	Enabled    bool             `yaml:"enabled"`
	Port       int              `yaml:"port"`
	Path       string           `yaml:"path"`
	Collectors []string         `yaml:"collectors"`
	Prometheus PrometheusConfig `yaml:"prometheus"`
}

type PrometheusConfig struct {
	Enabled     bool   `yaml:"enabled"`
	PushGateway string `yaml:"push_gateway"`
}

type LoggingConfig struct {
	Level  string     `yaml:"level"`
	Format string     `yaml:"format"`
	Output string     `yaml:"output"`
	File   FileConfig `yaml:"file"`
}

type FileConfig struct {
	Path       string `yaml:"path"`
	MaxSize    string `yaml:"max_size"`
	MaxAge     int    `yaml:"max_age"`
	MaxBackups int    `yaml:"max_backups"`
	Compress   bool   `yaml:"compress"`
}

type LimitsConfig struct {
	MaxItemSize    string          `yaml:"max_item_size"`
	MaxRequestSize string          `yaml:"max_request_size"`
	RateLimit      RateLimitConfig `yaml:"rate_limit"`
}

type RateLimitConfig struct {
	Enabled           bool `yaml:"enabled"`
	RequestsPerSecond int  `yaml:"requests_per_second"`
	Burst             int  `yaml:"burst"`
}

func LoadConfig(env string) (*Config, error) {
	filename := fmt.Sprintf("config/%s.yaml", env)
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing config file: %w", err)
	}

	return &config, nil
}
