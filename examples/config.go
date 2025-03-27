package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the application configuration
type Config struct {
	Flight         FlightConfig         `yaml:"flight"`
	Valkey         ValkeyConfig         `yaml:"valkey"`
	Workers        WorkerConfig         `yaml:"workers"`
	DataGeneration DataGenerationConfig `yaml:"data_generation"`
	Processing     ProcessingConfig     `yaml:"processing"`
	Output         OutputConfig         `yaml:"output"`
	Monitoring     MonitoringConfig     `yaml:"monitoring"`
}

// FlightConfig holds Arrow Flight server configuration
type FlightConfig struct {
	Address string     `yaml:"address"`
	TLS     *TLSConfig `yaml:"tls,omitempty"`
}

// TLSConfig holds TLS configuration
type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// ValkeyConfig holds Valkey (Redis) configuration
type ValkeyConfig struct {
	Address           string        `yaml:"address"`
	Password          string        `yaml:"password"`
	MaxRetries        int           `yaml:"max_retries"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
}

// WorkerConfig holds worker configuration
type WorkerConfig struct {
	Count        int           `yaml:"count"`
	PollInterval time.Duration `yaml:"poll_interval"`
	Backoff      BackoffConfig `yaml:"backoff"`
}

// BackoffConfig holds retry backoff configuration
type BackoffConfig struct {
	Min           time.Duration `yaml:"min"`
	Max           time.Duration `yaml:"max"`
	Factor        float64       `yaml:"factor"`
	Randomization float64       `yaml:"randomization"`
}

// DataGenerationConfig holds data generation configuration
type DataGenerationConfig struct {
	RecordCount int    `yaml:"record_count"`
	BatchSize   int    `yaml:"batch_size"`
	OutputDir   string `yaml:"output_dir"`
	Seed        uint64 `yaml:"seed"`
}

// ProcessingConfig holds data processing configuration
type ProcessingConfig struct {
	FraudDetection FraudDetectionConfig `yaml:"fraud_detection"`
	Enrichment     EnrichmentConfig     `yaml:"enrichment"`
}

// FraudDetectionConfig holds fraud detection configuration
type FraudDetectionConfig struct {
	Enabled   bool    `yaml:"enabled"`
	Threshold float64 `yaml:"threshold"`
}

// EnrichmentConfig holds data enrichment configuration
type EnrichmentConfig struct {
	Enabled     bool   `yaml:"enabled"`
	GeoDatabase string `yaml:"geo_database"`
}

// OutputConfig holds output configuration
type OutputConfig struct {
	Format     string `yaml:"format"`
	Directory  string `yaml:"directory"`
	FilePrefix string `yaml:"file_prefix"`
}

// MonitoringConfig holds monitoring configuration
type MonitoringConfig struct {
	Enabled     bool   `yaml:"enabled"`
	MetricsPort int    `yaml:"metrics_port"`
	LogLevel    string `yaml:"log_level"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(configPath string) (*Config, error) {
	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse YAML
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Parse durations (yaml.v3 doesn't automatically parse duration strings)
	if err := parseDurations(&config); err != nil {
		return nil, fmt.Errorf("failed to parse durations: %w", err)
	}

	return &config, nil
}

// parseDurations parses duration strings in the config
func parseDurations(config *Config) error {
	// Parse heartbeat interval
	heartbeatInterval, err := time.ParseDuration(config.Valkey.HeartbeatInterval.String())
	if err != nil {
		return fmt.Errorf("invalid heartbeat interval: %w", err)
	}
	config.Valkey.HeartbeatInterval = heartbeatInterval

	// Parse poll interval
	pollInterval, err := time.ParseDuration(config.Workers.PollInterval.String())
	if err != nil {
		return fmt.Errorf("invalid poll interval: %w", err)
	}
	config.Workers.PollInterval = pollInterval

	// Parse backoff durations
	backoffMin, err := time.ParseDuration(config.Workers.Backoff.Min.String())
	if err != nil {
		return fmt.Errorf("invalid backoff min: %w", err)
	}
	config.Workers.Backoff.Min = backoffMin

	backoffMax, err := time.ParseDuration(config.Workers.Backoff.Max.String())
	if err != nil {
		return fmt.Errorf("invalid backoff max: %w", err)
	}
	config.Workers.Backoff.Max = backoffMax

	return nil
}
