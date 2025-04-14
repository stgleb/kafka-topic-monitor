package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	. "kafka-topic-monitor/pkg/logger"
)

var (
	ErrEmptyBootstrapServers = errors.New("empty bootstrap server list")
)

// Config holds the configuration values
type Config struct {
	BootstrapServers []string `yaml:"bootstrap_servers"`
	InactivityDays   int      `yaml:"inactivity_days"`
	LogLevel         string   `yaml:"log_level"`
}

// LoadConfig loads configuration from a YAML file or from environment variables
func LoadConfig(configFileName string) (*Config, error) {
	config := Config{
		InactivityDays: 7,
	}

	// Load from file first
	err := loadFromFile(configFileName, &config)
	if err != nil {
		GetLogger().Warnf("Error loading config file %s: %v", configFileName, err)
	}
	// Load from env as bigger priority.
	loadFromEnv(config)
	if config.BootstrapServers == nil {
		return nil, ErrEmptyBootstrapServers
	}
	return &config, nil
}

func loadFromEnv(config Config) {
	// Override with environment variables if they exist
	if bootstrapServers := os.Getenv("BOOTSTRAP_SERVERS"); bootstrapServers != "" {
		config.BootstrapServers = []string{bootstrapServers}
	}

	if inactivityDays := os.Getenv("INACTIVITY_DAYS"); inactivityDays != "" {
		var value int
		if _, err := fmt.Sscanf(inactivityDays, "%d", &value); err == nil {
			config.InactivityDays = value
		}
	}
}

func loadFromFile(configFileName string, config *Config) error {
	file, err := os.Open(configFileName)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(config)
	if err != nil {
		return err
	}
	return nil
}
