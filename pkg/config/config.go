package config

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strings"

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
	Addr             string   `yaml:"addr"`
}

// LoadConfig loads configuration from a YAML file or from environment variables
func LoadConfig(bootstrapServers string, inactivityDays int, logLvl, addr string, configFileName string) (*Config, error) {
	config := &Config{
		InactivityDays: 7,
	}

	// Load from file first
	err := loadFromFile(configFileName, config)
	if err != nil {
		GetLogger().Warnf("Error loading config file %s: %v", configFileName, err)
	}
	// Load from env as bigger priority.
	loadFromEnv(config)

	// Override config values with command-line flags if provided as they have higher priority.
	if bootstrapServers != "" {
		config.BootstrapServers = strings.Split(bootstrapServers, ",")
	}

	if inactivityDays > 0 {
		config.InactivityDays = inactivityDays
	}

	if logLvl != "" {
		config.LogLevel = logLvl
	}

	if addr != "" {
		config.Addr = addr
	}

	return config, nil
}

func loadFromEnv(config *Config) {
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

	if listenAddr := os.Getenv("LISTEN_ADDR"); listenAddr != "" {
		config.Addr = listenAddr
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
