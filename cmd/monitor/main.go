package main

import (
	"flag"
	"os"

	"github.com/sirupsen/logrus"

	"kafka-topic-monitor/pkg/config"
	"kafka-topic-monitor/pkg/logger"
)

func main() {
	configFile := flag.String("config-file", "config.yml", "Path to the configuration file")

	// Parse the command-line flags
	flag.Parse()

	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		logger.GetLogger().Fatalf("Error loading configuration: %v", err)
	}
	lvl, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		lvl = logrus.InfoLevel
	}

	logger.NewLogger(os.Stdout, lvl)
}
