package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"kafka-topic-monitor/pkg/config"
	"kafka-topic-monitor/pkg/logger"
	"kafka-topic-monitor/pkg/monitor"
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

	reporter := monitor.NewCsvReporter()
	checker := monitor.NewTopicChecker()
	m, err := monitor.NewMonitor(cfg.BootstrapServers, checker, reporter)
	if err != nil {
		logger.GetLogger().Fatalf("Error creating monitor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go gracefulShutdown(cancel)
	m.Start(ctx)
}

func gracefulShutdown(cancel context.CancelFunc) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
}
