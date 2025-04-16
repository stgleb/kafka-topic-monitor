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
	"kafka-topic-monitor/pkg/monitor/report"
)

func main() {
	configFile := flag.String("config-file", "config.yml", "Path to the configuration file")

	// Define temporary variable for comma-separated bootstrap servers
	var (
		bootstrapServers string
		inactivityDays   int
		logLevel         string
		addr             string
	)

	// Define command line flags
	flag.StringVar(&bootstrapServers, "bootstrap-servers", "", "Comma-separated list of Kafka bootstrap servers")
	flag.IntVar(&inactivityDays, "inactivity-days", 0, "Number of days to consider a topic inactive")
	flag.StringVar(&logLevel, "log-level", "", "Log level (debug, info, warn, error)")
	flag.StringVar(&addr, "addr", "", "HTTP server address")

	// Parse the command-line flags
	flag.Parse()

	// First load from file and env.
	cfg, err := config.LoadConfig(bootstrapServers, inactivityDays, logLevel, addr, *configFile)
	if err != nil {
		logger.GetLogger().Fatalf("Error loading configuration: %v", err)
	}

	lvl, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		lvl = logrus.InfoLevel
	}
	logger.NewLogger(os.Stdout, lvl)

	reporter := report.NewCsvReporter()
	checker := monitor.NewTopicChecker()
	m, err := monitor.NewMonitor(cfg.BootstrapServers, cfg.InactivityDays, cfg.Addr, checker, reporter)
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
