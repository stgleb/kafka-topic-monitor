package monitor

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"

	. "kafka-topic-monitor/pkg/logger"
)

// Monitor struct to manage Kafka connections and operations
type Monitor struct {
	BootstrapServers []string
	ListenAddr       string
	InactivityDays   int

	client sarama.Client
	admin  sarama.ClusterAdmin

	checker  TopicChecker
	reporter Reporter

	reportTaskChan chan chan []byte
}

type TopicChecker interface {
	CheckTopic(context.Context, string, sarama.Client, sarama.ClusterAdmin) (*TopicActivityInfo, error)
}

type Reporter interface {
	Report([]*TopicActivityInfo) ([]byte, error)
}

// NewMonitor creates a new Monitor instance
func NewMonitor(servers []string, inActivityDays int, ListenAddr string, checker TopicChecker, reporter Reporter) (*Monitor, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Net.SASL.Enable = false
	config.Net.TLS.Enable = false

	client, err := sarama.NewClient(servers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka cluster admin: %w", err)
	}

	return &Monitor{
		BootstrapServers: servers,
		ListenAddr:       ListenAddr,
		InactivityDays:   inActivityDays,

		client:         client,
		admin:          admin,
		checker:        checker,
		reporter:       reporter,
		reportTaskChan: make(chan chan []byte),
	}, nil
}

// ListTopics lists the Kafka topics available in the connected cluster
func (m *Monitor) ListTopics() ([]string, error) {
	topics, err := m.client.Topics()
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}
	return topics, nil
}

// Start initiates the monitoring loop
func (m *Monitor) Start(ctx context.Context) {
	GetLogger().Infof("Starting Kafka Monitor...")
	defer m.Close() // Ensure the client is closed when exiting the loop
	// Start the HTTP server
	if err := StartHTTPServer(ctx, m.ListenAddr, m.reportTaskChan); err != nil {
		GetLogger().Fatalf("Failed to start HTTP server: %v\n", err)
	}
	for {
		select {
		case <-ctx.Done():
			GetLogger().Infof("Shutdown signal received.")
			m.Close()
			return
		case reportChan := <-m.reportTaskChan:
			topics, err := m.ListTopics()
			if err != nil {
				GetLogger().Infof("Failed to list topics: %v\n", err)
				continue // Proceed to the next iteration
			}
			var topicActivityInfos []*TopicActivityInfo
			for _, topic := range topics {
				GetLogger().Infof("topic: %s\n", topic)
				info, err := m.checker.CheckTopic(ctx, topic, m.client, m.admin)
				if err != nil {
					GetLogger().Errorf("failed to check topic %s: %v", topic, err)
					continue
				}
				info.Active = isActive(info.LastWriteTime, info.LastReadTime, m.InactivityDays)
				topicActivityInfos = append(topicActivityInfos, info)
			}
			report, err := m.reporter.Report(topicActivityInfos)
			if err != nil {
				GetLogger().Errorf("failed to report topics: %v", err)
				continue
			}
			reportChan <- report
		}
	}
}

// Close shuts down the Kafka client connection
func (m *Monitor) Close() {
	if err := m.client.Close(); err != nil {
		GetLogger().Infof("Error closing Kafka client: %v\n", err)
	}

	if err := m.admin.Close(); err != nil {
		GetLogger().Infof("Error closing Kafka admin: %v\n", err)
	}
}

// isActive checks if the topic is active based on the last write and read times.
func isActive(lastWriteTime, lastReadTime time.Time, inactivityDays int) bool {
	// Check if the topic is active based on the last write and read times
	if lastWriteTime.IsZero() && lastReadTime.IsZero() {
		return false
	}

	inactivityDuration := time.Duration(inactivityDays) * 24 * time.Hour
	return time.Since(lastWriteTime) < inactivityDuration || time.Since(lastReadTime) < inactivityDuration
}
