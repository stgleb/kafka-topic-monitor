package monitor

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"

	. "kafka-topic-monitor/pkg/logger"
)

// Monitor struct to manage Kafka connections and operations
type Monitor struct {
	BootstrapServers []string
	client           sarama.Client
	admin            sarama.ClusterAdmin

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
func NewMonitor(servers []string, checker TopicChecker, reporter Reporter) (*Monitor, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0 // Specify supported Kafka version

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
		client:           client,
		admin:            admin,
		checker:          checker,
		reportTaskChan:   make(chan chan []byte),
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
	fmt.Println("Starting Kafka Monitor...")
	defer m.Close() // Ensure the client is closed when exiting the loop
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Shutdown signal received.")
			m.Close()
			return
		case reportChan := <-m.reportTaskChan:
			topics, err := m.ListTopics()
			if err != nil {
				fmt.Printf("Failed to list topics: %v\n", err)
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
		fmt.Printf("Error closing Kafka client: %v\n", err)
	}

	if err := m.admin.Close(); err != nil {
		fmt.Printf("Error closing Kafka admin: %v\n", err)
	}
}
