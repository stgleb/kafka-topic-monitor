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
	Client           sarama.Client
	reportTaskChan   chan chan Report
}

// NewMonitor creates a new Monitor instance
func NewMonitor(servers []string) (*Monitor, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0 // Specify supported Kafka version

	client, err := sarama.NewClient(servers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	return &Monitor{
		BootstrapServers: servers,
		Client:           client,
		reportTaskChan:   make(chan chan Report),
	}, nil
}

// ListTopics lists the Kafka topics available in the connected cluster
func (m *Monitor) ListTopics() ([]string, error) {
	topics, err := m.Client.Topics()
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
			return
		case reportChan := <-m.reportTaskChan:
			topics, err := m.ListTopics()
			if err != nil {
				fmt.Printf("Failed to list topics: %v\n", err)
				continue // Proceed to the next iteration
			}
			for _, topic := range topics {
				// TODO(stgleb): process all topics. Find out last write/reads timestamp.
				GetLogger().Infof("topic: %s\n", topic)
			}
			// TODO(stgleb): write report here.
			reportChan <- Report{}
		}
	}
}

// Close shuts down the Kafka client connection
func (m *Monitor) Close() {
	if err := m.Client.Close(); err != nil {
		fmt.Printf("Error closing Kafka client: %v\n", err)
	}
}
