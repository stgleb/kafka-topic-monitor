package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/IBM/sarama"

	. "kafka-topic-monitor/pkg/logger"
)

type Config struct {
	KafkaBrokers  string
	NumTopics     int
	MaxPartitions int
	StartDate     string
	EndDate       string
	MaxMessages   int
	TopicPrefix   string
}

type Message struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Value     int       `json:"value"`
}

func main() {
	// Parse command line arguments
	config := parseFlags()

	// Set up context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Parse date range
	startDate, endDate, err := parseDateRange(config.StartDate, config.EndDate)
	if err != nil {
		GetLogger().Fatalf("Error parsing date range: %v", err)
	}

	// Create Kafka client
	client, admin, producer, err := setupKafka(config.KafkaBrokers)
	if err != nil {
		GetLogger().Fatalf("Error setting up Kafka: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			GetLogger().Warnf("Error closing producer: %v", err)
		}
		if err := admin.Close(); err != nil {
			GetLogger().Warnf("Error closing admin client: %v", err)
		}
		if err := client.Close(); err != nil {
			GetLogger().Warnf("Error closing client: %v", err)
		}
	}()

	// Generate and send messages
	err = generateMessages(ctx, admin, producer, config, startDate, endDate)
	if err != nil {
		GetLogger().Fatalf("Error generating messages: %v", err)
	}

	if err := ProcessTopicsSelectively(client, admin, "test-consumer-group", config.TopicPrefix); err != nil {
		GetLogger().Fatalf("Error processing topics: %v", err)
	}
	GetLogger().Infof("Message generation completed successfully")
}

func parseFlags() *Config {
	config := &Config{}

	flag.StringVar(&config.KafkaBrokers, "brokers", "localhost:9092", "Kafka brokers (comma-separated)")
	flag.IntVar(&config.NumTopics, "topics", 3, "Number of topics to create")
	flag.IntVar(&config.MaxPartitions, "partitions", 3, "Maximum number of partitions per topic (random 1 to this value)")
	flag.StringVar(&config.StartDate, "start-date", time.Now().AddDate(0, -1, 0).Format("2006-01-02"), "Start date for message timestamps (YYYY-MM-DD)")
	flag.StringVar(&config.EndDate, "end-date", time.Now().Format("2006-01-02"), "End date for message timestamps (YYYY-MM-DD)")
	flag.IntVar(&config.MaxMessages, "messages", 100, "Maximum number of messages per topic")
	flag.StringVar(&config.TopicPrefix, "prefix", "test-topic-", "Prefix for topic names")

	flag.Parse()

	return config
}

func parseDateRange(startStr, endStr string) (time.Time, time.Time, error) {
	startDate, err := time.Parse("2006-01-02", startStr)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid start date format (use YYYY-MM-DD): %w", err)
	}

	endDate, err := time.Parse("2006-01-02", endStr)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid end date format (use YYYY-MM-DD): %w", err)
	}

	if endDate.Before(startDate) {
		return time.Time{}, time.Time{}, fmt.Errorf("end date cannot be before start date")
	}

	return startDate, endDate, nil
}

func setupKafka(brokers string) (sarama.Client, sarama.ClusterAdmin, sarama.SyncProducer, error) {
	// Create Sarama configuration
	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0 // Use appropriate version for your Kafka cluster
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	// Split brokers string
	brokerList := strings.Split(brokers, ",")

	// Create client
	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error creating client: %w", err)
	}

	// Create admin client
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
		return nil, nil, nil, fmt.Errorf("error creating admin: %w", err)
	}

	// Create producer
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		admin.Close()
		client.Close()
		return nil, nil, nil, fmt.Errorf("error creating producer: %w", err)
	}

	return client, admin, producer, nil
}

func generateMessages(ctx context.Context, admin sarama.ClusterAdmin, producer sarama.SyncProducer, config *Config, startDate, endDate time.Time) error {
	log.Printf("Generating messages for %d topics from %s to %s...",
		config.NumTopics, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

	// Create and send messages for each topic
	for i := 0; i < config.NumTopics; i++ {
		// Check if context was cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		topicName := fmt.Sprintf("%s%d", config.TopicPrefix, i+1)

		// Random number of partitions
		numPartitions := rand.Intn(config.MaxPartitions) + 1

		// Create the topic
		if err := createTopic(admin, topicName, numPartitions); err != nil {
			GetLogger().Infof("Warning: %v", err)
		}

		// Random number of messages to generate
		numMessages := rand.Intn(config.MaxMessages + 1)

		log.Printf("Generating %d messages for topic %s with %d partitions",
			numMessages, topicName, numPartitions)

		// Generate and send messages
		for j := 0; j < numMessages; j++ {
			// Calculate a random time between start and end dates
			messageTime := randomTimeBetween(startDate, endDate)

			// Create message
			message := Message{
				ID:        fmt.Sprintf("%s-msg-%d", topicName, j),
				Timestamp: messageTime,
				Value:     rand.Intn(1000),
			}

			// Serialize message to JSON
			msgBytes, err := json.Marshal(message)
			if err != nil {
				return fmt.Errorf("error serializing message: %w", err)
			}

			// Create and send Kafka message
			msg := &sarama.ProducerMessage{
				Topic:     topicName,
				Key:       sarama.StringEncoder(message.ID),
				Value:     sarama.ByteEncoder(msgBytes),
				Timestamp: messageTime,
			}

			_, _, err = producer.SendMessage(msg)
			if err != nil {
				return fmt.Errorf("error sending message to topic %s: %w", topicName, err)
			}
		}

		GetLogger().Infof("Completed sending %d messages to topic %s", numMessages, topicName)
	}

	return nil
}

func createTopic(admin sarama.ClusterAdmin, topic string, partitions int) error {
	// Check if topic already exists
	topics, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("error listing topics: %w", err)
	}

	if _, exists := topics[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}

	// Create topic configuration
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: 1, // Use higher value in production
		ConfigEntries:     make(map[string]*string),
	}

	// Create the topic
	if err := admin.CreateTopic(topic, topicDetail, false); err != nil {
		return fmt.Errorf("error creating topic %s: %w", topic, err)
	}

	GetLogger().Infof("Created topic %s with %d partitions", topic, partitions)
	return nil
}

func randomTimeBetween(start, end time.Time) time.Time {
	delta := end.Sub(start)
	randomDelta := time.Duration(rand.Int63n(int64(delta)))
	return start.Add(randomDelta)
}

// ProcessTopicsSelectively simulates reading topics by just committing offsets
// based on the topic's index remainder when divided by 3
func ProcessTopicsSelectively(client sarama.Client, admin sarama.ClusterAdmin, consumerGroupID string, topicPrefix string) error {
	// List all topics
	topicsMap, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	// Filter topics by prefix
	var topics []string
	for topic := range topicsMap {
		if strings.HasPrefix(topic, topicPrefix) {
			topics = append(topics, topic)
		}
	}

	GetLogger().Infof("Found %d topics with prefix %s", len(topics), topicPrefix)
	// Create offset manager
	offsetManager, err := sarama.NewOffsetManagerFromClient(consumerGroupID, client)
	if err != nil {
		return fmt.Errorf("failed to create offset manager: %w", err)
	}
	defer func() {
		if err := offsetManager.Close(); err != nil {
			GetLogger().Errorf("Failed to close offset manager: %v", err)
		}
	}()

	// Process each topic based on its index
	for i, topic := range topics {
		remainder := i % 3

		switch remainder {
		case 0:
			// Fully commit offsets (simulating full read)
			GetLogger().Infof("Topic %s (index %d): Committing full offsets", topic, i)
			if err := commitFullOffsets(client, offsetManager, topic); err != nil {
				GetLogger().Infof("Error committing offsets for topic %s: %v", topic, err)
			}

		case 1:
			// Partially commit offsets (simulating partial read)
			GetLogger().Infof("Topic %s (index %d): Committing partial offsets", topic, i)
			if err := commitPartialOffsets(client, offsetManager, topic); err != nil {
				GetLogger().Infof("Error committing offsets for topic %s: %v", topic, err)
			}

		case 2:
			// Skip
			GetLogger().Infof("Topic %s (index %d): Skipping", topic, i)
		}
	}

	return nil
}

// commitFullOffsets sets offsets to the latest position for a topic's partitions
func commitFullOffsets(client sarama.Client, offsetManager sarama.OffsetManager, topic string) error {
	// Get all partitions for the topic
	partitions, err := client.Partitions(topic)
	if err != nil {
		return fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
	}

	if len(partitions) == 0 {
		return fmt.Errorf("no partitions found for topic %s", topic)
	}

	for _, partition := range partitions {
		// Get the latest offset
		newestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			GetLogger().Warnf("Failed to get newest offset for %s partition %d: %v", topic, partition, err)
			continue
		}

		// Skip if no messages in partition
		if newestOffset <= 0 {
			GetLogger().Infof("No messages in %s partition %d, skipping", topic, partition)
			continue
		}

		// Create partition offset manager
		partitionManager, err := offsetManager.ManagePartition(topic, partition)
		if err != nil {
			GetLogger().Warnf("Failed to create partition manager for %s partition %d: %v", topic, partition, err)
			continue
		}

		// Mark offset
		GetLogger().Infof("Committing full offset %d for %s partition %d", newestOffset, topic, partition)
		partitionManager.MarkOffset(newestOffset, "")

		// Close partition manager to commit offset
		if err := partitionManager.Close(); err != nil {
			GetLogger().Warnf("Failed to close partition manager for %s partition %d: %v", topic, partition, err)
		}
	}

	return nil
}

// commitPartialOffsets sets offsets to the midpoint for a topic's partitions
func commitPartialOffsets(client sarama.Client, offsetManager sarama.OffsetManager, topic string) error {
	// Get all partitions for the topic
	partitions, err := client.Partitions(topic)
	if err != nil {
		return fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
	}

	if len(partitions) == 0 {
		return fmt.Errorf("no partitions found for topic %s", topic)
	}

	for _, partition := range partitions {
		// Get oldest and newest offsets
		oldestOffset, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			GetLogger().Warnf("Failed to get oldest offset for %s partition %d: %v", topic, partition, err)
			continue
		}

		newestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			GetLogger().Warnf("Failed to get newest offset for %s partition %d: %v", topic, partition, err)
			continue
		}

		// Skip if no messages in partition
		if newestOffset <= oldestOffset {
			GetLogger().Infof("No messages in %s partition %d, skipping", topic, partition)
			continue
		}

		// Calculate midpoint
		midOffset := oldestOffset + (newestOffset-oldestOffset)/2

		// Create partition offset manager
		partitionManager, err := offsetManager.ManagePartition(topic, partition)
		if err != nil {
			GetLogger().Warnf("Failed to create partition manager for %s partition %d: %v", topic, partition, err)
			continue
		}

		// Mark offset
		GetLogger().Infof("Committing partial offset %d for %s partition %d", midOffset, topic, partition)
		partitionManager.MarkOffset(midOffset, "")

		// Close partition manager to commit offset
		if err := partitionManager.Close(); err != nil {
			GetLogger().Warnf("Failed to close partition manager for %s partition %d: %v", topic, partition, err)
		}
	}

	return nil
}
