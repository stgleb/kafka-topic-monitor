package monitor

import (
	"context"
	"fmt"
	"kafka-topic-monitor/pkg/monitor/report"
	"time"

	"github.com/IBM/sarama"
)

type KafkaTopicChecker struct{}

var (
	_ TopicChecker = &KafkaTopicChecker{}
)

func NewTopicChecker() TopicChecker {
	return &KafkaTopicChecker{}
}

// CheckTopic examines a Kafka topic to determine when and where the last write and read operations occurred
// Parameters:
// - ctx: Context for timeout/cancellation
// - topicName: The name of the Kafka topic to check
// - kafkaClient: A sarama Kafka client
// - kafkaAdminClient: A sarama Kafka admin client
func (c *KafkaTopicChecker) CheckTopic(ctx context.Context, topicName string, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin) (*report.TopicActivityInfo, error) {
	topicActivityInfo := &report.TopicActivityInfo{}
	// Get topic partitions
	partitions, err := kafkaClient.Partitions(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions for topic %s: %w", topicName, err)
	}

	topicActivityInfo.LastWriteTime, err = getLastWrite(kafkaClient, topicName, partitions)
	if err != nil {
		return nil, fmt.Errorf("error getting last read of topic %s: %v", topicName, err)
	}

	topicActivityInfo.LastReadTime, err = getLastRead(kafkaAdminClient, topicName, partitions)
	if err != nil {
		return nil, fmt.Errorf("error getting last read of topic %s: %v", topicName, err)
	}

	return topicActivityInfo, nil
}

func getLastWrite(kafkaClient sarama.Client, topicName string, partitions []int32) (time.Time, error) {
	var lastWriteTime time.Time

	// Find the latest message offsets (last write) among partitions.
	for _, partition := range partitions {
		oldestOffset, err := kafkaClient.GetOffset(topicName, partition, sarama.OffsetOldest)
		newestOffset, err := kafkaClient.GetOffset(topicName, partition, sarama.OffsetNewest)

		// Check if there are any messages
		if newestOffset <= oldestOffset {
			return time.Time{}, fmt.Errorf("no messages in partition %d", partition)
		}

		// Create a consumer
		consumer, err := sarama.NewConsumerFromClient(kafkaClient)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to create consumer for partition %d: %w", partition, err)
		}
		defer consumer.Close()

		// Create a partition consumer
		partitionConsumer, err := consumer.ConsumePartition(topicName, partition, sarama.OffsetNewest-1)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to consume from partition %d: %w", partition, err)
		}
		defer partitionConsumer.Close()

		// Get the first message
		message := <-partitionConsumer.Messages()
		if message.Timestamp.Unix() > lastWriteTime.Unix() {
			lastWriteTime = message.Timestamp
		}
	}
	return lastWriteTime, nil
}

func getLastRead(kafkaAdminClient sarama.ClusterAdmin, topicName string, partitions []int32) (time.Time, error) {
	listGroupsResponse, err := kafkaAdminClient.ListConsumerGroups()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	var lastReadTime time.Time
	// Get consumer group offsets for each group
	for groupID := range listGroupsResponse {
		// Get consumer group offsets for our topic and partitions
		listRequest := make(map[string][]int32)
		listRequest[topicName] = partitions

		offsetResponse, err := kafkaAdminClient.ListConsumerGroupOffsets(groupID, listRequest)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to list offsets for group %s: %w", groupID, err)
		}

		if topicBlocks, exists := offsetResponse.Blocks[topicName]; exists {
			for _, block := range topicBlocks {
				// Skip if there's no committed offset
				if block.Offset < 0 {
					continue
				}

				offsetMeta := block.Metadata

				// Check if we have the Sarama offset manager metadata
				// In Sarama's default implementation, the metadata contains timestamp info
				if offsetMeta != "" {
					parsed, err := parseOffsetMetadata(offsetMeta)
					if err == nil && !parsed.IsZero() {
						if parsed.Unix() > lastReadTime.Unix() {
							lastReadTime = parsed
						}
					}
				}
			}
		}
	}
	return lastReadTime, nil
}

// parseOffsetMetadata attempts to extract timestamp from metadata string
// This is client-specific and depends on how offsets were committed.
func parseOffsetMetadata(metadata string) (time.Time, error) {
	var timestamp time.Time

	if len(metadata) > 0 {
		// Attempt to parse ISO8601 format (example)
		t, err := time.Parse(time.RFC3339, metadata)
		if err == nil {
			return t, nil
		}
	}

	return timestamp, fmt.Errorf("couldn't parse timestamp from metadata: %s", metadata)
}
