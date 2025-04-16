package report

import "time"

// TopicActivityInfo contains information about the last read and write operations for a topic.
type TopicActivityInfo struct {
	TopicName       string    // Name of the topic.
	LastWriteTime   time.Time // Time when last message was written to any partition.
	LastReadTime    time.Time // Time when message was consumed by any consumer group.
	PartitionNumber int       // Number of partitions in topic.
	Active          bool      // Indicates if the topic is active (has recent activity).
}
