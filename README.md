# Kafka Topic Activity Monitor

A service that monitors the activity of Kafka topics by tracking their last write and read operations.

## Overview

This service connects to a Kafka cluster and monitors topic activity, providing a REST API to check which topics are actively being used and which ones have become inactive. It's useful for identifying abandoned topics that could be cleaned up or archived.

## Features

- Monitors both write (producer) and read (consumer) operations for each topic
- Tracks the last activity timestamps across all partitions
- Configurable inactivity threshold
- HTTP API to query topic activity status
- Support for Kafka clusters with or without ZooKeeper (KRaft mode)

## Quick Start

### Prerequisites

- Go 1.16+
- Docker and Docker Compose

### Running the Kafka Cluster

Start a local Kafka cluster using Docker Compose:

```bash
docker-compose up kafka -d
```

This will start a Kafka instance in KRaft mode (without ZooKeeper) listening on port 9092.

Run both kafka and server:

```bash
docker-compose up -d
```

### Running the Monitor Service

Run the service with:

```bash
go run cmd/monitor/main.go
```

By default, the service will:
- Connect to Kafka at `localhost:9092`
- Consider topics inactive after 7 days of no activity
- Listen for HTTP requests on port 8080

### Generating Test Data

For testing purposes, you can generate Kafka topics and messages with specific timestamps:

```bash
go run cmd/gendata/main.go
```

Available parameters:

```
--brokers string       Kafka brokers (comma-separated) (default "localhost:9092")
--topics int           Number of topics to create (default 3)
--partitions int       Maximum number of partitions per topic (random 1 to this value) (default 3)
--start-date string    Start date for message timestamps (YYYY-MM-DD) (default "2024-03-15")
--end-date string      End date for message timestamps (YYYY-MM-DD) (default "2024-04-15")
--messages int         Maximum number of messages per topic (default 100)
--prefix string        Prefix for topic names (default "test-topic-")
```

Example with custom values:

```bash
go run cmd/gendata/main.go --brokers="localhost:9092" --topics=10 --partitions=5 --messages=500 --start-date="2024-01-01" --end-date="2024-03-31" --prefix="monitor-test-"
```

This will:
1. Create 10 topics with random partition counts (1-5 partitions each)
2. Generate up to 500 messages per topic
3. Set message timestamps between Jan 1, 2024 and Mar 31, 2024

### API Endpoints

Get all topic activity information:

```bash
curl http://localhost:8080/topics
```

The response will be a JSON array of topics with their activity status, last write time, and last read time.

## Configuration

The service can be configured through:

1. Command-line flags
2. Environment variables
3. Configuration file

### Command-line Options

```
Usage: kafka-monitor [options]

Options:
  --bootstrap-servers string   Kafka bootstrap servers (default "localhost:9092")
  --config string              Path to configuration file
  --inactivity-days int        Number of days to consider a topic inactive (default 7)
  --port int                   HTTP server port (default 8080)
  --verbose                    Enable verbose logging
```

### Environment Variables

- `BOOTSTRAP_SERVERS`: Comma-separated list of Kafka brokers
- `INACTIVITY_DAYS`: Number of days to consider a topic inactive
- `PORT`: HTTP server port

### Configuration File

The service supports YAML configuration:

```yaml
bootstrapServers:
  - localhost:9092
inactivityDays: 7
```

## Development

### Building

```bash
go build -o kafka-monitor cmd/monitor/main.go
```

### Testing

Run all tests with:

```bash
go test ./...
```

## License

[MIT License](LICENSE)