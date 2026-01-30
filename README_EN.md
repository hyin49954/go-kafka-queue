# Go Kafka Queue

[![Go Version](https://img.shields.io/badge/Go-%3E%3D1.24-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/hyin49954/go-kafka-queue)](https://goreportcard.com/report/github.com/hyin49954/go-kafka-queue)

A high-performance, production-ready Go Kafka client library with a clean and easy-to-use API, supporting batch sending, goroutine pool concurrent processing, automatic retry, and other enterprise-level features.

## ✨ Features

- 🚀 **High-Performance Batch Sending**: Automatic batch sending controlled by channels, reducing network round trips
- 🔄 **Goroutine Pool Concurrent Processing**: Consumers use goroutine pools to process messages, improving concurrent processing capabilities
- 🛡️ **Enterprise-Grade Reliability**: Supports automatic retry, graceful shutdown, and connection pool management
- 📦 **Automatic Topic Management**: Automatically checks and creates topics with customizable partition count and replication factor
- 🔐 **SASL Authentication Support**: Supports SASL_PLAINTEXT and SASL_SSL authentication
- 🎯 **Flexible Partitioning Strategy**: Supports key-based partitioning and round-robin partitioning
- 📊 **Observability**: Built-in logging and error handling for easy monitoring and debugging
- 🧩 **Modular Design**: Clear code structure, easy to extend and maintain

## 📦 Installation

```bash
go get github.com/hyin49954/go-kafka-queue
```

## 🚀 Quick Start

### Producer Example

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/hyin49954/go-kafka-queue/producer"
)

func main() {
	// Create producer
	p, err := producer.NewProducer([]string{"localhost:9092"})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Ensure topic exists (3 partitions, 1 replica)
	if err := p.EnsureTopic("my-topic", 3, 1); err != nil {
		log.Printf("Warning: %v", err)
	}

	// Start producer (batch size: 10, timeout: 5 seconds)
	if err := p.Start(10, 5*time.Second); err != nil {
		log.Fatal(err)
	}

	// Send messages
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("message-%d", i)
		if err := p.SendMessage("my-topic", key, value); err != nil {
			log.Printf("Send failed: %v", err)
		}
	}

	// Wait for all messages to be sent
	remaining := p.Flush(5000)
	log.Printf("Remaining unsent messages: %d", remaining)
}
```

### Consumer Example

```go
package main

import (
	"log"

	"github.com/hyin49954/go-kafka-queue/consumer"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Custom message handler
type MyHandler struct{}

func (h *MyHandler) Handle(msg *kafka.Message) error {
	log.Printf("Received message: %s", string(msg.Value))
	// Process business logic
	return nil
}

func main() {
	// Create consumer manager
	manager := consumer.NewConsumerManager()

	// Create consumer
	handler := &MyHandler{}
	c, err := consumer.NewConsumer(
		[]string{"localhost:9092"},
		"my-group",
		"my-topic",
		handler,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Add to manager
	if err := manager.AddConsumer(c); err != nil {
		log.Fatal(err)
	}

	// Start all consumers (pool size: 10, queue size: 100)
	if err := manager.StartAll(10, 100); err != nil {
		log.Fatal(err)
	}

	// Wait for interrupt signal
	select {}
}
```

## 📚 API Documentation

### Producer

#### NewProducer

Creates a new Kafka producer client instance.

```go
func NewProducer(brokers []string) (*Producer, error)
```

**Parameters:**
- `brokers`: List of Kafka broker addresses

**Returns:**
- `*Producer`: Producer instance
- `error`: Error if creation fails

#### NewProducerWithConfig

Creates a new Kafka producer client instance with configuration.

```go
func NewProducerWithConfig(config *Config) (*Producer, error)
```

#### EnsureTopic

Checks if a topic exists, creates it if it doesn't.

```go
func (p *Producer) EnsureTopic(topic string, numPartitions, replicationFactor int) error
```

**Parameters:**
- `topic`: Topic name
- `numPartitions`: Number of partitions (default: 3)
- `replicationFactor`: Replication factor (default: 1)

#### Start

Starts the producer, begins processing events and batch sending.

```go
func (p *Producer) Start(batchSize int, batchTimeout time.Duration) error
```

**Parameters:**
- `batchSize`: Number of messages in a batch, automatically sends when reached
- `batchTimeout`: Batch timeout, automatically sends even if batch size is not reached

#### SendMessage

Sends a message to the specified topic (asynchronous, messages will be batched).

```go
func (p *Producer) SendMessage(topic, key, value string) error
```

**Note:**
- If `key` is empty, Kafka uses round-robin partitioning, messages are evenly distributed across partitions
- If `key` is set, Kafka assigns to a specific partition based on key hash

#### Flush

Flushes all pending messages.

```go
func (p *Producer) Flush(timeoutMs int) int
```

#### Close

Closes the producer, waits for all messages to be sent.

```go
func (p *Producer) Close()
```

### Consumer

#### NewConsumer

Creates a new Kafka consumer (single topic).

```go
func NewConsumer(brokers []string, groupID, topic string, handler MessageHandler) (*Consumer, error)
```

**Parameters:**
- `brokers`: List of Kafka broker addresses
- `groupID`: Consumer group ID
- `topic`: Topic name to subscribe to
- `handler`: Message handler implementing the `MessageHandler` interface

#### NewConsumerWithConfig

Creates a new Kafka consumer with configuration.

```go
func NewConsumerWithConfig(config *Config, handler MessageHandler) (*Consumer, error)
```

#### MessageHandler Interface

```go
type MessageHandler interface {
    Handle(message *kafka.Message) error
}
```

#### Start

Starts the consumer, subscribes to the specified topic and begins consuming messages.

```go
func (c *Consumer) Start(poolSize, queueSize int) error
```

**Parameters:**
- `poolSize`: Goroutine pool size, controls the number of concurrent goroutines processing messages
- `queueSize`: Task queue size, controls the number of messages that can be queued for processing

#### Stop

Stops the consumer, graceful shutdown.

```go
func (c *Consumer) Stop() error
```

#### ConsumerManager

Consumer manager for unified management of multiple consumers.

```go
manager := consumer.NewConsumerManager()
manager.AddConsumer(consumer)
manager.StartAll(poolSize, queueSize)
manager.StopAll()
```

## ⚙️ Configuration

### Producer Configuration

```go
config := producer.DefaultConfig([]string{"localhost:9092"})
config.ClientID = "my-producer"
config.Producer.Acks = "all"
config.Producer.Retries = 3
config.Batch.Size = 100
config.Batch.Timeout = 5 * time.Second

// SASL configuration
config.SASL.SecurityProtocol = "SASL_PLAINTEXT"
config.SASL.Mechanism = "PLAIN"
config.SASL.Username = "kafka"
config.SASL.Password = "kafka123"

p, err := producer.NewProducerWithConfig(config)
```

### Consumer Configuration

```go
config := consumer.DefaultConfig(
	[]string{"localhost:9092"},
	"my-group",
	"my-topic",
)
config.Consumer.AutoOffsetReset = "earliest"
config.Pool.Size = 20
config.Pool.QueueSize = 200

// SASL configuration
config.SASL.SecurityProtocol = "SASL_PLAINTEXT"
config.SASL.Mechanism = "PLAIN"
config.SASL.Username = "kafka"
config.SASL.Password = "kafka123"

c, err := consumer.NewConsumerWithConfig(config, handler)
```

### SASL Authentication

The default configuration uses SASL_PLAINTEXT authentication. You can modify the authentication method through configuration.

### Batch Sending Configuration

- `batchSize`: Number of messages in a batch, automatically sends when reached
- `batchTimeout`: Batch timeout, automatically sends even if batch size is not reached

### Goroutine Pool Configuration

- `poolSize`: Goroutine pool size, controls the number of concurrent goroutines processing messages
- `queueSize`: Task queue size, controls the number of messages that can be queued for processing

## 🏗️ Architecture

### Producer Architecture

```
Producer
├── Batch Sending Mechanism
│   ├── Channel Buffer Queue
│   ├── Batch Size Trigger
│   └── Timeout Trigger
├── Event Processing
│   └── Goroutine Pool Processing Results
└── Topic Management
    └── Automatic Check and Create
```

### Consumer Architecture

```
ConsumerManager
├── Consumer (One per Topic)
│   ├── Message Reception
│   ├── Goroutine Pool Processing
│   └── Message Handler Interface
└── Unified Management
    ├── Batch Start
    └── Batch Stop
```

## 🧪 Testing

```bash
# Run all tests
go test ./...

# Run tests for specific package
go test ./producer
go test ./consumer

# Run tests with coverage
go test -cover ./...

# Run benchmark tests
go test -bench=. ./...
```

## 📝 Best Practices

1. **Topic Management**: In production environments, it's recommended to disable Kafka's automatic topic creation (`auto.create.topics.enable=false`) and manage topics uniformly through code
2. **Partitioning Strategy**: Choose the appropriate partitioning strategy (key-based or round-robin) based on business requirements
3. **Error Handling**: Implement comprehensive error handling and retry mechanisms
4. **Monitoring and Alerting**: Monitor message send/consume rates, error rates, and other metrics
5. **Graceful Shutdown**: Ensure all messages are sent/processed when the application shuts down
6. **Batch Size Tuning**: Adjust batch size and timeout based on message size and network latency
7. **Goroutine Pool Size**: Adjust goroutine pool size based on CPU cores and business processing time

## 📊 Performance Optimization

1. **Batch Sending**: Reduces network round trips, improves throughput
2. **Goroutine Pool**: Controls concurrency, prevents resource exhaustion
3. **Channel Buffering**: Reduces blocking, improves response speed
4. **Connection Reuse**: Maintains long connections, reduces connection overhead

## 🛠️ Development

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run benchmark tests
go test -bench=. ./...
```

### Code Standards

- Follow Go official code standards
- Use `gofmt` to format code
- Use `golint` to check code quality

## 📝 Changelog

### v1.0.0
- ✅ Batch message sending support
- ✅ Goroutine pool concurrent processing
- ✅ Automatic topic management
- ✅ SASL authentication support
- ✅ Complete error handling and logging
- ✅ Configuration management module
- ✅ Unit tests

## 🤝 Contributing

Issues and Pull Requests are welcome!

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) - Kafka Go client
- [gopoolx](https://github.com/hyin49954/gopoolx) - Goroutine pool implementation

## 📧 Contact

If you have questions or suggestions, please contact us through Issues.

## ⭐ Star History

If this project helps you, please give it a Star!

---

**中文版本**: [README.md](README.md)
