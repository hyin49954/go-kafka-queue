# Go Kafka Queue

[![Go Version](https://img.shields.io/badge/Go-%3E%3D1.24-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/hyin49954/go-kafka-queue)](https://goreportcard.com/report/github.com/hyin49954/go-kafka-queue)

一个高性能、生产级别的 Go 语言 Kafka 客户端库，提供简洁易用的 API，支持批量发送、协程池并发处理、自动重试等企业级特性。

## ✨ 特性

- 🚀 **高性能批量发送**：通过 channel 控制批数量，自动批量发送消息，减少网络往返
- 🔄 **协程池并发处理**：消费者使用协程池处理消息，提高并发处理能力
- 🛡️ **企业级可靠性**：支持自动重试、优雅关闭、连接池管理
- 📦 **Topic 自动管理**：自动检查并创建 topic，支持自定义分区数和副本因子
- 🔐 **SASL 认证支持**：支持 SASL_PLAINTEXT 和 SASL_SSL 认证
- 🎯 **灵活的分区策略**：支持 key-based 分区和轮询分区
- 📊 **可观测性**：内置日志和错误处理，便于监控和调试
- 🧩 **模块化设计**：清晰的代码结构，易于扩展和维护

## 📦 安装

```bash
go get github.com/hyin49954/go-kafka-queue
```

## 🚀 快速开始

### 生产者示例

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/hyin49954/go-kafka-queue/producer"
)

func main() {
	// 创建生产者
	p, err := producer.NewProducer([]string{"localhost:9092"})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// 确保 topic 存在（3 个分区，1 个副本）
	if err := p.EnsureTopic("my-topic", 3, 1); err != nil {
		log.Printf("警告: %v", err)
	}

	// 启动生产者（批量大小：10，超时：5秒）
	if err := p.Start(10, 5*time.Second); err != nil {
		log.Fatal(err)
	}

	// 发送消息
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("message-%d", i)
		if err := p.SendMessage("my-topic", key, value); err != nil {
			log.Printf("发送失败: %v", err)
		}
	}

	// 等待所有消息发送完成
	remaining := p.Flush(5000)
	log.Printf("剩余未发送消息: %d", remaining)
}
```

### 消费者示例

```go
package main

import (
	"log"

	"github.com/hyin49954/go-kafka-queue/consumer"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// 自定义消息处理器
type MyHandler struct{}

func (h *MyHandler) Handle(msg *kafka.Message) error {
	log.Printf("收到消息: %s", string(msg.Value))
	// 处理业务逻辑
	return nil
}

func main() {
	// 创建消费者管理器
	manager := consumer.NewConsumerManager()

	// 创建消费者
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

	// 添加到管理器
	if err := manager.AddConsumer(c); err != nil {
		log.Fatal(err)
	}

	// 启动所有消费者（协程池大小：10，队列大小：100）
	if err := manager.StartAll(10, 100); err != nil {
		log.Fatal(err)
	}

	// 等待中断信号
	select {}
}
```

## 📚 API 文档

### Producer

#### NewProducer

创建新的 Kafka 生产者客户端实例。

```go
func NewProducer(brokers []string) (*Producer, error)
```

**参数：**
- `brokers`: Kafka broker 地址列表

**返回：**
- `*Producer`: 生产者实例
- `error`: 创建失败时返回错误

#### NewProducerWithConfig

使用配置创建新的 Kafka 生产者客户端实例。

```go
func NewProducerWithConfig(config *Config) (*Producer, error)
```

#### EnsureTopic

检查 topic 是否存在，如果不存在则创建。

```go
func (p *Producer) EnsureTopic(topic string, numPartitions, replicationFactor int) error
```

**参数：**
- `topic`: 主题名称
- `numPartitions`: 分区数量（默认 3）
- `replicationFactor`: 副本因子（默认 1）

#### Start

启动生产者，开始处理事件和批量发送。

```go
func (p *Producer) Start(batchSize int, batchTimeout time.Duration) error
```

**参数：**
- `batchSize`: 批量发送的消息数量，达到这个数量会自动发送
- `batchTimeout`: 批量发送的超时时间，即使未达到批数量，超过这个时间也会自动发送

#### SendMessage

发送消息到指定主题（异步，消息会被批量发送）。

```go
func (p *Producer) SendMessage(topic, key, value string) error
```

**注意：**
- 如果 `key` 为空，Kafka 会使用轮询方式分配分区，消息会均匀分布到各个分区
- 如果设置了 `key`，Kafka 会根据 key 的 hash 值分配到特定分区

#### Flush

刷新所有待发送的消息。

```go
func (p *Producer) Flush(timeoutMs int) int
```

#### Close

关闭生产者，等待所有消息发送完成。

```go
func (p *Producer) Close()
```

### Consumer

#### NewConsumer

创建新的 Kafka 消费者（单个 topic）。

```go
func NewConsumer(brokers []string, groupID, topic string, handler MessageHandler) (*Consumer, error)
```

**参数：**
- `brokers`: Kafka broker 地址列表
- `groupID`: 消费者组 ID
- `topic`: 要订阅的主题名称
- `handler`: 消息处理器，实现 `MessageHandler` 接口

#### NewConsumerWithConfig

使用配置创建新的 Kafka 消费者。

```go
func NewConsumerWithConfig(config *Config, handler MessageHandler) (*Consumer, error)
```

#### MessageHandler 接口

```go
type MessageHandler interface {
    Handle(message *kafka.Message) error
}
```

#### Start

启动消费者，订阅指定的 topic 并开始消费消息。

```go
func (c *Consumer) Start(poolSize, queueSize int) error
```

**参数：**
- `poolSize`: 协程池大小，控制并发处理消息的协程数量
- `queueSize`: 任务队列大小，控制可以排队等待处理的消息数量

#### Stop

停止消费者，优雅关闭。

```go
func (c *Consumer) Stop() error
```

#### ConsumerManager

消费者管理器，用于统一管理多个消费者。

```go
manager := consumer.NewConsumerManager()
manager.AddConsumer(consumer)
manager.StartAll(poolSize, queueSize)
manager.StopAll()
```

## ⚙️ 配置

### 生产者配置

```go
config := producer.DefaultConfig([]string{"localhost:9092"})
config.ClientID = "my-producer"
config.Producer.Acks = "all"
config.Producer.Retries = 3
config.Batch.Size = 100
config.Batch.Timeout = 5 * time.Second

// SASL 配置
config.SASL.SecurityProtocol = "SASL_PLAINTEXT"
config.SASL.Mechanism = "PLAIN"
config.SASL.Username = "kafka"
config.SASL.Password = "kafka123"

p, err := producer.NewProducerWithConfig(config)
```

### 消费者配置

```go
config := consumer.DefaultConfig(
	[]string{"localhost:9092"},
	"my-group",
	"my-topic",
)
config.Consumer.AutoOffsetReset = "earliest"
config.Pool.Size = 20
config.Pool.QueueSize = 200

// SASL 配置
config.SASL.SecurityProtocol = "SASL_PLAINTEXT"
config.SASL.Mechanism = "PLAIN"
config.SASL.Username = "kafka"
config.SASL.Password = "kafka123"

c, err := consumer.NewConsumerWithConfig(config, handler)
```

### SASL 认证

当前默认配置使用 SASL_PLAINTEXT 认证。可以通过配置修改认证方式。

### 批量发送配置

- `batchSize`: 批量发送的消息数量，达到这个数量会自动发送
- `batchTimeout`: 批量发送的超时时间，即使未达到批数量，超过这个时间也会自动发送

### 协程池配置

- `poolSize`: 协程池大小，控制并发处理消息的协程数量
- `queueSize`: 任务队列大小，控制可以排队等待处理的消息数量

## 🏗️ 架构设计

### 生产者架构

```
Producer
├── 批量发送机制
│   ├── Channel 缓冲队列
│   ├── 批量大小触发
│   └── 超时触发
├── 事件处理
│   └── 协程池处理发送结果
└── Topic 管理
    └── 自动检查并创建
```

### 消费者架构

```
ConsumerManager
├── Consumer (每个 Topic 一个)
│   ├── 消息接收
│   ├── 协程池处理
│   └── 消息处理器接口
└── 统一管理
    ├── 批量启动
    └── 批量停止
```

## 🧪 测试

```bash
# 运行所有测试
go test ./...

# 运行特定包的测试
go test ./producer
go test ./consumer

# 运行测试并显示覆盖率
go test -cover ./...

# 运行基准测试
go test -bench=. ./...
```

## 📝 最佳实践

1. **Topic 管理**：在生产环境中，建议禁用 Kafka 的自动创建 topic 功能（`auto.create.topics.enable=false`），通过代码统一管理
2. **分区策略**：根据业务需求选择合适的分区策略（key-based 或轮询）
3. **错误处理**：实现完善的错误处理和重试机制
4. **监控告警**：监控消息发送/消费速率、错误率等指标
5. **优雅关闭**：确保在应用关闭时，所有消息都已发送/处理完成
6. **批量大小调优**：根据消息大小和网络延迟调整批量大小和超时时间
7. **协程池大小**：根据 CPU 核心数和业务处理时间调整协程池大小

## 📊 性能优化

1. **批量发送**：减少网络往返，提高吞吐量
2. **协程池**：控制并发数量，避免资源耗尽
3. **Channel 缓冲**：减少阻塞，提高响应速度
4. **连接复用**：保持长连接，减少连接开销

## 🛠️ 开发

### 运行测试

```bash
# 运行所有测试
go test ./...

# 运行测试并显示覆盖率
go test -cover ./...

# 运行基准测试
go test -bench=. ./...
```

### 代码规范

- 遵循 Go 官方代码规范
- 使用 `gofmt` 格式化代码
- 使用 `golint` 检查代码质量

## 📝 更新日志

### v1.0.0
- ✅ 支持批量发送消息
- ✅ 支持协程池并发处理
- ✅ 支持 Topic 自动管理
- ✅ 支持 SASL 认证
- ✅ 完整的错误处理和日志
- ✅ 配置管理模块
- ✅ 单元测试

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

本项目采用 MIT 许可证。详情请参阅 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) - Kafka Go 客户端
- [gopoolx](https://github.com/hyin49954/gopoolx) - 协程池实现

## 📧 联系方式

如有问题或建议，请通过 Issue 联系我们。

## ⭐ Star History

如果这个项目对你有帮助，欢迎 Star！

---

**English Version**: [README_EN.md](README_EN.md)
