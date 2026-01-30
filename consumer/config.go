package consumer

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Config 消费者配置
type Config struct {
	// Brokers Kafka broker 地址列表
	Brokers []string

	// GroupID 消费者组 ID
	GroupID string

	// Topic 主题名称
	Topic string

	// ClientID 客户端 ID（如果为空，会自动生成）
	ClientID string

	// SASL 配置
	SASL SASLConfig

	// Consumer 配置
	Consumer ConsumerConfig

	// Pool 配置
	Pool PoolConfig
}

// SASLConfig SASL 认证配置
type SASLConfig struct {
	// SecurityProtocol 安全协议 (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
	SecurityProtocol string

	// Mechanism SASL 机制 (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
	Mechanism string

	// Username 用户名
	Username string

	// Password 密码
	Password string
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	// AutoOffsetReset 偏移量重置策略 (earliest, latest, none)
	AutoOffsetReset string

	// EnableAutoCommit 是否自动提交偏移量
	EnableAutoCommit bool

	// AutoCommitIntervalMs 自动提交间隔（毫秒）
	AutoCommitIntervalMs int

	// SessionTimeoutMs 会话超时（毫秒）
	SessionTimeoutMs int

	// MaxPollIntervalMs 最大轮询间隔（毫秒）
	MaxPollIntervalMs int
}

// PoolConfig 协程池配置
type PoolConfig struct {
	// Size 协程池大小
	Size int

	// QueueSize 队列大小
	QueueSize int

	// Retry 重试次数
	Retry int

	// RetryDelay 重试延迟
	RetryDelay int // 毫秒
}

// DefaultConfig 返回默认配置
func DefaultConfig(brokers []string, groupID, topic string) *Config {
	clientID := fmt.Sprintf("go-kafka-consumer-%s", topic)
	if clientID == "" {
		clientID = "go-kafka-consumer"
	}

	return &Config{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		ClientID: clientID,
		SASL: SASLConfig{
			SecurityProtocol: "SASL_PLAINTEXT",
			Mechanism:        "PLAIN",
			Username:         "kafka",
			Password:         "kafka123",
		},
		Consumer: ConsumerConfig{
			AutoOffsetReset:      "latest",
			EnableAutoCommit:     true,
			AutoCommitIntervalMs: 5000,
			SessionTimeoutMs:     30000,
			MaxPollIntervalMs:    300000,
		},
		Pool: PoolConfig{
			Size:      10,
			QueueSize: 100,
			Retry:     2,
			RetryDelay: 200,
		},
	}
}

// ToKafkaConfigMap 转换为 kafka.ConfigMap
func (c *Config) ToKafkaConfigMap() kafka.ConfigMap {
	config := kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(c.Brokers, ","),
		"group.id":           c.GroupID,
		"auto.offset.reset":  c.Consumer.AutoOffsetReset,
		"enable.auto.commit": c.Consumer.EnableAutoCommit,
		"client.id":          c.ClientID,
	}

	if c.Consumer.AutoCommitIntervalMs > 0 {
		config["auto.commit.interval.ms"] = c.Consumer.AutoCommitIntervalMs
	}
	if c.Consumer.SessionTimeoutMs > 0 {
		config["session.timeout.ms"] = c.Consumer.SessionTimeoutMs
	}
	if c.Consumer.MaxPollIntervalMs > 0 {
		config["max.poll.interval.ms"] = c.Consumer.MaxPollIntervalMs
	}

	// 添加 SASL 配置（如果启用）
	if c.SASL.SecurityProtocol != "" && c.SASL.SecurityProtocol != "PLAINTEXT" {
		config["security.protocol"] = c.SASL.SecurityProtocol
		if c.SASL.Mechanism != "" {
			config["sasl.mechanism"] = c.SASL.Mechanism
		}
		if c.SASL.Username != "" {
			config["sasl.username"] = c.SASL.Username
		}
		if c.SASL.Password != "" {
			config["sasl.password"] = c.SASL.Password
		}
	}

	return config
}
