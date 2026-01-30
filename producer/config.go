package producer

import (
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Config 生产者配置
type Config struct {
	// Brokers Kafka broker 地址列表
	Brokers []string

	// ClientID 客户端 ID
	ClientID string

	// SASL 配置
	SASL SASLConfig

	// Producer 配置
	Producer ProducerConfig

	// Batch 配置
	Batch BatchConfig
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

// ProducerConfig 生产者配置
type ProducerConfig struct {
	// Acks 确认模式 (0: 不等待, 1: 等待 leader, all: 等待所有副本)
	Acks string

	// Retries 重试次数
	Retries int

	// RetryBackoffMs 重试间隔（毫秒）
	RetryBackoffMs int

	// RequestTimeoutMs 请求超时（毫秒）
	RequestTimeoutMs int

	// DeliveryTimeoutMs 消息传递超时（毫秒）
	DeliveryTimeoutMs int

	// SocketKeepaliveEnable 启用 socket keepalive
	SocketKeepaliveEnable bool

	// ConnectionsMaxIdleMs 连接最大空闲时间（毫秒）
	ConnectionsMaxIdleMs int
}

// BatchConfig 批量发送配置
type BatchConfig struct {
	// Size 批量大小
	Size int

	// Timeout 批量超时时间
	Timeout time.Duration
}

// DefaultConfig 返回默认配置
func DefaultConfig(brokers []string) *Config {
	return &Config{
		Brokers:  brokers,
		ClientID: "go-kafka-producer",
		SASL: SASLConfig{
			SecurityProtocol: "SASL_PLAINTEXT",
			Mechanism:        "PLAIN",
			Username:         "kafka",
			Password:         "kafka123",
		},
		Producer: ProducerConfig{
			Acks:                   "all",
			Retries:                3,
			RetryBackoffMs:         100,
			RequestTimeoutMs:       30000,
			DeliveryTimeoutMs:      120000,
			SocketKeepaliveEnable:  true,
			ConnectionsMaxIdleMs:   600000,
		},
		Batch: BatchConfig{
			Size:    10,
			Timeout: 5 * time.Second,
		},
	}
}

// ToKafkaConfigMap 转换为 kafka.ConfigMap
func (c *Config) ToKafkaConfigMap() kafka.ConfigMap {
	config := kafka.ConfigMap{
		"bootstrap.servers":       strings.Join(c.Brokers, ","),
		"client.id":               c.ClientID,
		"acks":                    c.Producer.Acks,
		"retries":                 c.Producer.Retries,
		"retry.backoff.ms":        c.Producer.RetryBackoffMs,
		"request.timeout.ms":      c.Producer.RequestTimeoutMs,
		"delivery.timeout.ms":     c.Producer.DeliveryTimeoutMs,
		"socket.keepalive.enable": c.Producer.SocketKeepaliveEnable,
		"connections.max.idle.ms": c.Producer.ConnectionsMaxIdleMs,
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
