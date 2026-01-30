package producer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// EnsureTopic 检查 topic 是否存在，如果不存在则创建
// 参数:
//   - topic: 主题名称
//   - numPartitions: 分区数量（默认 3）
//   - replicationFactor: 副本因子（默认 1）
//
// 返回:
//   - error: 检查或创建失败时返回错误
func (p *Producer) EnsureTopic(topic string, numPartitions, replicationFactor int) error {
	if numPartitions <= 0 {
		numPartitions = 3 // 默认 3 个分区
	}
	if replicationFactor <= 0 {
		replicationFactor = 1 // 默认 1 个副本
	}

	// 创建 AdminClient 配置（使用与 Producer 相同的配置）
	config := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(p.brokers, ","),
		"client.id":         "go-kafka-admin",
		// SASL 认证配置
		"security.protocol": "SASL_PLAINTEXT",
		"sasl.mechanism":    "PLAIN",
		"sasl.username":     "kafka",
		"sasl.password":     "kafka123",
	}

	// 创建 AdminClient
	adminClient, err := kafka.NewAdminClient(&config)
	if err != nil {
		return fmt.Errorf("创建 AdminClient 失败: %w", err)
	}
	defer adminClient.Close()

	// 检查 topic 是否存在
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 获取 topic 元数据
	metadata, err := adminClient.GetMetadata(&topic, false, 5000)
	if err != nil {
		return fmt.Errorf("获取 topic 元数据失败: %w", err)
	}

	// 检查 topic 是否存在
	// metadata.Topics 是 map[string]TopicMetadata，TopicMetadata 是结构体值类型，不是指针
	// 所以不能与 nil 比较，应该检查 map 中是否存在该 key，或者检查 Error 字段
	topicMetadata, exists := metadata.Topics[topic]
	if exists && topicMetadata.Error.Code() == kafka.ErrNoError {
		// Topic 已存在，检查分区数
		actualPartitions := len(topicMetadata.Partitions)
		if actualPartitions == numPartitions {
			log.Printf("[Producer] Topic '%s' 已存在，分区数: %d（符合要求）", topic, actualPartitions)
		} else {
			log.Printf("[Producer] Topic '%s' 已存在，但分区数不匹配 - 当前: %d, 期望: %d", topic, actualPartitions, numPartitions)
			log.Printf("[Producer] 注意: Kafka 不支持修改已存在 topic 的分区数，如需 %d 个分区，请先删除该 topic", numPartitions)
		}
		return nil
	}

	// Topic 不存在，创建 topic
	log.Printf("[Producer] Topic '%s' 不存在，正在创建（分区数: %d, 副本因子: %d）...", topic, numPartitions, replicationFactor)

	// 创建 topic 配置
	topicSpec := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}

	// 创建 topic
	result, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec}, kafka.SetAdminOperationTimeout(10*time.Second))
	if err != nil {
		return fmt.Errorf("创建 topic 失败: %w", err)
	}

	// 检查创建结果
	for _, topicResult := range result {
		if topicResult.Error.Code() != kafka.ErrNoError {
			if topicResult.Error.Code() == kafka.ErrTopicAlreadyExists {
				log.Printf("[Producer] Topic '%s' 已存在（可能在创建过程中被其他进程或 Kafka 自动创建）", topic)
				// 重新获取元数据，检查实际分区数
				time.Sleep(500 * time.Millisecond) // 等待一下，确保 topic 已完全创建
				metadata, err := adminClient.GetMetadata(&topic, false, 5000)
				if err == nil {
					if topicMeta, exists := metadata.Topics[topic]; exists {
						actualPartitions := len(topicMeta.Partitions)
						log.Printf("[Producer] Topic '%s' 实际分区数: %d（期望: %d）", topic, actualPartitions, numPartitions)
						if actualPartitions != numPartitions {
							return fmt.Errorf("Topic '%s' 的分区数不匹配 - 实际: %d, 期望: %d（可能是 Kafka 自动创建使用了默认配置，请先删除该 topic 或禁用 auto.create.topics.enable）", topic, actualPartitions, numPartitions)
						}
					}
				}
				return nil
			}
			return fmt.Errorf("创建 topic '%s' 失败: %v", topicResult.Topic, topicResult.Error)
		}
		log.Printf("[Producer] Topic '%s' 创建成功（分区数: %d, 副本因子: %d）", topic, numPartitions, replicationFactor)

		// 验证创建的分区数是否正确
		time.Sleep(500 * time.Millisecond) // 等待一下，确保 topic 已完全创建
		metadata, err := adminClient.GetMetadata(&topic, false, 5000)
		if err == nil {
			if topicMeta, exists := metadata.Topics[topic]; exists {
				actualPartitions := len(topicMeta.Partitions)
				if actualPartitions != numPartitions {
					log.Printf("[Producer] 警告: Topic '%s' 创建成功，但分区数不匹配 - 实际: %d, 期望: %d", topic, actualPartitions, numPartitions)
				} else {
					log.Printf("[Producer] Topic '%s' 分区数验证通过: %d", topic, actualPartitions)
				}
			}
		}
	}

	return nil
}

// EnsureTopicDefault 检查 topic 是否存在，如果不存在则创建（使用默认配置）
// 参数:
//   - topic: 主题名称
//
// 返回:
//   - error: 检查或创建失败时返回错误
func (p *Producer) EnsureTopicDefault(topic string) error {
	return p.EnsureTopic(topic, 3, 1)
}
