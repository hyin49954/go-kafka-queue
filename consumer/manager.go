package consumer

import (
	"fmt"
	"log"
	"sync"
)

// ConsumerManager 消费者管理器
// 用于统一管理多个消费者，支持批量启动、停止等操作
// 每个 topic 对应一个消费者实例
type ConsumerManager struct {
	consumers map[string]*Consumer // 消费者映射表，key: topic, value: consumer
	mu        sync.RWMutex         // 读写锁，保护并发访问
}

// NewConsumerManager 创建消费者管理器
// 返回:
//   - *ConsumerManager: 新创建的管理器实例
func NewConsumerManager() *ConsumerManager {
	return &ConsumerManager{
		consumers: make(map[string]*Consumer),
	}
}

// AddConsumer 添加消费者到管理器
// 参数:
//   - consumer: 要添加的消费者实例
//
// 返回:
//   - error: 如果该 topic 的消费者已存在，返回错误
func (m *ConsumerManager) AddConsumer(consumer *Consumer) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic := consumer.Topic()
	if _, exists := m.consumers[topic]; exists {
		return fmt.Errorf("消费者已存在: topic=%s", topic)
	}

	m.consumers[topic] = consumer
	return nil
}

// StartAll 启动所有消费者
// 批量启动管理器中所有的消费者，使用协程池并发处理消息
// 参数:
//   - poolSize: 协程池大小，控制每个消费者并发处理消息的协程数量
//   - queueSize: 任务队列大小，控制每个消费者可以排队等待处理的消息数量
//
// 返回:
//   - error: 如果有消费者启动失败，返回包含所有错误的组合错误
func (m *ConsumerManager) StartAll(poolSize, queueSize int) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var errors []error
	for topic, consumer := range m.consumers {
		if err := consumer.Start(poolSize, queueSize); err != nil {
			log.Printf("[ConsumerManager] 启动消费者失败 [Topic: %s]: %v", topic, err)
			errors = append(errors, fmt.Errorf("topic %s: %w", topic, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("部分消费者启动失败: %v", errors)
	}

	log.Printf("[ConsumerManager] 所有消费者已启动，共 %d 个", len(m.consumers))
	return nil
}

// StopAll 停止所有消费者
// 批量停止管理器中所有的消费者
// 返回:
//   - error: 如果有消费者停止失败，返回包含所有错误的组合错误
func (m *ConsumerManager) StopAll() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var errors []error
	for topic, consumer := range m.consumers {
		if err := consumer.Stop(); err != nil {
			log.Printf("[ConsumerManager] 停止消费者失败 [Topic: %s]: %v", topic, err)
			errors = append(errors, fmt.Errorf("topic %s: %w", topic, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("部分消费者停止失败: %v", errors)
	}

	log.Printf("[ConsumerManager] 所有消费者已停止，共 %d 个", len(m.consumers))
	return nil
}

// GetConsumer 获取指定 topic 的消费者
// 参数:
//   - topic: 主题名称
//
// 返回:
//   - *Consumer: 消费者实例，如果不存在返回 nil
//   - bool: 是否存在，true 表示存在，false 表示不存在
func (m *ConsumerManager) GetConsumer(topic string) (*Consumer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	consumer, exists := m.consumers[topic]
	return consumer, exists
}

// RemoveConsumer 移除消费者
// 先停止消费者，然后从管理器中移除
// 参数:
//   - topic: 要移除的主题名称
//
// 返回:
//   - error: 如果消费者不存在或停止失败，返回错误
func (m *ConsumerManager) RemoveConsumer(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumer, exists := m.consumers[topic]
	if !exists {
		return fmt.Errorf("消费者不存在: topic=%s", topic)
	}

	// 先停止消费者
	if err := consumer.Stop(); err != nil {
		return fmt.Errorf("停止消费者失败: %w", err)
	}

	delete(m.consumers, topic)
	return nil
}

// ListConsumers 列出所有消费者的 topic 列表
// 返回:
//   - []string: 所有已添加的 topic 名称列表
func (m *ConsumerManager) ListConsumers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topics := make([]string, 0, len(m.consumers))
	for topic := range m.consumers {
		topics = append(topics, topic)
	}
	return topics
}

// Count 获取消费者数量
// 返回:
//   - int: 当前管理器中消费者的数量
func (m *ConsumerManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.consumers)
}
