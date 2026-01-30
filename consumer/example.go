package consumer

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ExampleMessageHandler 示例消息处理器
// 这是一个简单的消息处理器实现，用于演示如何使用 MessageHandler 接口
// 在实际使用中，应该根据业务需求实现自己的处理器
type ExampleMessageHandler struct {
	topic string // 主题名称
}

// Handle 实现 MessageHandler 接口
// 处理接收到的 Kafka 消息，打印消息信息
// 参数:
//   - message: Kafka 消息对象
// 返回:
//   - error: 处理失败时返回错误，成功返回 nil
func (h *ExampleMessageHandler) Handle(message *kafka.Message) error {
	topic := ""
	if message.TopicPartition.Topic != nil {
		topic = *message.TopicPartition.Topic
	}

	log.Printf("[ExampleHandler] [%s] 收到消息 - 分区: %d, 偏移量: %d, 键: %s, 值: %s",
		topic, message.TopicPartition.Partition, int64(message.TopicPartition.Offset),
		string(message.Key), string(message.Value))

	// 这里可以添加业务逻辑处理
	// 例如：解析消息、存储到数据库、调用其他服务等

	return nil
}

// NewExampleMessageHandler 创建示例消息处理器
// 参数:
//   - topic: 主题名称
// 返回:
//   - *ExampleMessageHandler: 新创建的处理器实例
func NewExampleMessageHandler(topic string) *ExampleMessageHandler {
	return &ExampleMessageHandler{
		topic: topic,
	}
}

// AdvancedMessageHandler 高级消息处理器示例
// 这是一个更复杂的处理器实现，支持错误重试等高级功能
// 可以作为实现自定义处理器的参考
type AdvancedMessageHandler struct {
	topic      string // 主题名称
	maxRetries int    // 最大重试次数
}

// Handle 实现 MessageHandler 接口
// 处理接收到的 Kafka 消息，包含业务逻辑处理和错误处理
// 参数:
//   - message: Kafka 消息对象
// 返回:
//   - error: 处理失败时返回错误，成功返回 nil
func (h *AdvancedMessageHandler) Handle(message *kafka.Message) error {
	topic := ""
	if message.TopicPartition.Topic != nil {
		topic = *message.TopicPartition.Topic
	}

	log.Printf("[%s] 处理消息 - 分区: %d, 偏移量: %d, 键: %s, 值: %s",
		topic, message.TopicPartition.Partition, int64(message.TopicPartition.Offset),
		string(message.Key), string(message.Value))

	// 模拟业务处理
	if err := h.processMessage(message); err != nil {
		// 可以在这里实现重试逻辑
		log.Printf("[%s] 处理消息失败: %v", topic, err)
		return err
	}

	return nil
}

// processMessage 处理消息的业务逻辑
// 这里实现具体的业务处理，例如：解析 JSON、验证数据、调用 API、存储数据库等
// 参数:
//   - message: Kafka 消息对象
// 返回:
//   - error: 处理失败时返回错误
func (h *AdvancedMessageHandler) processMessage(message *kafka.Message) error {
	// 这里实现具体的业务逻辑
	// 例如：解析 JSON、验证数据、调用 API、存储数据库等

	// 模拟处理时间
	time.Sleep(10 * time.Millisecond)

	// 模拟处理失败（示例）
	// if someCondition {
	//     return fmt.Errorf("处理失败")
	// }

	return nil
}

// NewAdvancedMessageHandler 创建高级消息处理器
// 参数:
//   - topic: 主题名称
//   - maxRetries: 最大重试次数
// 返回:
//   - *AdvancedMessageHandler: 新创建的处理器实例
func NewAdvancedMessageHandler(topic string, maxRetries int) *AdvancedMessageHandler {
	return &AdvancedMessageHandler{
		topic:      topic,
		maxRetries: maxRetries,
	}
}
