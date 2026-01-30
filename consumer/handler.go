package consumer

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

// MessageHandler 消息处理接口
// 所有消息处理器必须实现此接口
// Handle 方法用于处理从 Kafka 接收到的消息
// 参数:
//   - message: Kafka 消息对象，包含主题、分区、偏移量、键值等信息
// 返回:
//   - error: 处理失败时返回错误，成功返回 nil
type MessageHandler interface {
	Handle(message *kafka.Message) error
}
