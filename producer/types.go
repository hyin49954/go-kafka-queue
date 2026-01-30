package producer

// MessageItem 消息项
// 用于在批量发送时暂存消息
type MessageItem struct {
	Topic string // 主题名称
	Key   string // 消息键（如果为空，Kafka 会使用轮询方式分配分区，消息会均匀分布到各个分区）
	Value string // 消息值
}
