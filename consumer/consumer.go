package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hyin49954/gopoolx"
)

// Consumer Kafka 消费者客户端
// 设计原则：一个 topic 对应一个消费者实例
// 每个消费者独立管理自己的生命周期和消息处理
// 使用协程池处理消息，提高并发处理能力
type Consumer struct {
	consumer *kafka.Consumer    // Kafka 底层消费者实例
	topic    string             // 订阅的主题名称
	groupID  string             // 消费者组 ID
	handler  MessageHandler     // 消息处理器接口
	pool     *gopoolx.Pool      // 协程池，用于并发处理消息
	ctx      context.Context    // 上下文，用于控制 goroutine 生命周期
	cancel   context.CancelFunc // 取消函数，用于停止消费
	wg       sync.WaitGroup     // 等待组，用于等待 goroutine 完成
	started  bool               // 是否已启动
	mu       sync.Mutex         // 互斥锁，保护并发访问
}

// NewConsumer 创建新的 Kafka 消费者（单个 topic）
// 参数:
//   - brokers: Kafka broker 地址列表，例如 []string{"localhost:9092"}
//   - groupID: 消费者组 ID，相同 groupID 的消费者会共享消息消费
//   - topic: 要订阅的主题名称
//   - handler: 消息处理器，实现 MessageHandler 接口
//
// 返回:
//   - *Consumer: 消费者实例
//   - error: 创建失败时返回错误
func NewConsumer(brokers []string, groupID, topic string, handler MessageHandler) (*Consumer, error) {
	cfg := DefaultConfig(brokers, groupID, topic)
	return NewConsumerWithConfig(cfg, handler)
}

// NewConsumerWithConfig 使用配置创建新的 Kafka 消费者
// 参数:
//   - config: 消费者配置
//   - handler: 消息处理器，实现 MessageHandler 接口
//
// 返回:
//   - *Consumer: 消费者实例
//   - error: 创建失败时返回错误
func NewConsumerWithConfig(config *Config, handler MessageHandler) (*Consumer, error) {
	if config == nil {
		return nil, fmt.Errorf("配置不能为空")
	}
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("broker 地址列表不能为空")
	}
	if config.GroupID == "" {
		return nil, fmt.Errorf("消费者组 ID 不能为空")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("主题名称不能为空")
	}
	if handler == nil {
		return nil, fmt.Errorf("消息处理器不能为空")
	}

	kafkaConfig := config.ToKafkaConfigMap()
	consumer, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建消费者失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		consumer: consumer,
		topic:    config.Topic,
		groupID:  config.GroupID,
		handler:  handler,
		ctx:      ctx,
		cancel:   cancel,
		started:  false,
	}, nil
}

// Start 启动消费者
// 订阅指定的 topic 并开始消费消息
// 启动后会创建协程池和消费 goroutine
// 消费 goroutine 持续读取消息，使用协程池并发处理消息
// 参数:
//   - poolSize: 协程池大小，控制并发处理消息的协程数量
//   - queueSize: 任务队列大小，控制可以排队等待处理的消息数量
//
// 返回:
//   - error: 启动失败时返回错误（例如：已经启动、订阅失败等）
func (c *Consumer) Start(poolSize, queueSize int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return fmt.Errorf("消费者已经启动")
	}

	// 参数验证和默认值设置
	if poolSize <= 0 {
		poolSize = 10 // 默认值
	}
	if queueSize <= 0 {
		queueSize = 100 // 默认值
	}

	// 创建协程池，用于并发处理消息
	retryCount := 2
	retryDelay := 200 * time.Millisecond

	pool := gopoolx.New(
		poolSize,
		gopoolx.WithRetry(retryCount),
		gopoolx.WithRetryDelay(retryDelay),
		gopoolx.WithQueueSize(queueSize),
	)
	pool.Run(c.ctx)
	c.pool = pool

	// 订阅主题
	err := c.consumer.SubscribeTopics([]string{c.topic}, nil)
	if err != nil {
		c.cancel() // 取消 context，停止协程池
		return fmt.Errorf("订阅主题失败: %w", err)
	}

	c.started = true
	c.wg.Add(1)

	// 启动消费 goroutine
	go c.consume()

	log.Printf("[Consumer] 消费者已启动 - Topic: %s, GroupID: %s, 协程池大小: %d, 队列大小: %d",
		c.topic, c.groupID, poolSize, queueSize)
	return nil
}

// consume 消费消息的 goroutine
// 持续从 Kafka 读取消息，使用协程池并发处理消息
// 使用 100ms 超时来定期检查 context，以便能够优雅停止
// 消息处理通过协程池异步执行，不会阻塞消息接收，提高处理速度
func (c *Consumer) consume() {
	defer c.wg.Done()

	log.Printf("[Consumer] 消费者开始消费消息 - Topic: %s", c.topic)

	for {
		select {
		case <-c.ctx.Done():
			log.Printf("[Consumer] 收到停止信号，正在关闭消费者 - Topic: %s", c.topic)
			return

		default:
			// 设置超时，以便可以检查 context
			msg, err := c.consumer.ReadMessage(100) // 100ms 超时
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					// 超时是正常的，继续循环
					continue
				}
				log.Printf("[Consumer] 读取消息错误 [Topic: %s]: %v", c.topic, err)
				continue
			}

			// 使用协程池异步处理消息，不阻塞消息接收
			// 这样可以提高处理速度，让有限的协程处理无限的消息
			message := msg // 创建副本，避免闭包问题
			c.pool.Submit(func(ctx context.Context) error {
				// 使用接口处理消息
				if err := c.handler.Handle(message); err != nil {
					log.Printf("[Consumer] 处理消息失败 [Topic: %s, 分区: %d, 偏移量: %d]: %v",
						c.topic, message.TopicPartition.Partition, message.TopicPartition.Offset, err)
					return err
				}
				return nil
			})
		}
	}
}

// Stop 停止消费者
// 优雅停止流程：
//  1. 取消 context，停止接收新消息
//  2. 等待消费 goroutine 完成
//  3. 关闭协程池，等待所有正在处理的消息完成
//  4. 关闭底层消费者
//
// 返回:
//   - error: 停止失败时返回错误
func (c *Consumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return nil
	}

	log.Printf("[Consumer] 正在停止消费者 - Topic: %s", c.topic)

	// 取消 context，停止接收新消息
	c.cancel()

	// 等待消费 goroutine 完成
	c.wg.Wait()

	// 协程池会在 context 取消后自动停止，等待所有正在处理的消息完成
	// 给一些时间让正在处理的任务完成
	if c.pool != nil {
		// pool 会在 context 取消后自动停止，这里不需要显式关闭
		// 等待一小段时间，确保所有任务完成
		time.Sleep(100 * time.Millisecond)
	}

	// 关闭消费者
	err := c.consumer.Close()
	if err != nil {
		return fmt.Errorf("关闭消费者失败: %w", err)
	}

	c.started = false
	log.Printf("[Consumer] 消费者已停止 - Topic: %s", c.topic)
	return nil
}

// Close 关闭消费者（别名，保持兼容性）
// 调用 Stop() 方法停止消费者
func (c *Consumer) Close() error {
	return c.Stop()
}

// Topic 获取消费者订阅的 topic
// 返回:
//   - string: 主题名称
func (c *Consumer) Topic() string {
	return c.topic
}

// GroupID 获取消费者组 ID
// 返回:
//   - string: 消费者组 ID
func (c *Consumer) GroupID() string {
	return c.groupID
}
