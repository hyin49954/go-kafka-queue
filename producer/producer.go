package producer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hyin49954/gopoolx"
)

// Producer Kafka 生产者客户端
// 支持批量发送消息，通过 channel 控制批数量
// 当 channel 满了或达到超时时间时，自动批量发送所有消息
type Producer struct {
	producer     *kafka.Producer    // Kafka 底层生产者实例
	brokers      []string           // Kafka broker 地址列表
	messageChan  chan *MessageItem  // 消息通道，用于收集待发送的消息
	batchSize    int                // 批量发送的消息数量
	batchTimeout time.Duration      // 批量发送的超时时间
	ctx          context.Context    // 上下文，用于控制 goroutine 生命周期
	cancel       context.CancelFunc // 取消函数，用于停止生产者
	wg           sync.WaitGroup     // 等待组，用于等待 goroutine 完成
	pool         *gopoolx.Pool      // 协程池，用于处理发送结果事件
	started      bool               // 是否已启动
	mu           sync.Mutex         // 互斥锁，保护并发访问
}

// NewProducer 创建新的 Kafka 生产者客户端实例
// 只负责创建客户端，不启动任何 goroutine
// 参数:
//   - brokers: Kafka broker 地址列表，例如 []string{"localhost:9092"}
//
// 返回:
//   - *Producer: 生产者实例
//   - error: 创建失败时返回错误
func NewProducer(brokers []string) (*Producer, error) {
	cfg := DefaultConfig(brokers)
	return NewProducerWithConfig(cfg)
}

// NewProducerWithConfig 使用配置创建新的 Kafka 生产者客户端实例
// 参数:
//   - config: 生产者配置
//
// 返回:
//   - *Producer: 生产者实例
//   - error: 创建失败时返回错误
func NewProducerWithConfig(config *Config) (*Producer, error) {
	if config == nil {
		return nil, fmt.Errorf("配置不能为空")
	}
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("broker 地址列表不能为空")
	}

	kafkaConfig := config.ToKafkaConfigMap()
	producer, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建生产者失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Producer{
		producer:    producer,
		brokers:     config.Brokers,
		messageChan: nil, // 在 Start 方法中初始化
		ctx:         ctx,
		cancel:      cancel,
		started:     false,
	}

	return p, nil
}

// Start 启动生产者，开始处理事件和批量发送
// 启动后会创建两个 goroutine：
//  1. 处理发送结果事件的 goroutine
//  2. 批量发送消息的 goroutine
//
// 参数:
//   - batchSize: 批量发送的消息数量，达到这个数量会自动发送
//   - batchTimeout: 批量发送的超时时间，即使未达到批数量，超过这个时间也会自动发送
//
// 返回:
//   - error: 启动失败时返回错误（例如：已经启动）
func (p *Producer) Start(batchSize int, batchTimeout time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return fmt.Errorf("生产者已经启动")
	}

	// 参数验证
	if batchSize <= 0 {
		return fmt.Errorf("批量大小必须大于 0，当前值: %d", batchSize)
	}
	if batchTimeout <= 0 {
		return fmt.Errorf("批量超时时间必须大于 0，当前值: %v", batchTimeout)
	}

	p.batchSize = batchSize
	p.batchTimeout = batchTimeout
	// channel 缓冲区设置为 batchSize 的 2 倍，避免频繁阻塞
	// 当达到 batchSize 时会触发批量发送，但允许一些缓冲
	p.messageChan = make(chan *MessageItem, batchSize*2)

	// 启动一个 goroutine 来处理发送结果
	pool := gopoolx.New(
		5,
		gopoolx.WithRetry(2),
		gopoolx.WithRetryDelay(200*time.Millisecond),
		gopoolx.WithQueueSize(100),
	)
	pool.Run(p.ctx)
	p.pool = pool

	pool.Submit(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case e, ok := <-p.producer.Events():
				if !ok {
					return nil
				}
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Printf("[Producer] 消息发送失败 - 主题: %s, 错误: %v",
							getTopicName(ev.TopicPartition.Topic), ev.TopicPartition.Error)
					} else {
						log.Printf("[Producer] 消息已发送 - 主题: %s, 分区: %d, 偏移量: %d",
							getTopicName(ev.TopicPartition.Topic), ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				case kafka.Error:
					log.Printf("[Producer] Kafka 错误: %v", ev)
				}
			}
		}
	})

	p.wg.Add(1)
	go p.batchSender()

	p.started = true
	return nil
}

// batchSender 批量发送消息的 goroutine
// 从 channel 读取消息并累积，触发条件：
//  1. 当累积的消息数量达到 batchSize 时，立即批量发送
//  2. 当达到 batchTimeout 超时时间时，发送当前累积的所有消息
//
// 这样可以减少网络往返，提高发送效率
func (p *Producer) batchSender() {
	defer p.wg.Done()

	batch := make([]*MessageItem, 0, p.batchSize)
	ticker := time.NewTicker(p.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			// 关闭时发送剩余的消息
			p.produceBatch(batch)
			return

		case msg, ok := <-p.messageChan:
			if !ok {
				// channel 已关闭，发送剩余消息
				p.produceBatch(batch)
				return
			}

			batch = append(batch, msg)

			// 当达到批数量时立即发送全部消息（channel 满了会自动触发）
			if len(batch) >= p.batchSize {
				p.produceBatch(batch)
				batch = batch[:0]            // 清空批次
				ticker.Reset(p.batchTimeout) // 重置定时器
			}

		case <-ticker.C:
			// 超时（5秒）发送当前 channel 中的所有消息
			if len(batch) > 0 {
				p.produceBatch(batch)
				batch = batch[:0] // 清空批次
			}
		}
	}
}

// produceBatch 批量发送消息
// 真正的批量发送实现，减少网络往返次数
// 先批量提交所有消息到内部队列，然后统一刷新，让 librdkafka 批量发送
// 参数:
//   - batch: 要发送的消息批次
func (p *Producer) produceBatch(batch []*MessageItem) {
	if len(batch) == 0 {
		return
	}

	// 记录批量发送开始
	startTime := time.Now()
	log.Printf("[Producer] 开始批量发送 %d 条消息", len(batch))

	// 先批量提交所有消息到内部队列（不立即发送，减少网络往返）
	// Produce 是异步的，消息会被放入内部队列，然后批量发送
	messages := make([]*kafka.Message, 0, len(batch))
	for _, item := range batch {
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &item.Topic,
				Partition: kafka.PartitionAny, // 自动分配分区
			},
		}

		// 如果 key 不为空，设置 key（Kafka 会根据 key 的 hash 值分配到特定分区）
		// 如果 key 为空，Kafka 会使用轮询方式分配分区，消息会均匀分布到各个分区
		if item.Key != "" {
			msg.Key = []byte(item.Key)
		}

		msg.Value = []byte(item.Value)
		messages = append(messages, msg)
	}

	// 批量提交所有消息（快速提交到内部队列，不等待发送）
	var firstErr error
	for _, msg := range messages {
		if err := p.producer.Produce(msg, nil); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			log.Printf("[Producer] 批量提交消息失败: %v", err)
		}
	}

	// 统一刷新，让 librdkafka 批量发送所有消息（减少网络往返）
	// Flush 会等待所有待发送的消息批量发送完成
	remaining := p.producer.Flush(1000)
	if remaining > 0 {
		log.Printf("[Producer] 警告: 仍有 %d 条消息未发送完成", remaining)
	}

	duration := time.Since(startTime)
	if firstErr != nil {
		log.Printf("[Producer] 批量发送完成（耗时: %v, 错误: %v）", duration, firstErr)
	} else {
		log.Printf("[Producer] 批量发送完成（耗时: %v, 成功: %d 条）", duration, len(batch))
	}
}

// getTopicName 安全获取 topic 名称
func getTopicName(topic *string) string {
	if topic == nil {
		return "<nil>"
	}
	return *topic
}

// SendMessage 发送消息到指定主题（异步，消息会被批量发送）
// 消息会被放入 channel，等待批量发送
// 当 channel 满了（达到 batchSize）或超时（batchTimeout）时，会自动批量发送所有消息
//
// 注意：如果设置了 key，Kafka 会根据 key 的 hash 值分配到特定分区
//
//	如果 key 为空，Kafka 会使用轮询方式分配分区，消息会均匀分布到各个分区
//
// 参数:
//   - topic: 主题名称
//   - key: 消息键（如果为空，消息会均匀分布到各个分区）
//   - value: 消息值
//
// 返回:
//   - error: 发送失败时返回错误（例如：生产者未启动、已关闭、channel 已满等）
func (p *Producer) SendMessage(topic, key, value string) error {
	// 参数验证
	if topic == "" {
		return fmt.Errorf("topic 名称不能为空")
	}
	if value == "" {
		return fmt.Errorf("消息内容不能为空")
	}

	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return fmt.Errorf("生产者未启动，请先调用 Start() 方法")
	}
	msgChan := p.messageChan
	p.mu.Unlock()

	select {
	case <-p.ctx.Done():
		return fmt.Errorf("生产者已关闭")
	case msgChan <- &MessageItem{
		Topic: topic,
		Key:   key,
		Value: value,
	}:
		return nil
	default:
		// channel 已满，返回错误
		return fmt.Errorf("消息队列已满，无法发送消息（batchSize: %d）", p.batchSize)
	}
}

// Flush 刷新所有待发送的消息
// 等待所有待发送的消息发送完成
// 参数:
//   - timeoutMs: 超时时间（毫秒）
//
// 返回:
//   - int: 剩余未发送的消息数量，0 表示全部发送完成
func (p *Producer) Flush(timeoutMs int) int {
	return p.producer.Flush(timeoutMs)
}

// Close 关闭生产者，等待所有消息发送完成
// 优雅关闭流程：
//  1. 关闭 channel，停止接收新消息
//  2. 等待批量发送 goroutine 完成（处理 channel 中的剩余消息）
//  3. 多次刷新，确保所有消息都发送完成（最多重试 10 次）
//  4. 取消 context，关闭底层生产者
func (p *Producer) Close() {
	p.mu.Lock()
	started := p.started
	msgChan := p.messageChan
	p.mu.Unlock()

	if !started {
		return
	}

	log.Println("[Producer] 正在关闭生产者，等待所有消息发送完成...")

	// 如果已启动，关闭 channel 停止接收新消息
	if msgChan != nil {
		close(msgChan)
	}

	// 等待批量发送 goroutine 完成（处理 channel 中的剩余消息）
	p.wg.Wait()

	// 多次刷新，确保所有消息都发送完成
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		remaining := p.producer.Flush(1000) // 每次等待 1 秒
		if remaining == 0 {
			log.Println("[Producer] 所有消息已发送完成")
			break
		}
		if i < maxRetries-1 {
			log.Printf("[Producer] 仍有 %d 条消息未发送完成，继续等待... (重试 %d/%d)", remaining, i+1, maxRetries)
			time.Sleep(500 * time.Millisecond)
		} else {
			log.Printf("[Producer] 警告: 仍有 %d 条消息未发送完成，强制关闭", remaining)
		}
	}

	// 取消 context
	p.cancel()

	// 关闭生产者
	p.producer.Close()
	log.Println("[Producer] 生产者已关闭")
}
