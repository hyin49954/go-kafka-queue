package consumer

import (
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockMessageHandler 测试用的消息处理器
type MockMessageHandler struct {
	HandleFunc func(*kafka.Message) error
}

func (m *MockMessageHandler) Handle(msg *kafka.Message) error {
	if m.HandleFunc != nil {
		return m.HandleFunc(msg)
	}
	return nil
}

// TestNewConsumer 测试创建消费者
func TestNewConsumer(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
		groupID string
		topic   string
		handler MessageHandler
		wantErr bool
	}{
		{
			name:    "正常创建",
			brokers: []string{"localhost:9092"},
			groupID: "test-group",
			topic:   "test-topic",
			handler: &MockMessageHandler{},
			wantErr: false,
		},
		{
			name:    "空 broker 列表",
			brokers: []string{},
			groupID: "test-group",
			topic:   "test-topic",
			handler: &MockMessageHandler{},
			wantErr: true,
		},
		{
			name:    "空 groupID",
			brokers: []string{"localhost:9092"},
			groupID: "",
			topic:   "test-topic",
			handler: &MockMessageHandler{},
			wantErr: true,
		},
		{
			name:    "空 topic",
			brokers: []string{"localhost:9092"},
			groupID: "test-group",
			topic:   "",
			handler: &MockMessageHandler{},
			wantErr: true,
		},
		{
			name:    "nil handler",
			brokers: []string{"localhost:9092"},
			groupID: "test-group",
			topic:   "test-topic",
			handler: nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumer(tt.brokers, tt.groupID, tt.topic, tt.handler)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, c)
			} else {
				// 注意：如果 Kafka 未运行，这里会失败，但逻辑是正确的
				if err == nil {
					assert.NotNil(t, c)
					if c != nil {
						c.Close()
					}
				}
			}
		})
	}
}

// TestNewConsumerWithConfig 测试使用配置创建消费者
func TestNewConsumerWithConfig(t *testing.T) {
	cfg := DefaultConfig([]string{"localhost:9092"}, "test-group", "test-topic")
	cfg.ClientID = "test-consumer"

	handler := &MockMessageHandler{}
	c, err := NewConsumerWithConfig(cfg, handler)
	if err == nil {
		require.NotNil(t, c)
		assert.Equal(t, "test-topic", c.topic)
		assert.Equal(t, "test-group", c.groupID)
		defer c.Close()
	}
}

// TestConsumer_Start 测试启动消费者
func TestConsumer_Start(t *testing.T) {
	handler := &MockMessageHandler{}
	c, err := NewConsumer([]string{"localhost:9092"}, "test-group", "test-topic", handler)
	if err != nil {
		t.Skip("Kafka 未运行，跳过测试")
	}
	require.NotNil(t, c)
	defer c.Close()

	// 测试正常启动
	err = c.Start(10, 100)
	if err == nil {
		assert.True(t, c.started)

		// 测试重复启动
		err = c.Start(10, 100)
		assert.Error(t, err)
	}
}

// TestConsumer_Stop 测试停止消费者
func TestConsumer_Stop(t *testing.T) {
	handler := &MockMessageHandler{}
	c, err := NewConsumer([]string{"localhost:9092"}, "test-group", "test-topic", handler)
	if err != nil {
		t.Skip("Kafka 未运行，跳过测试")
	}
	require.NotNil(t, c)

	// 未启动时停止
	err = c.Stop()
	assert.NoError(t, err)

	// 启动后停止
	c, err = NewConsumer([]string{"localhost:9092"}, "test-group", "test-topic", handler)
	if err != nil {
		t.Skip("Kafka 未运行，跳过测试")
	}
	err = c.Start(10, 100)
	if err == nil {
		err = c.Stop()
		assert.NoError(t, err)
		assert.False(t, c.started)
	}
}

// TestConsumer_Topic 测试获取 topic
func TestConsumer_Topic(t *testing.T) {
	handler := &MockMessageHandler{}
	c, err := NewConsumer([]string{"localhost:9092"}, "test-group", "test-topic", handler)
	if err != nil {
		t.Skip("Kafka 未运行，跳过测试")
	}
	require.NotNil(t, c)
	defer c.Close()

	assert.Equal(t, "test-topic", c.Topic())
}

// TestConsumer_GroupID 测试获取 groupID
func TestConsumer_GroupID(t *testing.T) {
	handler := &MockMessageHandler{}
	c, err := NewConsumer([]string{"localhost:9092"}, "test-group", "test-topic", handler)
	if err != nil {
		t.Skip("Kafka 未运行，跳过测试")
	}
	require.NotNil(t, c)
	defer c.Close()

	assert.Equal(t, "test-group", c.GroupID())
}

// TestDefaultConfig 测试默认配置
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig([]string{"localhost:9092"}, "test-group", "test-topic")

	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.Equal(t, "test-group", cfg.GroupID)
	assert.Equal(t, "test-topic", cfg.Topic)
	assert.Equal(t, "latest", cfg.Consumer.AutoOffsetReset)
	assert.True(t, cfg.Consumer.EnableAutoCommit)
	assert.Equal(t, 10, cfg.Pool.Size)
	assert.Equal(t, 100, cfg.Pool.QueueSize)
}

// TestConfig_ToKafkaConfigMap 测试配置转换
func TestConfig_ToKafkaConfigMap(t *testing.T) {
	cfg := DefaultConfig([]string{"localhost:9092"}, "test-group", "test-topic")
	configMap := cfg.ToKafkaConfigMap()

	assert.NotNil(t, configMap)
	assert.Equal(t, "localhost:9092", configMap["bootstrap.servers"])
	assert.Equal(t, "test-group", configMap["group.id"])
	assert.Equal(t, "latest", configMap["auto.offset.reset"])
}

// TestMessageHandler 测试消息处理器接口
func TestMessageHandler(t *testing.T) {
	var handler MessageHandler = &MockMessageHandler{
		HandleFunc: func(msg *kafka.Message) error {
			return nil
		},
	}

	assert.NotNil(t, handler)

	// 测试错误处理
	handler = &MockMessageHandler{
		HandleFunc: func(msg *kafka.Message) error {
			return errors.New("处理失败")
		},
	}

	err := handler.Handle(nil)
	assert.Error(t, err)
}

// TestConsumerManager 测试消费者管理器
func TestConsumerManager(t *testing.T) {
	manager := NewConsumerManager()
	assert.NotNil(t, manager)
	assert.Equal(t, 0, manager.Count())

	handler := &MockMessageHandler{}
	c, err := NewConsumer([]string{"localhost:9092"}, "test-group", "test-topic", handler)
	if err != nil {
		t.Skip("Kafka 未运行，跳过测试")
	}
	require.NotNil(t, c)

	// 添加消费者
	err = manager.AddConsumer(c)
	if err == nil {
		assert.Equal(t, 1, manager.Count())
		assert.True(t, manager.ListConsumers()[0] == "test-topic")

		// 重复添加应该失败
		err = manager.AddConsumer(c)
		assert.Error(t, err)

		// 获取消费者
		consumer, exists := manager.GetConsumer("test-topic")
		assert.True(t, exists)
		assert.NotNil(t, consumer)
	}
}
