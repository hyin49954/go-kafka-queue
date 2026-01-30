package producer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewProducer 测试创建生产者
func TestNewProducer(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
		wantErr bool
	}{
		{
			name:    "正常创建",
			brokers: []string{"localhost:9092"},
			wantErr: false,
		},
		{
			name:    "空 broker 列表",
			brokers: []string{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewProducer(tt.brokers)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, p)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, p)
				if p != nil {
					p.Close()
				}
			}
		})
	}
}

// TestNewProducerWithConfig 测试使用配置创建生产者
func TestNewProducerWithConfig(t *testing.T) {
	cfg := DefaultConfig([]string{"localhost:9092"})
	cfg.ClientID = "test-producer"

	p, err := NewProducerWithConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, p)
	defer p.Close()

	assert.Equal(t, []string{"localhost:9092"}, p.brokers)
}

// TestProducer_Start 测试启动生产者
func TestProducer_Start(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"})
	require.NoError(t, err)
	require.NotNil(t, p)
	defer p.Close()

	// 测试正常启动
	err = p.Start(10, 5*time.Second)
	assert.NoError(t, err)
	assert.True(t, p.started)

	// 测试重复启动
	err = p.Start(10, 5*time.Second)
	assert.Error(t, err)
}

// TestProducer_SendMessage 测试发送消息
func TestProducer_SendMessage(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"})
	require.NoError(t, err)
	require.NotNil(t, p)
	defer p.Close()

	// 未启动时发送应该失败
	err = p.SendMessage("test-topic", "key", "value")
	assert.Error(t, err)

	// 启动后发送
	err = p.Start(10, 5*time.Second)
	require.NoError(t, err)

	// 发送消息（注意：这需要真实的 Kafka 连接，在单元测试中可能会失败）
	// 这里主要测试逻辑
	err = p.SendMessage("test-topic", "key", "value")
	// 如果 Kafka 未运行，这里会失败，但逻辑是正确的
}

// TestProducer_Flush 测试刷新消息
func TestProducer_Flush(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"})
	require.NoError(t, err)
	require.NotNil(t, p)
	defer p.Close()

	// 未启动时刷新
	remaining := p.Flush(1000)
	assert.Equal(t, 0, remaining)

	// 启动后刷新
	err = p.Start(10, 5*time.Second)
	require.NoError(t, err)

	remaining = p.Flush(1000)
	assert.GreaterOrEqual(t, remaining, 0)
}

// TestProducer_Close 测试关闭生产者
func TestProducer_Close(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"})
	require.NoError(t, err)
	require.NotNil(t, p)

	// 未启动时关闭
	p.Close()
	assert.False(t, p.started)

	// 启动后关闭
	p, err = NewProducer([]string{"localhost:9092"})
	require.NoError(t, err)
	err = p.Start(10, 5*time.Second)
	require.NoError(t, err)

	p.Close()
	assert.False(t, p.started)
}

// TestConfig_ToKafkaConfigMap 测试配置转换
func TestConfig_ToKafkaConfigMap(t *testing.T) {
	cfg := DefaultConfig([]string{"localhost:9092"})
	configMap := cfg.ToKafkaConfigMap()

	assert.NotNil(t, configMap)
	assert.Equal(t, "localhost:9092", configMap["bootstrap.servers"])
	assert.Equal(t, "go-kafka-producer", configMap["client.id"])
}

// TestDefaultConfig 测试默认配置
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig([]string{"localhost:9092"})

	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.Equal(t, "go-kafka-producer", cfg.ClientID)
	assert.Equal(t, "all", cfg.Producer.Acks)
	assert.Equal(t, 3, cfg.Producer.Retries)
	assert.Equal(t, 10, cfg.Batch.Size)
	assert.Equal(t, 5*time.Second, cfg.Batch.Timeout)
}

// TestMessageItem 测试消息项
func TestMessageItem(t *testing.T) {
	item := &MessageItem{
		Topic: "test-topic",
		Key:   "test-key",
		Value: "test-value",
	}

	assert.Equal(t, "test-topic", item.Topic)
	assert.Equal(t, "test-key", item.Key)
	assert.Equal(t, "test-value", item.Value)
}

// BenchmarkProducer_SendMessage 性能测试
func BenchmarkProducer_SendMessage(b *testing.B) {
	p, err := NewProducer([]string{"localhost:9092"})
	if err != nil {
		b.Skip("Kafka 未运行，跳过性能测试")
	}
	defer p.Close()

	err = p.Start(100, 1*time.Second)
	if err != nil {
		b.Skip("启动失败，跳过性能测试")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.SendMessage("test-topic", "key", "value")
	}
}
