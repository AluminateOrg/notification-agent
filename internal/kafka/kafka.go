package kafka

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Writer = kafka.Writer
type Reader = kafka.Reader
type Message = kafka.Message

type KafkaClient struct {
	Brokers []string
}

func NewKafkaClient(brokers []string) *KafkaClient {
	return &KafkaClient{Brokers: brokers}
}

func (k *KafkaClient) NewReader(topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: k.Brokers,
		Topic: topic,
		GroupID: groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
}

func (k *KafkaClient) NewWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(k.Brokers...),
		Topic: topic,
		Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		Async: true,
		Compression: kafka.Snappy,
		BatchTimeout: 200 * time.Millisecond,
	}
}

func (k *KafkaClient) NewEmailReader(topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: k.Brokers,
		Topic: topic,
		GroupID: groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
}

func ConsumeLoop(ctx context.Context, r *kafka.Reader, handler func(key, value []byte) error) error {
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if err == io.EOF || err == context.Canceled {
				return nil
			}
			log.Printf("kafka fetch error: %v\n", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err := handler(m.Key, m.Value); err != nil {
			log.Printf("kafka handler error: %v\n", err)
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Printf("kafka commit failed: %v", err)
		}
	}
}

func WriteWithRetires(ctx context.Context, w *kafka.Writer, key, value []byte, maxRetires int) error {
	var err error
	msg := kafka.Message{Key: key, Value: value}
	for i:=0; i<=maxRetires; i++ {
		ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = w.WriteMessages(ctx2, msg)
		cancel()
		if err == nil {return nil}
		log.Printf("kafka write failed attempt %d: %v", i+1, err)
		time.Sleep(time.Duration(i+1) * 500 * time.Millisecond) // backoff
	}
	return err
}