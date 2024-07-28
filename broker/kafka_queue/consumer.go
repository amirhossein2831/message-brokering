package kafka_queue

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkaAndRabbitAndReddisAndGooooo/Jobb"
	"os"
	"sync"
	"time"
)

type KafkaConsumer struct {
	Consumer *kafka.Consumer
}

// NewKafkaConsumer creates a new KafkaProducer consumer
func NewKafkaConsumer(groupID string, topics []string) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")),
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}

	if err := c.SubscribeTopics(topics, nil); err != nil {
		return nil, err
	}

	return &KafkaConsumer{Consumer: c}, nil
}

func (k *KafkaConsumer) Consume(ctx context.Context, job Jobb.Job) {
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Received shutdown signal, waiting for all processes to finish...")
			k.Consumer.Close()
			wg.Wait()
			return
		default:
			msg, err := k.Consumer.ReadMessage(-1)
			if err != nil {
				fmt.Printf("Consumer error: %v\n", err)
				time.Sleep(time.Second)
				continue
			}

			wg.Add(1)
			go func(msg *kafka.Message) {
				defer wg.Done()

				fmt.Printf("Received message: %s\n", string(msg.Value))
				if err = job(msg.Value); err != nil {
					fmt.Printf("Error processing message: %v\n", err)
				}
			}(msg)
		}
	}
}
