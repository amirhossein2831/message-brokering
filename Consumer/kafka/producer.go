package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

type Publisher struct {
	Producer *kafka.Producer
}

func NewKafkaProducer() (*Publisher, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")),
		"client.id":         os.Getenv("KAFKA_CLIENT_ID"),
		"acks":              os.Getenv("KAFKA_CLIENT_ACK"),
	})
	if err != nil {
		return nil, err
	}

	return &Publisher{Producer: p}, nil
}

func (k *Publisher) Produce(topic string, data []byte, partition int32) error {
	deliveryChan := make(chan kafka.Event)
	defer k.Producer.Close()

	err := k.Producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
			Value:          data,
		}, deliveryChan,
	)
	if err != nil {
		return err
	}
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %v", m.TopicPartition.Error)
	}

	close(deliveryChan)
	return nil
}
