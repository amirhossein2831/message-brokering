package kafka_queue

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

type Kafka struct {
	Producer *kafka.Producer
}

func NewKafka(clientId string, ack string) (*Kafka, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")),
		"client.id":         clientId,
		"acks":              ack,
	})
	if err != nil {
		return nil, err
	}
	return &Kafka{Producer: p}, nil
}

func (k *Kafka) Produce(topic string, data []byte, partition int32) error {
	deliveryChan := make(chan kafka.Event)

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
	fmt.Printf("Delivered message to %v,messsage: %s \n", m.TopicPartition, data)

	close(deliveryChan)
	return nil
}
