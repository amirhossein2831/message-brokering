package Publisher

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	k "kafkaAndRabbitAndReddisAndGooooo/Consumer/kafka"
	redis2 "kafkaAndRabbitAndReddisAndGooooo/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/job"
)

type Channel string

const (
	Redis    Channel = "connection"
	RabbitMQ Channel = "rabbitmq"
	Kafka    Channel = "kafka"
)

type Publisher struct {
	Channel Channel
	Queue   job.Queue
}

func NewPublisher(channel Channel, queue job.Queue) *Publisher {
	return &Publisher{
		Channel: channel,
		Queue:   queue,
	}
}

func (p *Publisher) Publish(payload []byte) error {
	switch p.Channel {
	case Redis:
		err := p.redisPub(payload)
		if err != nil {
			return err
		}
	case RabbitMQ:
		err := p.rabbitMqPub(payload)
		if err != nil {
			return err
		}
	case Kafka:
		err := p.kafkaPub(payload)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Publisher) redisPub(payload []byte) error {
	ctx := context.Background()
	err := redis2.GetInstance().GetClient().Publish(ctx, string(p.Queue), payload).Err()
	if err != nil {
		return fmt.Errorf("error publishing message: %v", err)
	}
	return nil
}

func (p *Publisher) rabbitMqPub(payload []byte) error {
	return nil
}

func (p *Publisher) kafkaPub(payload []byte) error {
	pub, err := k.NewKafka()
	if err != nil {
		return fmt.Errorf("failed to create connection: %s", err)
	}

	err = pub.Produce(string(p.Queue), payload, kafka.PartitionAny)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	pub.Producer.Close()
	return nil
}
