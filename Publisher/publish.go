package Publisher

import (
	"context"
	"fmt"
	redis2 "kafkaAndRabbitAndReddisAndGooooo/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/queue"
)

type Channel string

const (
	Redis    Channel = "Consumer"
	RabbitMQ Channel = "rabbitmq"
	Kafka    Channel = "kafka"
)

type Publisher struct {
	Channel Channel
	Queue   queue.Queue
}

func NewPublisher(channel Channel, queue queue.Queue) *Publisher {
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
	return nil
}
