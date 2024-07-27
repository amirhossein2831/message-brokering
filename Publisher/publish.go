package Publisher

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	k "kafkaAndRabbitAndReddisAndGooooo/Consumer/kafka"
	redis2 "kafkaAndRabbitAndReddisAndGooooo/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/driver"
	"kafkaAndRabbitAndReddisAndGooooo/job"
)

type Publisher struct {
	Driver driver.Driver
	Queue  job.Queue
}

func NewPublisher(driver driver.Driver, queue job.Queue) *Publisher {
	return &Publisher{
		Driver: driver,
		Queue:  queue,
	}
}

func (p *Publisher) Publish(payload []byte) error {
	switch p.Driver {
	case driver.Redis:
		err := p.redisPub(payload)
		if err != nil {
			return err
		}
	case driver.RabbitMQ:
		err := p.rabbitMqPub(payload)
		if err != nil {
			return err
		}
	case driver.Kafka:
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
