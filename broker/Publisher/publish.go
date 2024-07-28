package Publisher

import (
	"context"
	"fmt"
	k "github.com/amirhossein2831/message-brokering/broker/Consumer/kafka"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/rabbitmq"
	redis2 "github.com/amirhossein2831/message-brokering/broker/Consumer/redis"
	"github.com/amirhossein2831/message-brokering/broker/Driver"
	"github.com/amirhossein2831/message-brokering/job"
	"github.com/amirhossein2831/message-brokering/pkg/logger"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
	"os"
	"time"
)

type Publisher struct {
	Driver Driver.Driver
	Queue  job.Queue
}

func NewPublisher(queue job.Queue) *Publisher {
	return &Publisher{
		Driver: Driver.Driver(os.Getenv("MESSAGE_BROKER_DRIVER")),
		Queue:  queue,
	}
}

func (p *Publisher) Publish(payload []byte) error {
	switch p.Driver {
	case Driver.Redis:
		err := p.redisPub(payload)
		if err != nil {
			return err
		}
	case Driver.RabbitMQ:
		err := p.rabbitMqPub(payload)
		if err != nil {
			return err
		}
	case Driver.Kafka:
		err := p.kafkaPub(payload)
		if err != nil {
			return err
		}
	}

	logger.GetInstance().Info("Publisher: Message Publish Successfully: ", zap.Any("Driver", p.Driver), zap.Any("QueueName: ", p.Queue), zap.ByteString("payload", payload), zap.Time("timestamp", time.Now()))
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
	err := rabbitmq.GetInstance().Publish(string(p.Queue), payload)
	if err != nil {
		return err
	}

	return nil
}

func (p *Publisher) kafkaPub(payload []byte) error {
	pub, err := k.NewKafkaProducer()
	if err != nil {
		return fmt.Errorf("failed to create connection: %s", err)
	}

	err = pub.Produce(string(p.Queue), payload, kafka.PartitionAny)
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	return nil
}
