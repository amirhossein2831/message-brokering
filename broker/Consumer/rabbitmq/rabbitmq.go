package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"kafkaAndRabbitAndReddisAndGooooo/job"
	"kafkaAndRabbitAndReddisAndGooooo/pkg/logger"
	"log"
	"os"
	"sync"
	"time"
)

var (
	instance   *RabbitMQ
	clientOnce sync.Once
	wg         sync.WaitGroup
)

type RabbitMQ struct {
	connection *amqp.Connection
	channels   []*amqp.Channel
}

func GetInstance() *RabbitMQ {
	clientOnce.Do(func() {
		conn, err := amqp.Dial(fmt.Sprintf(
			fmt.Sprintf("amqp://%s:%s@%s:%s/",
				os.Getenv("RABBITMQ_USERNAME"),
				os.Getenv("RABBITMQ_PASSWORD"),
				os.Getenv("RABBITMQ_HOST"),
				os.Getenv("RABBITMQ_PORT"),
			),
		))
		if err != nil {
			log.Fatalf("Failed to connect to RabbitMQ: %v", err)
		}
		instance = &RabbitMQ{
			connection: conn,
			channels:   make([]*amqp.Channel, 0),
		}
	})
	return instance
}

func (r *RabbitMQ) GetChannel() (*amqp.Channel, error) {
	ch, err := r.connection.Channel()
	if err != nil {
		return nil, err
	}
	r.channels = append(r.channels, ch)
	return ch, nil
}

func (r *RabbitMQ) Consume(job job.Job) {
	log.Printf("RabbitMQ: Start Consume job: %v", job.GetQueue())
	logger.GetInstance().Info("RabbitMQ: Start Consume job: ", zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))

	ch, err := r.GetChannel()
	if err != nil {
		logger.GetInstance().Error("RabbitMQ: Failed to open a channel: ", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
		return
	}

	q, err := ch.QueueDeclare(string(job.GetQueue()), true, false, false, false, nil)
	if err != nil {
		logger.GetInstance().Error("RabbitMQ: Failed to open a cha: ", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
		return
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		logger.GetInstance().Error("RabbitMQ: Failed to register a consumer:", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
		return
	}

	for d := range msgs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := job.Process(d.Body)
			if err != nil {
				logger.GetInstance().Error("RabbitMQ: Failed to  process job:", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
				d.Nack(false, false)
			} else {
				logger.GetInstance().Info("RabbitMQ: Job Process Successfully: ", zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
				d.Ack(false)
			}
		}()
	}
}

func (r *RabbitMQ) Publish(queue string, body []byte) error {
	ch, err := r.GetChannel()
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return err
	}

	var js json.RawMessage
	contentType := "text/plain"
	if json.Unmarshal(body, &js) == nil {
		contentType = "application/json"
	}

	err = ch.Publish("", q.Name, false, false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		},
	)
	return err
}

func (r *RabbitMQ) Shutdown(ctx context.Context) {
	<-ctx.Done()
	wg.Wait()
	for _, ch := range r.channels {
		ch.Close()
	}

	if r.connection != nil {
		r.connection.Close()
	}
	log.Println("All RabbitMQ jobs completed, shutting down")
	logger.GetInstance().Info("RabbitMq: All jobs completed, shutting down: ", zap.Time("timestamp", time.Now()))
}
