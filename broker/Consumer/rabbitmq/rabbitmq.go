package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/amirhossein2831/message-brokering/job"
	"github.com/amirhossein2831/message-brokering/pkg/logger"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
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

func (r *RabbitMQ) Consume(ctx context.Context, job job.Job) {
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

	for {
		select {
		case <-ctx.Done():
			ch.Close()
			r.Shutdown(string(job.GetQueue()))
			return
		case d, ok := <-msgs:
			if !ok {
				log.Printf("Channel closed for queue: %s\n", job.GetQueue())
				return
			}
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

func (r *RabbitMQ) Shutdown(queue string) {
	log.Println("RabbitMQ: Shutdown the RabbitMQ consumer:", queue, "...")

	wg.Wait()
	if r.connection != nil {
		r.connection.Close()
	}
	logger.GetInstance().Info("RabbitMQ: Shutdown the RabbitMQ consumer:", zap.String("QueueName: ", queue), zap.Time("timestamp", time.Now()))
}
