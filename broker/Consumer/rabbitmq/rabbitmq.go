package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"kafkaAndRabbitAndReddisAndGooooo/job"
	"log"
	"os"
	"sync"
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

	ch, err := r.GetChannel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	q, err := ch.QueueDeclare(string(job.GetQueue()), true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	for d := range msgs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := job.Process(d.Body)
			if err != nil {
				d.Nack(false, false)
				log.Println("Error processing job:", err)
			} else {
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
}
