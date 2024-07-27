package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafkaAndRabbitAndReddisAndGooooo/job"
	"log"
	"os"
	"sync"
	"time"
)

var wg sync.WaitGroup
var topics []string

type Kafka struct {
	connection *kafka.Consumer
}

// GetInstance creates a new KafkaInstance consumer
func GetInstance() *Kafka {
	//TODO: read group id  and offset from .env
	c, _ := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")),
		"group.id":          os.Getenv("KAFKA_CONSUMER_GROUP_ID"),
		"auto.offset.reset": os.Getenv("KAFKA_CONSUMER_OFFSET"),
	})
	return &Kafka{
		connection: c,
	}
}

func (k *Kafka) Consume(job job.Job) {
	log.Printf("Kafka: Start Consume job: %v", job.GetQueue())

	ctx := context.Background()
	err := k.connection.Subscribe(string(job.GetQueue()), nil)
	if err != nil {
		log.Println(err.Error())
		return
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Received shutdown signal, waiting for all processes to finish...")
			k.connection.Close()
			wg.Wait()
			return
		default:
			msg, err := k.connection.ReadMessage(-1)
			if err != nil {
				fmt.Printf("connection error: %v\n", err)
				time.Sleep(time.Second)
				continue
			}

			wg.Add(1)
			go func(msg *kafka.Message) {
				defer wg.Done()

				fmt.Printf("Received message: %s\n", string(msg.Value))
				if err = job.Process(msg.Value); err != nil {
					fmt.Printf("Error processing message: %v\n", err)
				}
			}(msg)
		}
	}
}
