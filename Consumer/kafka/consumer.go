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
var connections []*kafka.Consumer

type Kafka struct {
	connection *kafka.Consumer
}

// GetInstance creates a new KafkaInstance consumer
func GetInstance() *Kafka {
	c, _ := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")),
		"group.id":          os.Getenv("KAFKA_CONSUMER_GROUP_ID"),
		"auto.offset.reset": os.Getenv("KAFKA_CONSUMER_OFFSET"),
	})
	if connections == nil {
		connections = make([]*kafka.Consumer, 0)
	}
	connections = append(connections, c)

	return &Kafka{
		connection: c,
	}
}

func (k *Kafka) Consume(job job.Job) {
	log.Printf("Kafka: Start Consume job: %v", job.GetQueue())

	err := k.connection.Subscribe(string(job.GetQueue()), nil)
	if err != nil {
		log.Println(err.Error())
		return
	}

	for {
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

func (k *Kafka) Shutdown(ctx context.Context) {
	<-ctx.Done()
	wg.Wait()

	for _, conn := range connections {
		conn.Close()
	}
	log.Println("All Kafka jobs completed, shutting down")
}
