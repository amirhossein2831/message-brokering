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

	err := k.createTopic(string(job.GetQueue()), 2, 1)
	if err != nil {
		log.Println(err.Error())
		return
	}

	err = k.connection.Subscribe(string(job.GetQueue()), nil)
	if err != nil {
		log.Printf("kafka: failed to assign partitions: %v", err)
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

func (k *Kafka) createTopic(topic string, numPartitions int, replicationFactor int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")),
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	metadata, err := adminClient.GetMetadata(nil, false, int(5*time.Second))
	if err != nil {
		return fmt.Errorf("failed to get metadata: %v", err)
	}

	for _, t := range metadata.Topics {
		if t.Topic == topic {
			return nil
		}
	}

	results, err := adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		}},
	)
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("failed to create topic %s: %v", topic, result.Error)
		}
	}

	return nil
}
