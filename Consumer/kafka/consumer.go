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
		"bootstrap.servers":  fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")),
		"group.id":           os.Getenv("KAFKA_CONSUMER_GROUP_ID"),
		"auto.offset.reset":  os.Getenv("KAFKA_CONSUMER_OFFSET"),
		"enable.auto.commit": false,
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
	topic := string(job.GetQueue())
	log.Printf("Kafka: Start Consume job: %v", job.GetQueue())

	partitions, err := k.createTopic(topic, 2, 1)
	if err != nil {
		log.Println(err.Error())
		return
	}

	var topicPartitions []kafka.TopicPartition
	for _, partition := range partitions {
		topicPartitions = append(topicPartitions, kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    kafka.OffsetInvalid,
		})
	}

	err = k.connection.Assign(topicPartitions)
	if err != nil {
		log.Fatalf("Failed to assign partitions: %s", err)
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

			_, err = k.connection.CommitMessage(msg)
			if err != nil {
				fmt.Printf("Failed to commit message: %v\n", err)
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
func (k *Kafka) createTopic(topic string, numPartitions int, replicationFactor int) ([]int32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")),
	})
	if err != nil {
		return nil, err
	}
	defer adminClient.Close()

	metadata, err := adminClient.GetMetadata(nil, false, int(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %v", err)
	}

	for _, t := range metadata.Topics {
		if t.Topic == topic {
			return getPartitionsFromMetadata(t), nil
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
		return nil, fmt.Errorf("failed to create topic: %v", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return nil, fmt.Errorf("failed to create topic %s: %v", topic, result.Error)
		}
	}

	// Fetch metadata again to get the partitions of the newly created topic
	metadata, err = adminClient.GetMetadata(&topic, false, int(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata after topic creation: %v", err)
	}

	topicMetadata, exists := metadata.Topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist after creation", topic)
	}

	return getPartitionsFromMetadata(topicMetadata), nil
}

func getPartitionsFromMetadata(topicMetadata kafka.TopicMetadata) []int32 {
	var partitions []int32
	for _, partition := range topicMetadata.Partitions {
		partitions = append(partitions, partition.ID)
	}
	return partitions
}
