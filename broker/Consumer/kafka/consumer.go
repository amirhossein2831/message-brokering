package kafka

import (
	"context"
	"fmt"
	"github.com/amirhossein2831/message-brokering/job"
	"github.com/amirhossein2831/message-brokering/pkg/logger"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
	"log"
	"os"
	"sync"
	"time"
)

var wg sync.WaitGroup

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

	return &Kafka{
		connection: c,
	}
}

func (k *Kafka) Consume(ctx context.Context, job job.Job) {
	topic := string(job.GetQueue())
	log.Printf("Kafka: Start Consume job: %v", job.GetQueue())
	logger.GetInstance().Info("Kafka: Start Consume job: ", zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))

	partitions, err := k.createTopic(topic, 2, 1)
	if err != nil {
		logger.GetInstance().Error("Kafka: Failed to create Topic: ", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
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
		logger.GetInstance().Error("Kafka: Failed to assign partitions: ", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
		return
	}

	messages := make(chan *kafka.Message)

	go k.ReadMessages(ctx, messages)

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka: Shutdown the kafka channel: ", string(job.GetQueue()), "...")
			k.Shutdown()
			// Perform any necessary cleanup here
			return
		case msg, ok := <-messages:
			if !ok {
				fmt.Println("Channel closed")
				return
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				if err = job.Process(msg.Value); err != nil {
					logger.GetInstance().Error("Kafka: Failed processing message: ", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
				}

				_, err = k.connection.CommitMessage(msg)
				if err != nil {
					logger.GetInstance().Error("Kafka: Failed to commit message: ", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
				}
				logger.GetInstance().Info("Kafka: Job Process Successfully: ", zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
			}()
		}
	}
}

func (k *Kafka) Shutdown() {
	wg.Wait()
	k.connection.Close()
	logger.GetInstance().Info("Kafka: Shutdown the Kafka consumer", zap.Time("timestamp", time.Now()))
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

func (k *Kafka) ReadMessages(ctx context.Context, messages chan<- *kafka.Message) {
	defer close(messages)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := k.connection.ReadMessage(-1)
			if err != nil {
				logger.GetInstance().Error("Kafka: Connection error: ", zap.Error(err), zap.Time("timestamp", time.Now()))
				time.Sleep(time.Second)
				continue
			}
			messages <- msg
		}
	}
}
