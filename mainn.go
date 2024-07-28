package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"kafkaAndRabbitAndReddisAndGooooo/Jobb"
	"kafkaAndRabbitAndReddisAndGooooo/broker/kafka_queue"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	p, err := kafka_queue.NewKafka(os.Getenv("KAFKA_CLIENT_ID"), "all")
	if err != nil {
		log.Fatalf("Failed to create connection: %s", err)
	}
	defer p.Producer.Close()

	topic := "test_topic"
	message := "Hello KafkaProducer from Go!"

	err = p.Produce(topic, []byte(message), kafka.PartitionAny)
	if err != nil {
		log.Fatalf("Failed to produce message: %s", err)
	}

	c, err := kafka_queue.NewKafkaConsumer("foo", []string{topic})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	go func() {
		<-shutdownChan
		fmt.Println("Shutdown signal received")
		cancel() // Cancel the context to signal the consumer to stop
	}()

	c.Consume(ctx, Jobb.LoggingJob)
}
