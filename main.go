package main

import (
	"context"
	"fmt"
	"kafkaAndRabbitAndReddisAndGooooo/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/Publisher"
	"kafkaAndRabbitAndReddisAndGooooo/bootstrap"
	"kafkaAndRabbitAndReddisAndGooooo/job"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Init...
	err := bootstrap.InitJobs()
	if err != nil {
		log.Fatal(err)
	}

	// Publish some message
	// TODO: read the channel from env
	_ = Publisher.NewPublisher(Publisher.Redis, job.LogQueue).Publish([]byte("hello"))
	fmt.Println("Published message at", "hi")
	_ = Publisher.NewPublisher(Publisher.Redis, job.LogQueue).Publish([]byte("hello"))
	fmt.Println("Published message at", "hi")

	// Wait for interrupt signal
	<-sigChan
	log.Println("Received shutdown signal")
	cancel()

	// Shutdowns...
	redis.GetInstance().Shutdown(ctx)
}
