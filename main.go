package main

import (
	"context"
	"github.com/joho/godotenv"
	"kafkaAndRabbitAndReddisAndGooooo/Consumer/kafka"
	"kafkaAndRabbitAndReddisAndGooooo/Consumer/rabbitmq"
	"kafkaAndRabbitAndReddisAndGooooo/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/bootstrap"
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

	// Init env var
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Init Jobs
	err = bootstrap.InitJobs()
	if err != nil {
		log.Fatal(err)
	}

	// Wait for interrupt signal
	<-sigChan
	log.Println("Received shutdown signal")
	cancel()

	// Shutdowns...
	redis.GetInstance().Shutdown(ctx)
	kafka.GetInstance().Shutdown(ctx)
	rabbitmq.GetInstance().Shutdown(ctx)
}
