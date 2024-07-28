package main

import (
	"context"
	"github.com/amirhossein2831/message-brokering/bootstrap"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/kafka"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/rabbitmq"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/redis"
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
	err := bootstrap.InitEnv()
	if err != nil {
		log.Fatal(err)
		return
	}

	// Init env driver
	err = bootstrap.InitDriver()
	if err != nil {
		log.Fatal("Error loading driver", err)
		return
	}

	// Init Jobs
	bootstrap.InitJobs()

	// Wait for interrupt signal
	<-sigChan
	log.Println("Received shutdown signal")
	cancel()

	// Shutdowns...
	redis.GetInstance().Shutdown(ctx)
	kafka.GetInstance().Shutdown(ctx)
	rabbitmq.GetInstance().Shutdown(ctx)
}
