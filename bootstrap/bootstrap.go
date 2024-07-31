package bootstrap

import (
	"context"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/kafka"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/rabbitmq"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/redis"
	"github.com/amirhossein2831/message-brokering/broker/Driver"
	"github.com/amirhossein2831/message-brokering/database"
	"log"
)

func InitApp(ctx context.Context) {
	err := Driver.Init()
	if err != nil {
		log.Fatalf("Driver Service: Failed to Initialize. %v", err)
	}
	log.Println("Driver Service: Initialized Successfully.")

	err = database.Init()
	if err != nil {
		log.Fatalf("Database Service: Failed to Initialize. %v", err)
	}
	log.Println("Database Service: Initialized Successfully.")

	if Driver.EnvDriver == Driver.Kafka {
		err := kafka.GetInstance().TestConnection()
		if err != nil {
			log.Fatalf("Kafka Service: Failed to Initialize. %v", err)
		}
		log.Println("Kafka Service: Initialized Successfully.")
	}

	if Driver.EnvDriver == Driver.RabbitMQ {
		rabbitmq.GetInstance()
		log.Println("Rabbitmq Service: Initialized Successfully.")
	}

	if Driver.EnvDriver == Driver.Redis {
		err := redis.GetInstance().TestConnection()
		if err != nil {
			log.Fatalf("Redis Service: Failed to Initialize. %v", err)
		}
		log.Println("Redis Service: Initialized Successfully.")
	}
}
