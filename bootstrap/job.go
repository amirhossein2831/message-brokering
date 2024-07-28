package bootstrap

import (
	"fmt"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/kafka"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/rabbitmq"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/redis"
	"github.com/amirhossein2831/message-brokering/broker/Driver"
	"github.com/amirhossein2831/message-brokering/job"
	"github.com/amirhossein2831/message-brokering/pkg/logger"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"time"
)

func InitEnv() error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("error loading .env file")
	}

	logger.GetInstance().Info("Env Service: Env var loaded successfully", zap.Time("timestamp", time.Now()))
	return nil
}

func InitDriver() error {
	err := Driver.GetDriver()
	if err != nil {
		return fmt.Errorf("error loading driver %v", err)
	}

	logger.GetInstance().Info("Env Service: Broker Driver set correctly", zap.Time("timestamp", time.Now()))
	return nil
}

func Register(job job.Job) {
	switch Driver.EnvDriver {
	case Driver.Redis:
		go redis.GetInstance().Consume(job)
	case Driver.RabbitMQ:
		go rabbitmq.GetInstance().Consume(job)
	case Driver.Kafka:
		go kafka.GetInstance().Consume(job)
	}
}

// InitJobs :place to initial the jobs
func InitJobs() {
	Register(job.NewLogJob())
	Register(job.NewHelloJob())
}
