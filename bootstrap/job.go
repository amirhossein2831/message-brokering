package bootstrap

import (
	"fmt"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/kafka"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/rabbitmq"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Driver"
	"kafkaAndRabbitAndReddisAndGooooo/job"
	"kafkaAndRabbitAndReddisAndGooooo/pkg/logger"
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
