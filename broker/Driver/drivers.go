package Driver

import (
	"fmt"
	"github.com/amirhossein2831/message-brokering/pkg/logger"
	"go.uber.org/zap"
	"os"
	"time"
)

type Driver string

const (
	Redis    Driver = "redis"
	RabbitMQ Driver = "rabbitmq"
	Kafka    Driver = "kafka"
)

var EnvDriver Driver

func Init() error {
	d := os.Getenv("MESSAGE_BROKER_DRIVER")

	if d == string(Redis) {
		logger.GetInstance().Info("Broker Driver set correctly", zap.Time("timestamp", time.Now()))
		EnvDriver = Redis
		return nil
	}

	if d == string(RabbitMQ) {
		logger.GetInstance().Info("Broker Driver set correctly", zap.Time("timestamp", time.Now()))
		EnvDriver = RabbitMQ
		return nil
	}

	if d == string(Kafka) {
		logger.GetInstance().Info("Broker Driver set correctly", zap.Time("timestamp", time.Now()))
		EnvDriver = Kafka
		return nil
	}

	return fmt.Errorf("not a valid driver")
}
