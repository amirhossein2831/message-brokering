package Driver

import (
	"fmt"
	"os"
)

type Driver string

const (
	Redis    Driver = "redis"
	RabbitMQ Driver = "rabbitmq"
	Kafka    Driver = "kafka"
)

var EnvDriver Driver

func GetDriver() error {
	d := os.Getenv("MESSAGE_BROKER_DRIVER")

	if d == string(Redis) {
		EnvDriver = Redis
		return nil
	}

	if d == string(RabbitMQ) {
		EnvDriver = RabbitMQ
		return nil
	}

	if d == string(Kafka) {
		EnvDriver = Kafka
		return nil
	}

	return fmt.Errorf("not a valid driver")
}
