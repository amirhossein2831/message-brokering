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

func GetDriver() (Driver, error) {
	d := os.Getenv("MESSAGE_BROKER_DRIVER")

	if d == string(Redis) || d == string(Kafka) || d == string(RabbitMQ) {
		return Driver(d), nil
	}
	return "", fmt.Errorf("not a valid driver")
}
