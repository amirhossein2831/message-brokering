package driver

type Driver string

const (
	Redis    Driver = "redis"
	RabbitMQ Driver = "rabbitmq"
	Kafka    Driver = "kafka"
)
