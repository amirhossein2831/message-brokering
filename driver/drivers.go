package driver

type Driver string

const (
	Redis    Driver = "Consumer"
	RabbitMQ Driver = "rabbitmq"
	Kafka    Driver = "kafka"
)
