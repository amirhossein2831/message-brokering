package bootstrap

import (
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/kafka"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/rabbitmq"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Driver"
	"kafkaAndRabbitAndReddisAndGooooo/job"
	"log"
	"os"
)

func InitJobs() {
	// add your job to jobs list...
	Register(job.NewLogJob())
	Register(job.NewHelloJob())
}

func Register(job job.Job) {
	d := os.Getenv("MESSAGE_BROKER_DRIVER")

	switch d {
	case string(Driver.Redis):
		go redis.GetInstance().Consume(job)
	case string(Driver.RabbitMQ):
		go rabbitmq.GetInstance().Consume(job)
	case string(Driver.Kafka):
		go kafka.GetInstance().Consume(job)
	default:
		log.Printf("not a valid driver %s", d)
	}
}
