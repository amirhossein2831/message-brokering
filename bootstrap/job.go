package bootstrap

import (
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/kafka"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/rabbitmq"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Driver"
	"kafkaAndRabbitAndReddisAndGooooo/job"
)

func InitJobs() {
	// add your job to jobs list...
	Register(job.NewLogJob())
	Register(job.NewHelloJob())
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
