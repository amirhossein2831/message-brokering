package bootstrap

import (
	"fmt"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/kafka"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/rabbitmq"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/broker/Driver"
	"kafkaAndRabbitAndReddisAndGooooo/job"
	"os"
)

var jobs []job.Job

func InitJobs() error {
	// add your job to jobs list...
	jobs = append(jobs, job.NewLogJob())
	jobs = append(jobs, job.NewHelloJob())

	// Register your Job
	err := Register(jobs)
	if err != nil {
		return err
	}
	return nil
}

// Register :Register jobs
func Register(jobs []job.Job) error {
	d := os.Getenv("MESSAGE_BROKER_DRIVER")

	switch d {
	case string(Driver.Redis):
		for _, j := range jobs {
			go redis.GetInstance().Consume(j)
		}
	case string(Driver.RabbitMQ):
		for _, j := range jobs {
			go rabbitmq.GetInstance().Consume(j)
		}
	case string(Driver.Kafka):
		for _, j := range jobs {
			go kafka.GetInstance().Consume(j)
		}
	default:
		return fmt.Errorf("not a valid channel")
	}
	return nil
}
