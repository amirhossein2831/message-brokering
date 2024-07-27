package bootstrap

import (
	"fmt"
	"kafkaAndRabbitAndReddisAndGooooo/Consumer/kafka"
	"kafkaAndRabbitAndReddisAndGooooo/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/driver"
	"kafkaAndRabbitAndReddisAndGooooo/job"
	"os"
)

var jobs []job.Job

func InitJobs() error {
	// add your job to jobs list...
	jobs = append(jobs, job.NewLogJob(job.LogQueue))

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
	case string(driver.Redis):
		for _, j := range jobs {
			go redis.GetInstance().Consume(j)
		}
		//TODO: rabbit is remain
	case string(driver.RabbitMQ):
		for _, j := range jobs {
			println("rabbit", j.GetQueue())
		}
	case string(driver.Kafka):
		for _, j := range jobs {
			go kafka.GetInstance().Consume(j)
		}
	default:
		return fmt.Errorf("not a valid channel")
	}
	return nil
}
