package bootstrap

import (
	"fmt"
	"kafkaAndRabbitAndReddisAndGooooo/Consumer/redis"
	"kafkaAndRabbitAndReddisAndGooooo/job"
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
	driver := "redis" // TODO: give from .env

	switch driver {
	case "redis":
		for _, j := range jobs {
			go redis.GetInstance().Consume(j)
		}
	case "rabbitmq":
	case "kafka":
	default:
		return fmt.Errorf("not a valid channel")
	}
	return nil
}
