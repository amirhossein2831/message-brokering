package job

import (
	"kafkaAndRabbitAndReddisAndGooooo/queue"
)

type LogJob struct {
	queue queue.Queue
}

func NewLogJob(queueName queue.Queue) *LogJob {
	return &LogJob{
		queue: queueName,
	}
}

func (j *LogJob) GetQueue() queue.Queue {
	return j.queue
}

func (j *LogJob) Process(payload []byte) error {
	println("Process login job" + string(payload))

	return nil
}
