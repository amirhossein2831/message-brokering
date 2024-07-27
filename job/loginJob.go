package job

const (
	LogQueue Queue = "log-queue"
)

type LogJob struct {
	queue Queue
}

func NewLogJob(queueName Queue) *LogJob {
	return &LogJob{
		queue: queueName,
	}
}

func (j *LogJob) GetQueue() Queue {
	return j.queue
}

func (j *LogJob) Process(payload []byte) error {
	println("Process login job" + string(payload))

	return nil
}
