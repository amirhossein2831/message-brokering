package job

const (
	HelloQueue Queue = "hello-queue"
)

type HelloJob struct {
	queue Queue
}

func NewHelloJob(queueName Queue) *HelloJob {
	return &HelloJob{
		queue: queueName,
	}
}

func (j *HelloJob) GetQueue() Queue {
	return j.queue
}

func (j *HelloJob) Process(payload []byte) error {
	println("Hello to :)" + string(payload))

	return nil
}
