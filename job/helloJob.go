package job

const (
	HelloQueue Queue = "hello-queue"
)

type HelloJob struct {
}

func NewHelloJob() *HelloJob {
	return &HelloJob{}
}

func (j *HelloJob) GetQueue() Queue {
	return HelloQueue
}

func (j *HelloJob) Process(payload []byte) error {
	println("Hello to :)" + string(payload))

	return nil
}
