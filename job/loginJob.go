package job

const LogQueue Queue = "log-queue"

type LogJob struct{}

func NewLogJob() *LogJob {
	return &LogJob{}
}

func (j *LogJob) GetQueue() Queue {
	return LogQueue
}

func (j *LogJob) Process(payload []byte) error {
	println("Process login job" + string(payload))

	return nil
}
