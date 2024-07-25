package job

type Queue string

type Job interface {
	GetQueue() Queue
	Process(payload []byte) error
}
