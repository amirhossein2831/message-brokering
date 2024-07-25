package job

import (
	"kafkaAndRabbitAndReddisAndGooooo/queue"
)

type Job interface {
	GetQueue() queue.Queue
	Process(payload []byte) error
}
