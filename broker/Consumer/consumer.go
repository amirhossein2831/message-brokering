package Consumer

import (
	"context"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/kafka"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/rabbitmq"
	"github.com/amirhossein2831/message-brokering/broker/Consumer/redis"
	"github.com/amirhossein2831/message-brokering/broker/Driver"
	"github.com/amirhossein2831/message-brokering/job"
	"github.com/amirhossein2831/message-brokering/pkg/logger"
	"go.uber.org/zap"
	"log"
	"time"
)

func RegisterJob(ctx context.Context, job job.Job) {
	switch Driver.EnvDriver {
	case Driver.Redis:
		go redis.GetInstance().Consume(ctx, job)
	case Driver.RabbitMQ:
		go rabbitmq.GetInstance().Consume(ctx, job)
	case Driver.Kafka:
		go kafka.GetInstance().Consume(ctx, job)
	default:
		log.Println("Unsupported driver")
		logger.GetInstance().Error("Unsupported driver", zap.Any("driver", Driver.EnvDriver), zap.Time("time", time.Now()))
	}
}
