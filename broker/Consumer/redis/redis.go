package redis

import (
	"context"
	"fmt"
	"github.com/amirhossein2831/message-brokering/job"
	"github.com/amirhossein2831/message-brokering/pkg/logger"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"log"
	"os"
	"sync"
	"time"
)

var (
	instance   *Redis
	clientOnce sync.Once
	wg         sync.WaitGroup
)

type Redis struct {
	connection *redis.Client
	pubSubs    []*redis.PubSub
}

func GetInstance() *Redis {
	clientOnce.Do(func() {
		instance = &Redis{
			connection: redis.NewClient(&redis.Options{
				Addr: fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT")),
			}),
			pubSubs: make([]*redis.PubSub, 0),
		}
	})
	return instance
}

func (r *Redis) GetClient() *redis.Client {
	return r.connection
}

func (r *Redis) Consume(job job.Job) {
	log.Printf("Redis: Start Consume job: %v", job.GetQueue())
	logger.GetInstance().Info("Redis: Start Consume job: ", zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))

	ctx := context.Background()
	pubSub := r.connection.Subscribe(ctx, string(job.GetQueue()))
	r.pubSubs = append(r.pubSubs, pubSub)

	for {
		msg, err := pubSub.ReceiveMessage(ctx)
		if err != nil {
			logger.GetInstance().Error("Redis: Error Receiving Message: ", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()

			err = job.Process([]byte(msg.Payload))
			if err != nil {
				logger.GetInstance().Error("Redis: Failed processing message: ", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
			}
			logger.GetInstance().Info("Redis: Job Process Successfully: ", zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))

		}()
	}
}

func (r *Redis) Shutdown(ctx context.Context) {
	<-ctx.Done()
	wg.Wait()
	for _, pubSub := range r.pubSubs {
		_ = pubSub.Close()
	}

	if r.connection != nil {
		r.connection.Close()
	}
	log.Println("All Redis jobs completed, shutting down")
	logger.GetInstance().Info("Redis: All jobs completed, shutting down: ", zap.Time("timestamp", time.Now()))
}
