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
}

func GetInstance() *Redis {
	clientOnce.Do(func() {
		instance = &Redis{
			connection: redis.NewClient(&redis.Options{
				Addr: fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT")),
			}),
		}
	})
	return instance
}

func (r *Redis) GetClient() *redis.Client {
	return r.connection
}

func (r *Redis) Consume(ctx context.Context, job job.Job) {
	log.Printf("Redis: Start Consume job: %v", job.GetQueue())
	logger.GetInstance().Info("Redis: Start Consume job: ", zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))

	pubSub := r.connection.Subscribe(ctx, string(job.GetQueue()))

	for {
		select {
		case <-ctx.Done():
			r.Shutdown(pubSub, string(job.GetQueue()))
			return
		case msg, ok := <-pubSub.Channel():
			if !ok {
				fmt.Printf("Channel closed for channel: %s\n", string(job.GetQueue()))
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := job.Process([]byte(msg.Payload))
				if err != nil {
					logger.GetInstance().Error("Redis: Failed processing message: ", zap.Error(err), zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))
				}
				logger.GetInstance().Info("Redis: Job Process Successfully: ", zap.Any("QueueName: ", job.GetQueue()), zap.Time("timestamp", time.Now()))

			}()
		}
	}
}

func (r *Redis) Shutdown(pubSub *redis.PubSub, queue string) {
	log.Println("Redis: Shutdown the redis channel: ", queue, "...")

	wg.Wait()
	if err := pubSub.Unsubscribe(context.Background()); err != nil {
		log.Println("Redis: Failed unsubscribe from redis channel: ", queue)
		logger.GetInstance().Error("Redis: Failed unsubscribe from redis channel: ", zap.String("QueueName: ", queue), zap.Error(err), zap.Time("timestamp", time.Now()))
	}

	if err := pubSub.Close(); err != nil {
		log.Println("Redis: Failed to close redis pubSub from redis channel: ", queue)
		logger.GetInstance().Error("Redis: Failed to close redis pubSub from redis channel: ", zap.String("QueueName: ", queue), zap.Error(err), zap.Time("timestamp", time.Now()))
	}
	logger.GetInstance().Info("Redis: Shutdown the redis channel: ", zap.String("QueueName: ", queue), zap.Time("timestamp", time.Now()))
}
