package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"kafkaAndRabbitAndReddisAndGooooo/job"
	"log"
	"os"
	"sync"
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

	ctx := context.Background()
	pubSub := r.connection.Subscribe(ctx, string(job.GetQueue()))
	r.pubSubs = append(r.pubSubs, pubSub)

	for {
		msg, err := pubSub.ReceiveMessage(ctx)
		if err != nil {
			log.Println("Error receiving message:", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()

			err = job.Process([]byte(msg.Payload))
			if err != nil {
				log.Println("Error executing job:", err)
			}
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
}
