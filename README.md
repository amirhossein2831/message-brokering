# Job Wrapper in Go

This Go library provides a unified interface for working with job queues in Redis, Kafka, and RabbitMQ. The goal is to abstract the complexity of interacting with different message brokers and provide a simple and consistent API for enqueueing and processing jobs.

## Getting Started

### 1. Introduction

This library allows you to:

- Consume jobs to a specified queue.
- Publish message to a queue.

### 2. Installation

```shell
go get github.com/amirhossein2831/message-brokering@v1.5.4
```

To use this library, copy the `.env.example` file to `.env`:

```sh
cp .env.example .env
```

### 3. Configuration

1_You need to set up your message broker (redis, rabbitmq, kafka) in the .env file without type. The MESSAGE_BROKER_DRIVER environment variable specifies which broker to use.
2_create a folder called log in the root of your project for save the log 


### 4. Job Interface and Declaration

You should have a Queue var type Queue in each Job and a job that implement following interface 

```go
    type Job interface {
        GetQueue() Queue
        Process(payload []byte) error
    }
```

a valid job example

```go
    const LogQueue Queue = "log-queue"
    
    type LogJob struct{}
    
    func NewLogJob() *LogJob {
        return &LogJob{}
    }
    
    func (j *LogJob) GetQueue() Queue {
        return LogQueue
    }
    
    func (j *LogJob) Process(payload []byte) error {
        println("Process log job: " + string(payload))
        return nil
    }

```

### 5. Register yourJob

use register method to register you job like this and call it in the root of project (main.go)
```go
    func InitJobs() {
        Register(job.NewLogJob())
        Register(job.NewHelloJob())
    }

// main.go
    InitJobs()
```

### 5. Publish Message

you can easily publish message, NewPublisher take the Queue type as arg to send message to that Queue

```go
    err := Publisher.NewPublisher(job.LogQueue).Publish([]byte("hello"))
	if err != nil {
		//handle Error
	}
```
### 6. Shutdown...

you need to wait for a kill signal for shutdown so after kill signal you can shutdown 

```go
sigChan := make(chan os.Signal, 1)
    //wait for kill signal
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// load .env
	err := godotenv.Load()
	if err != nil {
		return
	}
	
	// init the driver
	err = Driver.Init()
	if err != nil {
		log.Fatal(err)
		return
	}

	// publish a message to LogQueue
	err = Publisher.NewPublisher(job.LogQueue).Publish([]byte("hello"))
	if err != nil {
		log.Fatal(err)
		return
	}

	// Init Jobs
	bootstrap.InitJob()

	<-sigChan
	log.Println("Received shutdown signal")
	cancel()

	// Shutdowns...
    Consumer.ShutDown()

```


### 7.ENV

```dotenv
# APP
APP_HOST_NAME=localhost
MESSAGE_BROKER_DRIVER=rabbitmq
MESSAGE_BROKER_WORKER_NUMBER=3

# KAFKA
KAFKA_HOST=localhost
KAFKA_PORT=9092
KAFKA_CLIENT_ID=localhost
KAFKA_CLIENT_ACK=all
KAFKA_CONSUMER_OFFSET=earliest
KAFKA_CONSUMER_GROUP_ID=test

# REDIS
REDIS_HOST=localhost
REDIS_PORT=6379

# RABBIT
RABBITMQ_USERNAME=user
RABBITMQ_PASSWORD=password
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_MANAGEMENT_PORT=15672

# LOGGER
LOG_PATH=log/message-broker.log
```