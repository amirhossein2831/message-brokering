# Job Wrapper in Go

This Go library provides a unified interface for working with job queues in Redis, Kafka, and RabbitMQ. The goal is to abstract the complexity of interacting with different message brokers and provide a simple and consistent API for enqueueing and processing jobs.

## Getting Started

### 1. Introduction

This library allows you to:

- Consume jobs to a specified queue.
- Publish message to a queue.

### 2. Installation

To use this library, copy the `.env.example` file to `.env`:

```sh
cp .env.example .env
```

### 3. Configuration

You need to set up your message broker (redis, rabbitmq, kafka) in the .env file without type. The MESSAGE_BROKER_DRIVER environment variable specifies which broker to use.


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
