package model

import (
	"github.com/amirhossein2831/message-brokering/database"
	"github.com/amirhossein2831/message-brokering/job"
	"github.com/amirhossein2831/message-brokering/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"time"
)

type RabbitMQ struct {
	ID        uint           `json:"id" gorm:"primarykey"`
	Status    ConsumerStatus `json:"status" gorm:"type:varchar(255)"`
	QueueName job.Queue      `json:"queue_name" gorm:"type:varchar(255)"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `json:"deleted_at" gorm:"index"`
}

func NewRabbitMQ(status ConsumerStatus, queueName job.Queue) *RabbitMQ {
	return &RabbitMQ{
		Status:    status,
		QueueName: queueName,
	}
}

func (r *RabbitMQ) Create() {
	result := database.GetInstance().GetClient().Create(r)
	if result.Error != nil {
		logger.GetInstance().Error("Rabbitmq Service: Failed to create instance", zap.Error(result.Error), zap.Time("timestamp", time.Now()))
	}
}

func (r *RabbitMQ) UpdateStatus(status ConsumerStatus) {
	r.Status = status
	result := database.GetInstance().GetClient().Save(r)
	if result.Error != nil {
		logger.GetInstance().Error("Rabbitmq Service: Failed to update instance", zap.Error(result.Error), zap.Time("timestamp", time.Now()))
	}
}
