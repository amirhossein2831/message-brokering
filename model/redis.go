package model

import (
	"github.com/amirhossein2831/message-brokering/database"
	"github.com/amirhossein2831/message-brokering/job"
	"github.com/amirhossein2831/message-brokering/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"time"
)

type Redis struct {
	ID        uint           `json:"id" gorm:"primarykey"`
	Status    ConsumerStatus `json:"status" gorm:"type:varchar(255)"`
	QueueName job.Queue      `json:"queue_name" gorm:"type:varchar(255)"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `json:"deleted_at" gorm:"index"`
}

func NewRedis(status ConsumerStatus, queueName job.Queue) *Redis {
	return &Redis{
		Status:    status,
		QueueName: queueName,
	}
}

func (r *Redis) Create() {
	result := database.GetInstance().GetClient().Create(r)
	if result.Error != nil {
		logger.GetInstance().Error("Redis Service: Failed to create instance", zap.Error(result.Error), zap.Time("timestamp", time.Now()))
	}
}
func (r *Redis) UpdateStatus(status ConsumerStatus) {
	r.Status = status
	result := database.GetInstance().GetClient().Save(r)
	if result.Error != nil {
		logger.GetInstance().Error("Kafka Service: Failed to update instance", zap.Error(result.Error), zap.Time("timestamp", time.Now()))
	}
}
