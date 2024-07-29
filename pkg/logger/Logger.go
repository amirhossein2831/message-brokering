package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"sync"
)

var (
	loggerInstance *zap.Logger
	once           sync.Once
)

func Init() {
	GetInstance()
}

func GetInstance() *zap.Logger {
	once.Do(func() {
		core := zapcore.NewTee(zapcore.NewCore(encoderFile(), logFile(), zapcore.DebugLevel))
		loggerInstance = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	})
	return loggerInstance
}

func encoderFile() zapcore.Encoder {
	conf := zap.NewProductionEncoderConfig()
	conf.EncodeTime = zapcore.ISO8601TimeEncoder
	return zapcore.NewJSONEncoder(conf)
}

func logFile() zapcore.WriteSyncer {
	path := os.Getenv("LOG_PATH")
	if path == "" {
		path = "log/message-broker.log"
	}
	file, _ := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	return zapcore.AddSync(file)
}
