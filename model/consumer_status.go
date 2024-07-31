package model

type ConsumerStatus string

const (
	Queued     ConsumerStatus = "queued"
	InProgress ConsumerStatus = "in-progress"
	Faild      ConsumerStatus = "failed"
	Success    ConsumerStatus = "success"
)
