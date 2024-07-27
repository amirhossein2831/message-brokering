package Jobb

import "log"

func LoggingJob(payload []byte) error {
	log.Println(string(payload))
	return nil
}
