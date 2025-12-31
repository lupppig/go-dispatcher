package main

import (
	"log"
	"os"
)

func initLogger() *log.Logger {
	file, err := os.OpenFile("dispatcher.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	logger = log.New(file, "", log.LstdFlags|log.Lmicroseconds)
	return logger
}
