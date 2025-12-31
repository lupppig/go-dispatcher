package main

import (
	"fmt"
	"log"
	"time"
)

var logger *log.Logger

func main() {
	dispatcher := NewDispatcher(5)
	dispatcher.Run()

	logger = initLogger()
	for i := 0; i < 100; i++ {
		jobName := fmt.Sprintf("send %d", i)
		if i%2 == 0 {
			jobName = "no job name"
		}
		job := NewJob(
			fmt.Sprintf("payload-%d", i),
			jobName,
			3,
		)

		dispatcher.IncomingJob <- job
	}

	time.Sleep(5 * time.Second)
	dispatcher.Close() // graceful shutdown
}
