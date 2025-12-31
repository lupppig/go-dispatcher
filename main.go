package main

import (
	"fmt"
	"time"
)

func main() {
	dispatcher := NewDispatcher(5)
	dispatcher.Run()

	for i := 0; i < 50; i++ {
		jobName := fmt.Sprintf("send %d", i)
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
